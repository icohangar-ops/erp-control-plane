"""epicor_eclipse — Epicor Eclipse REST extraction (session-token auth).

STATUS: implemented against the documented Eclipse API surface (the archived
Eclipse Systems KB endpoint inventory — integration spec "Epicor Eclipse —
Connector Mechanisms", art_Hp74a48b) but NOT exercised against a live tenant.
The endpoint paths, license gates, and the Search Index Builder dependency are
spec-verified; nearly everything else about the wire contract — the
``POST /Sessions`` request body, the response field names, the header that
transports the session token, the paging parameter names, and every
rate/payload number — is per-tenant contract the spec marks [G]: it lives in
each tenant's deployed API documentation (``http://EclipseServer:Port``,
default port 5000), not in any public source. Per the wave's [D] discipline
(BisTrack §7.1 pattern), those unknowns are REQUIRED settings that fail closed
on empty — the connector refuses to run until the tenant's deployed docs pin
them — and the response shapes it does code (the Aurinko-reported
``sessionToken``/``refreshToken`` names, the ``LineItems`` collection) fail
closed at runtime when a tenant payload disagrees. Run ``python -m
connectors.cli plan --source epicor_eclipse_template`` (dry-run) before any
live call; it needs no network.

Extraction notes (spec citations refer to art_Hp74a48b):

- Eclipse runs on InterSystems Caché; there is no SQL/ODBC extraction path for
  an external service — the REST API is the only supported extraction surface
  (spec scope premise). The API engine serves from the tenant's own host
  (on-prem: ``http://EclipseServer:Port`` on RHEL; Eclipse Cloud: Azure), so
  there is deliberately NO https-only gate — on-prem tenants are plain HTTP
  behind a site VPN or reverse proxy (spec §1).
- Auth is the proprietary expiring-session model, not OAuth2 (spec §2.1):
  ``POST /Sessions`` creates a session and returns ``sessionToken`` +
  ``refreshToken`` (field names from the one shipped third-party connector —
  confirm against the tenant's deployed docs at onboarding);
  ``POST /SessionRefresh`` refreshes "the session that is expired, but not
  deleted". No token TTL is public, so none is assumed: one session rejection
  on a data call triggers ``/SessionRefresh`` (falling back to a full
  ``POST /Sessions`` re-login), and a second rejection fails the run. Page
  state is client-side (pinned page numbers), so recovery resumes the same
  page; the walked entity still dedupes by natural id so no recovery path can
  double-stage rows.
- Endpoint families are license-gated (spec §2.4/§3): ``/SalesOrders`` needs
  the Sales Order API license, ``/PurchaseOrders`` the Purchase Order API
  license, ``/GLInquiryDetail`` the Accounting API license, ``ARInquiry`` the
  Accounting/AR license, ``/PriceMatrices`` the Pricing API license,
  ``/WarehouseTasks/*`` the Warehouse API license. An acquired site may have
  bought none of the Premium bundles — entitlements are checked in diligence,
  not here; the connector fails honestly on an unlicensed family.
- Search-based GETs require Eclipse's Search Index to be built for the
  entity; a fresh tenant answers "To use the search, first index the records.
  Please run the Search Index Builder." The connector surfaces that condition
  as its own failure (distinct from an empty result) — make "Search Index
  Builder scheduled + verified per entity" a deployment prerequisite (spec §4).
- Incremental: "most endpoints support the 'updatedAfter' query parameter"
  and there are NO webhooks or change notifications (spec §4) — polling is the
  only incremental mechanism. The row field carrying the change stamp is
  per-tenant ([D] ``watermark_field``); inventory quantities are
  snapshot-polled with no watermark at all (full ``/ProductInventoryList``
  sweep per run, spec §4).
- Branch-scoped security shapes every payload (spec §7.5): visibility is
  bounded by the service identity's accessible branches, and file/auth keys
  gate FIELDS, not just endpoints (spec §2.3) — a too-narrow account yields
  silently thinner payloads.
- Customers carry a ``deleted`` flag (spec §7.7): extraction and the
  key-inventory scan drop flagged rows client-side so the anti-join compares
  live-to-live. Polarity follows the operative reading (flag set = deleted);
  like P21's ``delete_flag``, polarity is validated per site at onboarding.
- Pricing is runtime-resolved (contractor matrices/multipliers, spec §3) and
  UOMs ride raw — no conversion happens at this layer (the canonical
  conversion-factor column is flagged for the formalization task
  todo_m58JUGS6).
- Invoices: there is NO ``/Invoices`` endpoint (spec §3 — verified absence);
  the AR-side read is the license-gated, inquiry-shaped ``ARInquiry`` object,
  and historical invoice LINES likely need the hybrid report/file channel —
  ``invoice_lines`` stays a documented plan, not an improvised extraction.
- Field names are per-tenant contract ([G] §2.1): this adapter reads the
  canonical field names off the source payloads verbatim (identity maps) and
  validates them at onboarding — a mismatch quarantines fail-closed (the DMSi
  rowset-key treatment) rather than staging nothing.
"""

from __future__ import annotations

import datetime as dt
import json
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

import httpx

from connectors.base import (
    BaseConnector,
    ConnectorError,
    ConnectorMaturity,
    ConnectorNotConfigured,
    ConnectorNotImplemented,
    DeleteSemantics,
    ExtractionMode,
    ExtractionPlan,
    natural_id_for,
)
from control_plane.config import ControlPlaneConfig
from control_plane.models import QuarantineRecord, SourceConfig
from control_plane.store import ControlPlaneStore

#: Documented Session-family endpoints (spec §2.1 — KB-verified paths).
SESSIONS_PATH = "/Sessions"
SESSION_REFRESH_PATH = "/SessionRefresh"

#: Backfill watermark: the epoch-equivalent ``updatedAfter`` that captures
#: everything (our posture; the zero-value format is per-tenant [G] — confirm
#: at onboarding). House idiom shared with the DMSi connector.
EPOCH_UPDATED_AFTER = "1900-01-01T00:00:00"

#: Reason codes recorded on quarantined API pages (csv_sftp/NetSuite/P21/DMSi parity).
RC_MALFORMED_PAGE = "MALFORMED_PAGE"
RC_UNPARSEABLE_JSON = "UNPARSEABLE_JSON"

#: Per-call fetch ceiling for the page-size setting: no rate, concurrency, or
#: payload numbers are public (spec §5) — the value is a per-tenant [D] pin
#: from the deployed docs, with no default invented here.
MAX_RETRIES = 5
BACKOFF_SECONDS = 2.0


class SessionRejected(ConnectorError):
    """The tenant rejected the session token — refresh, then re-login, then fail."""


class SearchIndexNotBuilt(ConnectorError):
    """The tenant's Search Index is not built for the entity (spec §4).

    Distinct from an empty result on purpose: an empty page means "no data",
    this means "the deployment prerequisite is unmet" — run the Search Index
    Builder for the entity before extracting.
    """


def _today() -> dt.date:
    """Extraction clock (monkeypatched in fixtures for deterministic windows)."""
    return dt.date.today()


def _eclipse_stamp(raw: str) -> dt.datetime:
    """Parse a watermark stamp for comparison; the raw text rides the
    checkpoint verbatim (format is per-tenant [G]).

    Timezone-aware stamps normalize to naive UTC so mixed-aware/naive
    comparisons cannot raise; precision otherwise varies by tenant.
    """
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ConnectorError(
            f"unparseable Epicor Eclipse timestamp {raw!r} — refusing to advance a "
            "watermark on it (storing one would silently skip records)"
        ) from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.UTC).replace(tzinfo=None)
    return parsed


def _is_deleted_customer(row: dict[str, object]) -> bool:
    """Customers carry a ``deleted`` flag (spec §7.7): drop only on an
    EXPLICITLY set flag — a missing/None value means the flag is absent on
    this tenant, not that the account is dead. Polarity per the operative
    reading (flag set = deleted); per-site polarity validation at onboarding
    is mandatory (the P21 delete_flag polarity lesson)."""
    value = row.get("deleted")
    if value is True or value == 1:
        return True
    return isinstance(value, str) and value.strip().upper() in {"Y", "TRUE", "1"}


@dataclass(frozen=True)
class SearchSurface:
    """A paged GET family: search endpoint + optional per-record detail read.

    The spec's multi-tier read pattern is list -> detail -> line children
    (§3); flat families read the search pages alone.
    """

    endpoint: str
    #: The API bundle that gates the family ("" = standard, no license gate).
    license: str
    #: True when the family rides the ``updatedAfter`` watermark (spec §4) —
    #: False only for the snapshot sweep, which has no watermark possible.
    watermark_capable: bool
    #: Per-record detail path template for line-level entities, e.g.
    #: "/SalesOrders/{order_no}" — {detail_id_field} is substituted.
    detail_endpoint: str | None = None
    #: Field on the search row carrying the detail-read id (the natural key).
    detail_id_field: str | None = None
    #: Collection field on the detail object holding the line rows — the
    #: documented sub-resource name ("LineItems"), confirmed at onboarding.
    detail_lines_field: str | None = None


#: canonical field <- source field per entity. Eclipse field names are
#: per-tenant contract ([G] §2.1) with no public vocabulary, so the maps are
#: IDENTITY maps — the adapter reads the canonical names verbatim and
#: validates at onboarding; a mismatch fails closed at runtime.
_FIELD_MAPS: dict[str, dict[str, str]] = {
    "items": {
        "product_code": "product_code",
        "description": "description",
    },
    "customers": {
        "customer_id": "customer_id",
        "customer_name": "customer_name",
    },
    "vendors": {
        "vendor_id": "vendor_id",
        "vendor_name": "vendor_name",
    },
    "inventory_snapshots": {
        "branch": "branch",
        "product_code": "product_code",
        "on_hand_qty": "on_hand_qty",
    },
    "gl_entries": {
        "journal_no": "journal_no",
        "line_no": "line_no",
        "gl_account": "gl_account",
        "posting_date": "posting_date",
        "amount": "amount",
    },
}

#: Header context each line row inherits (canonical field <- header field).
_HEADER_CONTEXT: dict[str, dict[str, str]] = {
    "sales_order_lines": {"order_no": "order_no"},
    "purchase_order_lines": {"po_no": "po_no"},
}

#: canonical field <- source field on the detail object's line rows.
_LINE_MAPS: dict[str, dict[str, str]] = {
    "sales_order_lines": {
        "line_no": "line_no",
        "product_code": "product_code",
        "uom": "uom",
        "ordered_qty": "ordered_qty",
        "unit_price": "unit_price",
    },
    "purchase_order_lines": {
        "line_no": "line_no",
        "product_code": "product_code",
        "uom": "uom",
        "ordered_qty": "ordered_qty",
        "unit_cost": "unit_cost",
    },
}

#: entity -> Eclipse surface (spec §3). Entities absent here are plan-only.
_ENTITY_SURFACES: dict[str, SearchSurface] = {
    "items": SearchSurface(endpoint="/Products", license="", watermark_capable=True),
    "customers": SearchSurface(endpoint="/Customers", license="", watermark_capable=True),
    "vendors": SearchSurface(endpoint="/Vendors", license="", watermark_capable=True),
    "gl_entries": SearchSurface(
        endpoint="/GLInquiryDetail",
        license="Accounting API",
        watermark_capable=True,
    ),
    "sales_order_lines": SearchSurface(
        endpoint="/SalesOrders",
        license="Sales Order API",
        watermark_capable=True,
        detail_endpoint="/SalesOrders/{id}",
        detail_id_field="order_no",
        detail_lines_field="LineItems",
    ),
    "purchase_order_lines": SearchSurface(
        endpoint="/PurchaseOrders",
        license="Purchase Order API",
        watermark_capable=True,
        detail_endpoint="/PurchaseOrders/{id}",
        detail_id_field="po_no",
        detail_lines_field="LineItems",
    ),
    "inventory_snapshots": SearchSurface(
        endpoint="/ProductInventoryList",
        license="",
        watermark_capable=False,
    ),
}

#: Plan-only surfaces — the spec verifies these absences; extraction stays a
#: documented plan rather than an improvised read.
_PLAN_SURFACES: dict[str, str] = {
    "invoice_lines": (
        "No /Invoices endpoint exists (spec §3 — verified absence): the AR-side "
        "read is the license-gated (Accounting/AR API license), inquiry-shaped "
        "GET /ARInquiry object, and historical invoice LINES likely need the "
        "hybrid report/file channel negotiated with the Epicor CAM (spec §6) — "
        "the biggest gap vs the canonical model"
    ),
}

#: Per-entity extraction-plan caveats from the integration spec (art_Hp74a48b).
_ENTITY_NOTES: dict[str, str] = {
    "items": (
        "Standard Product family (full CRUD + search; GraphQL is an alternate "
        "richer read, spec §3). Search-based GET needs the tenant's Search "
        "Index built (spec §4)."
    ),
    "customers": (
        "Standard Customer family; ship-to records surface inside the Customer "
        "object (no /ShipTos family exists — spec §3 verified absence). The "
        "deleted flag drops flagged accounts client-side (spec §7.7)."
    ),
    "vendors": (
        "Standard Vendor family (added 9.0.5); vendor part cross-references "
        "ride /VendorPartNumbers — a child pass at onboarding if needed."
    ),
    "sales_order_lines": (
        "Sales Order API license (spec §2.4). Search pages + per-order detail "
        "read flattening the LineItems collection (the documented sub-resource "
        "name; its presence on the detail object confirms at onboarding). "
        "Status is a mutable sub-resource (PUT .../Status) — treat as its own "
        "slowly-changing dimension, not a field diff (spec §7.7); order "
        "changes additionally auditable via /SalesOrders/{oid}/OrderChangeLog."
    ),
    "purchase_order_lines": (
        "Purchase Order API license (spec §2.4). Search pages + per-PO detail "
        "read flattening the LineItems collection (confirmed at onboarding)."
    ),
    "gl_entries": (
        "Accounting API license (spec §2.4). GLInquiry/GLInquiryDetail are the "
        "windowed GL read (spec §4/§6.5); this connector polls GLInquiryDetail "
        "with the same pinned paging + updatedAfter contract as every family — "
        "the posting-period window parameters are the onboarding refinement "
        "once the tenant's docs pin their names, and inquiry backfill depth is "
        "an open tenant-specific question to test early (spec §6.5)."
    ),
    "inventory_snapshots": (
        "Snapshot semantics (spec §4): no watermark possible for quantities — "
        "a full /ProductInventoryList sweep per run on a short cadence, stamped "
        "with the extraction date. Branch granularity is first-class; bin-level "
        "availability and future/history ledgers are per-site additions "
        "(/FutureLedger, /HistoryLedger — 22.1). Quantities carry source UOM "
        "unconverted — never sum across items without UOM normalization; the "
        "canonical conversion-factor column is flagged for the formalization "
        "task (todo_m58JUGS6)."
    ),
    "invoice_lines": (
        "PLAN ONLY — no /Invoices endpoint exists (spec §3, verified absence): "
        "ARInquiry is inquiry-shaped and license-gated; historical invoice "
        "lines need the hybrid channel (report/file extract via the dealer or "
        "Epicor CAM). Plan the hybrid channel now, not after API attempts fail "
        "(spec §6.6)."
    ),
}


class EpicorEclipseConnector(BaseConnector):
    """Epicor Eclipse REST adapter. Credential-gated; dry-runs need no network."""

    erp_id = "epicor_eclipse"
    maturity = ConnectorMaturity.IMPLEMENTED  # coded — but see UNEXERCISED note above
    #: Runaway-paging guard: a tenant whose pinned paging params are silently
    #: ignored would otherwise page forever; refuse past this many pages.
    MAX_PAGES: ClassVar[int] = 10_000
    #: Watermark-incremental or full-refresh per entity — never a wholesale
    #: replacement — so deletes surface only through the scheduled anti-join.
    full_snapshot = False
    delete_handling = DeleteSemantics.ANTI_JOIN
    extraction_notes = (
        "Eclipse REST-only over InterSystems Caché with proprietary session-token "
        "auth (POST /Sessions mints sessionToken + refreshToken; /SessionRefresh "
        "recovers an expired-but-not-deleted session — no TTL is assumed: one "
        "rejection refreshes with a full re-login fallback, a second fails the "
        "run), query-param paging with per-tenant-pinned page-size/page-number "
        "parameters and short-page termination, updatedAfter watermarks on the "
        "families that support them (no webhooks exist — polling is the only "
        "incremental mechanism), a dated /ProductInventoryList snapshot sweep "
        "with no watermark, license-gated endpoint families (SalesOrders, "
        "PurchaseOrders, GLInquiryDetail, ARInquiry, PriceMatrices, "
        "WarehouseTasks), and the Search Index Builder deployment prerequisite "
        "surfaced as its own failure rather than an empty result. The session "
        "request body keys, token transport header, watermark stamp field, and "
        "paging parameter names are per-tenant [D] pins that fail closed on "
        "empty (BisTrack §7.1 pattern) — they live only in each tenant's "
        "deployed API docs. Watermarks are delete-blind: the scheduled "
        "anti-join reconciliation tombstones vanished keys, and customers "
        "flagged deleted drop client-side so the anti-join compares "
        "live-to-live. Not yet exercised against a live tenant; validate field "
        "names, the session wire contract, page semantics, and license bundle "
        "entitlements against the tenant's deployed docs at onboarding."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("product_code",),
        "customers": ("customer_id",),
        "vendors": ("vendor_id",),
        "gl_entries": ("journal_no", "line_no"),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch", "product_code"),
    }
    required_settings: ClassVar[tuple[str, ...]] = (
        # The tenant API engine URL — on-prem is the dealer's own server
        # (http://EclipseServer:Port, default port 5000); Eclipse Cloud is
        # Azure-hosted (spec §1). No https-only gate: on-prem is plain HTTP
        # behind a VPN/reverse proxy.
        "base_url",
        # The Eclipse service identity that POST /Sessions authenticates —
        # provisioned by the dealer admin with the right file/auth KEYS, not
        # just role membership (spec §2.3/§2.5); credentials mint a session,
        # they are not basic-auth headers on data calls.
        "username",
        "password",
        # [D] pins — per-tenant wire contract the spec marks [G] (§2.1/§5),
        # living only in the tenant's deployed API docs; fail-closed on empty
        # (BisTrack §7.1 pattern).
        "session_user_field",  # POST /Sessions body key for the username
        "session_password_field",  # POST /Sessions body key for the password
        "token_header",  # header carrying the sessionToken on data calls
        "watermark_field",  # row field carrying the last-change stamp
        "page_size_param",  # query param name for the page size
        "page_number_param",  # query param name for the page index
        "page_number_start",  # the tenant's first page index (0- or 1-based)
        "page_size",  # rows per page (no public ceiling — pinned per tenant)
        # The tenant's go-live date bounds transaction backfills (house
        # convention shared with the DMSi connector).
        "go_live_date",
    )

    def __init__(
        self, source: SourceConfig, store: ControlPlaneStore, config: ControlPlaneConfig
    ) -> None:
        super().__init__(source, store, config)
        self._http_client: httpx.Client | None = None
        self._session_token: str | None = None
        self._refresh_token: str | None = None
        self._max_incremental_seen: dict[str, str] = {}
        self._rows_yielded: dict[str, int] = {}
        self._stamps_observed: dict[str, int] = {}
        self._recovered_this_walk = False

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    def validate_config(self) -> list[str]:
        missing = [f for f in self.required_settings if not self.source.settings.get(f)]
        if missing:
            return [
                f"missing required Epicor Eclipse settings: {', '.join(missing)} — the "
                "session/paging wire contract is per-tenant ([D]: the POST /Sessions "
                "body keys, session-token transport header, watermark stamp field, and "
                "paging parameter names live only in the tenant's deployed API docs at "
                "http://EclipseServer:Port; spec §2.1/§5); see .env.example; source "
                "stays disabled until configured"
            ]
        problems: list[str] = []
        page_size = self.source.settings.get("page_size") or ""
        if not page_size.isdigit() or int(page_size) <= 0:
            problems.append("setting 'page_size' must be a positive integer")
        start = self.source.settings.get("page_number_start") or ""
        if not start.isdigit():
            problems.append(
                "setting 'page_number_start' must be a non-negative integer — the "
                "tenant's first page index (0- or 1-based per the deployed docs); a "
                "wrong start silently skips the first page"
            )
        go_live = self._go_live_date()
        if go_live is None:
            problems.append("setting 'go_live_date' must be an ISO date (YYYY-MM-DD)")
        elif go_live > _today():
            problems.append(
                "setting 'go_live_date' is in the future — backfill windows need a past date"
            )
        return problems

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        notes = "\n".join(
            part for part in (_ENTITY_NOTES.get(entity, ""), self.extraction_notes) if part
        )
        surface = _ENTITY_SURFACES.get(entity)
        if surface is None:
            planned = _PLAN_SURFACES.get(entity)
            if planned is None:
                raise ConnectorError(
                    f"entity '{entity}' has no Epicor Eclipse mapping; known: "
                    f"{', '.join(sorted(self.natural_key_fields))}"
                )
            return ExtractionPlan(entity=entity, surface=planned, incremental_key=None, notes=notes)
        text = (
            f"Eclipse REST: GET {surface.endpoint} — query-param pages "
            f"(pinned page-size/page-number params; short-page termination)"
        )
        if surface.license:
            text += f"; requires the {surface.license} license (spec §2.4)"
        if surface.detail_endpoint:
            text += (
                f", then GET {surface.detail_endpoint} per record flattening the "
                f"{surface.detail_lines_field} collection"
            )
        if surface.watermark_capable:
            incremental = "updatedAfter on the pinned watermark_field stamp"
        else:
            incremental = (
                "none — snapshot sweep (no watermark possible for quantities, spec §4)"
                if entity == "inventory_snapshots"
                else None
            )
        return ExtractionPlan(entity=entity, surface=text, incremental_key=incremental, notes=notes)

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        self._require_config()
        key_fields = self.natural_key_fields[entity]
        self._rows_yielded[entity] = 0
        self._stamps_observed[entity] = 0
        self._recovered_this_walk = False
        seen: set[str] = set()
        for record in self._iter_entity_records(entity, mode, watermark):
            self._rows_yielded[entity] += 1
            natural_id = natural_id_for(key_fields, record)
            if natural_id in seen:
                # Session recovery can reissue the same page call — the walk
                # must never double-stage a row (DMSi restart discipline).
                continue
            seen.add(natural_id)
            yield record

    def _iter_entity_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        surface = _ENTITY_SURFACES.get(entity)
        if surface is None:
            raise ConnectorNotImplemented(
                f"epicor_eclipse extraction for '{entity}' stays a documented plan: "
                f"{_PLAN_SURFACES[entity]}"
            )
        if surface.detail_endpoint:
            yield from self._walk_document_lines(entity, surface, mode, watermark)
            return
        yield from self._walk_list(entity, surface, mode, watermark)

    def _walk_list(
        self,
        entity: str,
        surface: SearchSurface,
        mode: ExtractionMode,
        watermark: str | None,
    ) -> Iterator[dict[str, object]]:
        params = self._base_params(entity, mode, watermark)
        page = self._page_number_start()
        for _ in range(self.MAX_PAGES):
            page_params = {**params, self.source.settings["page_number_param"]: page}
            payload = self._call_with_recovery(
                entity, surface.endpoint, self._endpoint_url(surface.endpoint), page_params
            )
            rows = self._rows_of(entity, surface.endpoint, payload)
            if surface.watermark_capable:
                self._observe_incremental(entity, rows)
            yield from self._records_from_rows(entity, surface.endpoint, payload, rows)
            if len(rows) < self._page_size():
                return
            page += 1
        raise ConnectorError(
            f"Epicor Eclipse paging on {surface.endpoint} exceeded {self.MAX_PAGES} "
            "pages — refusing to spin (check the pinned paging contract; a tenant "
            "ignoring the paging params pages forever)"
        )

    def _walk_document_lines(
        self,
        entity: str,
        surface: SearchSurface,
        mode: ExtractionMode,
        watermark: str | None,
    ) -> Iterator[dict[str, object]]:
        """Search pages -> per-record detail GET -> flatten the line collection.

        The spec's multi-tier read pattern (list -> detail -> line children,
        §3): every join lives in this layer, never in the API.
        """
        assert surface.detail_endpoint and surface.detail_id_field and surface.detail_lines_field
        params = self._base_params(entity, mode, watermark)
        page = self._page_number_start()
        for _ in range(self.MAX_PAGES):
            page_params = {**params, self.source.settings["page_number_param"]: page}
            payload = self._call_with_recovery(
                entity, surface.endpoint, self._endpoint_url(surface.endpoint), page_params
            )
            rows = self._rows_of(entity, surface.endpoint, payload)
            if surface.watermark_capable:
                self._observe_incremental(entity, rows)
            for header in rows:
                record_id = header.get(surface.detail_id_field)
                if record_id is None:
                    self._quarantine_page(
                        entity,
                        surface.endpoint,
                        json.dumps(payload, default=str).encode("utf-8"),
                        RC_MALFORMED_PAGE,
                        f"search row missing {surface.detail_id_field} — cannot fetch "
                        "the detail object without its id",
                    )
                    raise ConnectorError(
                        f"Epicor Eclipse search row on {surface.endpoint} is missing "
                        f"{surface.detail_id_field} — cannot read the detail object"
                    )
                detail_url = self._endpoint_url(
                    surface.detail_endpoint.replace("{id}", str(record_id))
                )
                detail = self._call_with_recovery(entity, surface.detail_endpoint, detail_url, None)
                detail_rows = self._detail_lines_of(
                    entity, surface.detail_endpoint, detail, surface.detail_lines_field
                )
                yield from self._line_records_from(
                    entity, surface, payload, header, str(record_id), detail_rows
                )
            if len(rows) < self._page_size():
                return
            page += 1
        raise ConnectorError(
            f"Epicor Eclipse paging on {surface.endpoint} exceeded {self.MAX_PAGES} "
            "pages — refusing to spin (check the pinned paging contract)"
        )

    def _line_records_from(
        self,
        entity: str,
        surface: SearchSurface,
        payload: dict[str, object] | list[dict[str, object]],
        header: dict[str, object],
        record_id: str,
        detail_rows: list[dict[str, object]],
    ) -> Iterator[dict[str, object]]:
        """Join one header with its detail-object line rows (in-payload join)."""
        assert surface.detail_id_field
        header_context = _HEADER_CONTEXT[entity]
        line_map = _LINE_MAPS[entity]
        key_fields = self.natural_key_fields[entity]
        payload_bytes = json.dumps(payload, default=str).encode("utf-8")
        for line in detail_rows:
            record: dict[str, object] = {
                canon: header.get(source) for canon, source in header_context.items()
            }
            record.update({canon: line.get(source) for canon, source in line_map.items()})
            missing = [f for f in key_fields if record.get(f) is None]
            if missing:
                self._quarantine_page(
                    entity,
                    surface.endpoint,
                    payload_bytes,
                    RC_MALFORMED_PAGE,
                    f"{surface.detail_lines_field} row for {surface.detail_id_field}="
                    f"{record_id!r} missing natural-key field(s) {missing} — refusing to "
                    "stage a row without its natural identity",
                )
                raise ConnectorError(
                    f"Epicor Eclipse line row on {surface.endpoint} "
                    f"({surface.detail_id_field}={record_id!r}) is missing natural-key "
                    f"field(s) {missing}"
                )
            yield record

    def source_key_inventory(self, entity: str) -> set[str]:
        """Key scan of the entity set — the anti-join's source side."""
        self._require_config()
        key_fields = self.natural_key_fields[entity]
        return {
            natural_id_for(key_fields, record)
            for record in self._iter_entity_records(entity, ExtractionMode.BACKFILL, None)
        }

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        surface = _ENTITY_SURFACES.get(entity)
        if (
            mode is not ExtractionMode.INCREMENTAL
            or surface is None
            or not surface.watermark_capable
        ):
            return watermark_before
        rows = self._rows_yielded.get(entity, 0)
        stamps = self._stamps_observed.get(entity, 0)
        if rows and not stamps:
            raise ConnectorError(
                f"Epicor Eclipse staged {rows} {entity} rows but observed no "
                f"'{self.source.settings.get('watermark_field')}' stamps — the pinned "
                "watermark_field does not exist on this payload; refusing to persist a "
                "checkpoint (a wrong field name would silently re-pull forever)"
            )
        # Cross-page checkpoint: the max stamp observed across every page —
        # the base persists it only after success (BC cross-company precedent).
        return self._max_incremental_seen.get(entity, watermark_before)

    # ------------------------------------------------------------------
    # Eclipse plumbing (session, paging, quarantine)
    # ------------------------------------------------------------------

    def _base_params(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> dict[str, object]:
        """Query params shared by every page of a walk: the pinned page size
        plus, on watermark-capable families, ``updatedAfter`` (spec §4: "most
        endpoints support the 'updatedAfter' query parameter" — always SENT;
        backfill sends the epoch-equivalent)."""
        surface = _ENTITY_SURFACES[entity]
        params: dict[str, object] = {self.source.settings["page_size_param"]: self._page_size()}
        if surface.watermark_capable:
            params["updatedAfter"] = self._changed_since(mode, watermark)
        return params

    def _changed_since(self, mode: ExtractionMode, watermark: str | None) -> str:
        if mode is ExtractionMode.INCREMENTAL and watermark:
            return watermark
        return EPOCH_UPDATED_AFTER

    def _call_with_recovery(
        self, entity: str, label: str, url: str, params: dict[str, object] | None
    ) -> dict[str, object] | list[dict[str, object]]:
        """One data call with the single-recovery ladder: a rejected session
        gets /SessionRefresh (full POST /Sessions fallback); a SECOND
        rejection fails the run. Page state is client-side, so recovery
        resumes the same call."""
        try:
            return self._call(entity, label, url, params)
        except SessionRejected:
            if self._recovered_this_walk:
                raise
            self._recovered_this_walk = True
            self._recover_session()
            return self._call(entity, label, url, params)

    def _call(
        self, entity: str, label: str, url: str, params: dict[str, object] | None
    ) -> dict[str, object] | list[dict[str, object]]:
        """One GET: session headers, 429/503 backoff honoring Retry-After,
        session rejection as :class:`SessionRejected`, the Search Index
        condition as :class:`SearchIndexNotBuilt`, quarantine fail-closed on
        unparseable bodies."""
        if not self._session_token:
            self._login()
        delay = BACKOFF_SECONDS
        for attempt in range(1, MAX_RETRIES + 1):
            response = self._client().get(url, params=params, headers=self._headers())
            if response.status_code in (429, 503):
                # No rate limits are published (spec §5) — this governor is
                # ours: back off honoring Retry-After, then give up honestly.
                if attempt == MAX_RETRIES:
                    raise ConnectorError(
                        f"Epicor Eclipse returned {response.status_code} on {label} "
                        f"after {MAX_RETRIES} backoff attempts"
                    )
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else delay
                time.sleep(delay)
                delay *= 2
                continue
            if response.status_code in (401, 403):
                raise SessionRejected(
                    f"Epicor Eclipse rejected the session token on {label} "
                    f"({response.status_code}) — sessions expire with no public TTL "
                    "(spec §2.1)"
                )
            if response.status_code >= 400:
                body_text = response.text or ""
                lowered = body_text.lower()
                if "search index builder" in lowered or "first index the records" in lowered:
                    # Distinct from an empty result on purpose (spec §4): run
                    # the Search Index Builder for the entity — a deployment
                    # prerequisite, not "no data".
                    raise SearchIndexNotBuilt(
                        f"Epicor Eclipse rejected {label}: the tenant's Search Index is "
                        "not built for this entity — run the Search Index Builder "
                        "(spec §4); this is a deployment prerequisite, not an empty result"
                    )
                raise ConnectorError(
                    f"Epicor Eclipse returned {response.status_code} on {label}: {body_text[:200]}"
                )
            try:
                payload: object = response.json()
            except ValueError as exc:
                self._quarantine_page(
                    entity, label, response.content, RC_UNPARSEABLE_JSON, str(exc)
                )
                raise ConnectorError(
                    f"malformed Epicor Eclipse response on {label}: body is not JSON"
                ) from exc
            if not isinstance(payload, (dict, list)):
                self._quarantine_page(
                    entity,
                    label,
                    json.dumps(payload, default=str).encode("utf-8"),
                    RC_MALFORMED_PAGE,
                    "response must be a JSON object or array",
                )
                raise ConnectorError(
                    f"malformed Epicor Eclipse response on {label}: not an object or array"
                )
            return payload
        raise ConnectorError("unreachable: retry loop must return or raise")  # pragma: no cover

    def _rows_of(
        self, entity: str, label: str, payload: dict[str, object] | list[dict[str, object]]
    ) -> list[dict[str, object]]:
        """A search page is a JSON array of row objects — the least-structure
        claim for an undocumented envelope ([G] §5); anything else quarantines
        fail-closed so the real envelope shape is fixed at onboarding, not
        guessed around."""
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            self._quarantine_page(
                entity,
                label,
                json.dumps(payload, default=str).encode("utf-8"),
                RC_MALFORMED_PAGE,
                "a search page must be a JSON array of objects — a different envelope "
                "is per-tenant contract to confirm against the deployed docs",
            )
            raise ConnectorError(
                f"malformed Epicor Eclipse page for {entity} on {label}: expected a JSON "
                "array of row objects"
            )
        return payload

    def _detail_lines_of(
        self,
        entity: str,
        label: str,
        payload: dict[str, object] | list[dict[str, object]],
        lines_field: str,
    ) -> list[dict[str, object]]:
        """The detail object's line collection (the documented sub-resource
        name, e.g. LineItems) — missing or non-list quarantines fail-closed:
        silently staging a header with no lines would drop the document's
        children."""
        if not isinstance(payload, dict) or not isinstance(payload.get(lines_field), list):
            self._quarantine_page(
                entity,
                label,
                json.dumps(payload, default=str).encode("utf-8"),
                RC_MALFORMED_PAGE,
                f"detail object must carry a '{lines_field}' list — a missing or "
                "different collection name is per-tenant contract to confirm at "
                "onboarding",
            )
            raise ConnectorError(
                f"malformed Epicor Eclipse detail object on {label}: expected a "
                f"'{lines_field}' list"
            )
        lines = payload[lines_field]
        if not all(isinstance(row, dict) for row in lines):
            self._quarantine_page(
                entity,
                label,
                json.dumps(payload, default=str).encode("utf-8"),
                RC_MALFORMED_PAGE,
                f"'{lines_field}' must be a list of objects",
            )
            raise ConnectorError(
                f"malformed Epicor Eclipse detail object on {label}: "
                f"'{lines_field}' must be a list of objects"
            )
        return lines

    def _records_from_rows(
        self,
        entity: str,
        label: str,
        payload: dict[str, object] | list[dict[str, object]],
        rows: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        """Map one flat page to canonical records. Customers flagged deleted
        drop here (so extraction and the key scan compare live-to-live); any
        row missing a natural-key field quarantines the page fail-closed — a
        row without its natural identity must never stage (provenance would
        be garbage)."""
        field_map = _FIELD_MAPS[entity]
        key_fields = self.natural_key_fields[entity]
        payload_bytes = json.dumps(payload, default=str).encode("utf-8")
        records: list[dict[str, object]] = []
        for row in rows:
            if entity == "customers" and _is_deleted_customer(row):
                continue
            record: dict[str, object] = {
                canon: row.get(source) for canon, source in field_map.items()
            }
            if entity == "inventory_snapshots":
                # Snapshot semantics (spec §4): the snapshot date is stamped
                # at extraction time — the sweep has no watermark.
                record["snapshot_date"] = _today().isoformat()
            missing = [f for f in key_fields if record.get(f) is None]
            if missing:
                self._quarantine_page(
                    entity,
                    label,
                    payload_bytes,
                    RC_MALFORMED_PAGE,
                    f"row missing natural-key field(s) {missing} — refusing to stage a "
                    "row without its natural identity",
                )
                raise ConnectorError(
                    f"Epicor Eclipse row on {label} is missing natural-key field(s) {missing}"
                )
            records.append(record)
        return records

    def _observe_incremental(self, entity: str, rows: list[dict[str, object]]) -> None:
        """Track the max watermark stamp seen — across all pages."""
        field = self.source.settings.get("watermark_field") or ""
        for row in rows:
            observed = row.get(field)
            if observed is None:
                continue
            self._stamps_observed[entity] += 1
            text = str(observed)
            current = self._max_incremental_seen.get(entity)
            if current is None or _eclipse_stamp(text) > _eclipse_stamp(current):
                self._max_incremental_seen[entity] = text

    def _login(self) -> None:
        """POST /Sessions (spec §2.1): the service-identity credentials in the
        JSON body under the tenant-pinned field names ([D] — the request body
        is not publicly documented, so the keys are REQUIRED settings,
        fail-closed on empty); the response's sessionToken is mandatory (fail
        closed on empty — never authenticate with nothing) and refreshToken is
        captured when present."""
        settings = self.source.settings
        body = {
            settings["session_user_field"]: settings["username"],
            settings["session_password_field"]: settings["password"],
        }
        response = self._client().post(
            self._endpoint_url(SESSIONS_PATH),
            content=json.dumps(body),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        if response.status_code >= 400:
            raise ConnectorError(
                f"Epicor Eclipse session creation failed ({response.status_code}): "
                f"{(response.text or '')[:200]}"
            )
        try:
            payload: object = response.json()
        except ValueError as exc:
            raise ConnectorError("Epicor Eclipse session response is not JSON") from exc
        if not isinstance(payload, dict):
            raise ConnectorError("Epicor Eclipse session response is not a JSON object")
        token = payload.get("sessionToken")
        # Fail closed on an empty token — never authenticate with nothing.
        # The field name is the Aurinko-reported convention, confirmed against
        # the tenant's deployed docs at onboarding (spec §2.1 [G]).
        if not isinstance(token, str) or not token:
            raise ConnectorError(
                "Epicor Eclipse session response carried no sessionToken — refusing "
                "to authenticate with an empty token; the response field names are "
                "per-tenant contract (spec §2.1 [G]) — confirm against the tenant's "
                "deployed API docs"
            )
        self._session_token = token
        refresh_token = payload.get("refreshToken")
        self._refresh_token = (
            refresh_token if isinstance(refresh_token, str) and refresh_token else None
        )

    def _refresh(self) -> None:
        """POST /SessionRefresh (spec §2.1): "refresh the session that is
        expired, but not deleted". The session identifies itself the same way
        data calls do — the current token in the pinned header ([I], confirmed
        at onboarding). A 2xx response adopts a fresh sessionToken when
        carried; a tokenless 2xx keeps the current one (the same session,
        refreshed in place). Any failure raises — the caller falls back to a
        full re-login."""
        if not self._session_token:
            raise ConnectorError("Epicor Eclipse refresh requires a current session token")
        response = self._client().post(
            self._endpoint_url(SESSION_REFRESH_PATH), headers=self._headers()
        )
        if response.status_code >= 400:
            raise ConnectorError(
                f"Epicor Eclipse session refresh failed ({response.status_code}) — the "
                "session may be deleted; falling back to a full re-login"
            )
        try:
            payload: object = response.json()
        except ValueError:
            return  # tokenless 2xx: the same session continues, refreshed in place
        if not isinstance(payload, dict):
            return
        token = payload.get("sessionToken")
        if isinstance(token, str) and token:
            self._session_token = token
        refresh_token = payload.get("refreshToken")
        if isinstance(refresh_token, str) and refresh_token:
            self._refresh_token = refresh_token

    def _recover_session(self) -> None:
        """One recovery attempt for a rejected session (spec §2.1): the
        documented /SessionRefresh first (it covers "expired, but not
        deleted"), a full POST /Sessions re-login on refresh failure. Both
        failing raises — the run never continues unauthenticated."""
        try:
            self._refresh()
            return
        except ConnectorError:
            self._session_token = None
            self._refresh_token = None
        self._login()

    def _quarantine_page(
        self, entity: str, label: str, body: bytes, reason_code: str, detail: str
    ) -> None:
        """Persist an unreadable API page with a machine-readable reason
        (csv_sftp/NetSuite/P21/DMSi parity): the run fails, but the offending
        body is preserved for inspection instead of being dropped silently."""
        file_name = f"{entity}-{uuid.uuid4().hex}.json"
        target = self.config.quarantine_root / self.source.source_id / "api_pages" / file_name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        self.store.record_quarantine(
            QuarantineRecord(
                source_id=self.source.source_id,
                batch_id=None,
                file_name=file_name,
                reason_code=reason_code,
                detail=f"{detail} (from {label})",
                quarantine_path=str(target),
                quarantined_at=datetime.now(UTC),
            )
        )

    def _require_config(self) -> None:
        """Fail closed BEFORE any network attempt — inert without credentials."""
        problems = self.validate_config()
        if problems:
            raise ConnectorNotConfigured(
                f"source {self.source.source_id} ({self.erp_id}) is not configurable: "
                + "; ".join(problems)
            )

    def _headers(self) -> dict[str, str]:
        if not self._session_token:
            raise ConnectorError(
                "Epicor Eclipse data calls require an active session (POST /Sessions)"
            )
        return {
            self.source.settings["token_header"]: self._session_token,
            "Accept": "application/json",
        }

    def _endpoint_url(self, path: str) -> str:
        base = self.source.settings["base_url"].rstrip("/")
        return f"{base}{path}"

    def _go_live_date(self) -> dt.date | None:
        raw = self.source.settings.get("go_live_date") or ""
        try:
            return dt.date.fromisoformat(raw)
        except ValueError:
            return None

    def _page_size(self) -> int:
        raw = self.source.settings.get("page_size") or ""
        if raw.isdigit() and int(raw) > 0:
            return int(raw)
        raise ConnectorError(
            "setting 'page_size' must be a positive integer — the page size is a "
            "per-tenant [D] pin (no public ceiling exists, spec §5)"
        )

    def _page_number_start(self) -> int:
        raw = self.source.settings.get("page_number_start") or ""
        if raw.isdigit():
            return int(raw)
        raise ConnectorError(
            "setting 'page_number_start' must be a non-negative integer — a wrong "
            "first-page index silently skips the tenant's first page"
        )

    def _client(self) -> httpx.Client:
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=60.0)
        return self._http_client
