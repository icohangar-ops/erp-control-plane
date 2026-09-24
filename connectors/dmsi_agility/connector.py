"""dmsi_agility — DMSi Agility extraction (AgilityPublic REST).

STATUS: implemented against the documented AgilityPublic surface (Session/Login
issues the ContextId; every data call carries ``ContextId`` + ``Branch`` headers
with ``Content-Type: application/json``; chunk-pointer paging walks
``ChunkStartPointer`` <- ``NextChunkStartPointer`` until ``MoreResultsAvailable``
is false; ``FetchOnlyChangedSince`` watermarks the eight list methods that
document it) but NOT exercised against a live dealer — per the locked
decisions, no fabricated API behavior ships as tested. Field names the spec
quotes verbatim (ItemCode, LastChanged, OrderID, InvoiceNumber,
dtOrder/dtOrderDetail, dtInvoiceDetailResponse) are mapped verbatim; rowset
keys and remaining field names are the reference's PascalCase conventions and
are validated against the tenant's ``AgilityVersion`` at onboarding — a
mismatch fails closed (missing rowsets quarantine, missing key fields refuse to
stamp) rather than staging nothing. Run ``python -m connectors.cli plan
--source <id>`` (dry-run) before any live call; it needs no network.

Extraction notes (integration spec "DMSi Agility — Connector Mechanisms",
art_4U49qR1B):
- Auth is a per-dealer Agility user, not API keys (spec §2): Session/Login
  takes LoginID/Password and returns SessionContextId + InitialBranch. A
  context expires unused — 4 h default, dealer-configurable to a 24 h max —
  so no TTL is assumed: one rejection re-logins and restarts the entity's
  chunk walk from pointer 0 (chunk state is server-side per method run, so
  resume-from-pointer after re-login needs re-validation, spec §4); a second
  rejection fails the run. The walked entity dedupes by natural id so the
  restart never double-stages rows.
- Orders and invoices are customer-scoped (spec §3 rows 7-8): ``CustomerID``
  is required and ``<all>`` is only valid with ``SearchBy`` — a targeted
  lookup, never a bulk path. The configured customer list (required setting,
  fail-closed on empty) scopes every order/invoice pull.
- Invoices have no changed-since filter: extraction slides
  ``InvoiceDateRangeStart/End`` windows (trailing ``invoice_window_days`` for
  incremental, go-live-forward windows for backfill — spec §4/§6).
- GL transactions and PO lists have NO AgilityPublic service (spec §3 rows
  9/11 — absences verified against the full method inventory). Both stay
  documented plans: GL rides the hybrid vendor-mediated channel (Agility's
  embedded Data Warehouse, report exports, or BInformed FTP), POs extract one
  ID at a time via ``PurchaseOrderGet`` from an ID source outside the API.
- Chunk ceilings are per-dealer System Config (Default/Max chunk size),
  invisible until discovery (spec §5); ``record_fetch_limit`` starts at the
  Appian-tuned 500. Heavy pulls land on the dealer's production OpenEdge
  database — extraction is sequential and schedules off-peak dealer time.
- ``ItemsList`` prices are computed against the API user's default customer
  (spec §7) — the item master walks with ``IncludePriceData`` off, and
  pricing extraction stays customer-scoped (out of this connector's canonical
  entity set). The ``ItemAuditResults`` review rule applies to pricing calls,
  which this connector does not make.
- Casing is significant: ``ShiptoSequence`` miscased as ``ShipToSequence``
  silently processes as null (spec §5).
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

LOGIN_SERVICE = "Session"
LOGIN_METHOD = "Login"

#: Backfill watermark for the watermark-capable methods — the spec's
#: "epoch-equivalent" FetchOnlyChangedSince that captures everything (§6 step 2).
EPOCH_CHANGED_SINCE = "1900-01-01T00:00:00"

#: Reason codes recorded on quarantined API pages (csv_sftp/NetSuite/P21/BC parity).
RC_MALFORMED_PAGE = "MALFORMED_PAGE"
RC_UNPARSEABLE_JSON = "UNPARSEABLE_JSON"

#: Per-call fetch ceiling. The published reference documents no rate limits or
#: quotas (spec §5) — chunk ceilings are per-dealer System Config and the one
#: real integration on record tuned 500 records/call (Appian, spec §5).
DEFAULT_RECORD_FETCH_LIMIT = 500

#: Incremental invoice window (spec §4: "re-pull trailing 7-14 days daily").
DEFAULT_INVOICE_WINDOW_DAYS = 14


class SessionExpired(ConnectorError):
    """The per-dealer ContextId was rejected — re-login and restart the walk."""


def _today() -> dt.date:
    """Extraction clock (monkeypatched in fixtures for deterministic windows)."""
    return dt.date.today()


def _dmsi_timestamp(raw: str) -> dt.datetime:
    """Parse a ``LastChanged``/``FetchOnlyChangedSince`` stamp (spec §4 format
    ``yyyy-mm-ddThh:mm:ss``).

    Comparison parses values because stamp precision may vary; the raw text
    rides the checkpoint verbatim. The spec documents no offset — stamps
    compare as dealer-local naive values, with the timezone convention
    confirmed at onboarding.
    """
    try:
        return dt.datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ConnectorError(
            f"unparseable DMSi Agility timestamp {raw!r} — refusing to advance a "
            "watermark on it (storing one would silently skip records)"
        ) from exc


@dataclass(frozen=True)
class ListSurface:
    """A flat list method: one rowset per chunked response.

    The spec quotes child-table names verbatim (dtOrder/dtOrderDetail,
    dtInvoiceDetailResponse, dtItemDimensionResponse) but not the top-level
    rowset keys of the list methods — those follow the reference's ``dt``
    conventions and are confirmed at onboarding; a mismatch quarantines
    fail-closed instead of staging nothing.
    """

    service: str
    method: str
    rows_key: str
    #: True when the method documents FetchOnlyChangedSince — exactly eight
    #: list methods do (spec §4): CustomersList, CustomerBilltosList,
    #: CustomerShiptosList, SalesOrderList, CreditMemoList, SuppliersList,
    #: SupplierRemittosList, SupplierShipfromsList.
    watermark_capable: bool
    #: Item walk carries item-branch quantities (the spec §4 snapshot option).
    include_quantity_data: bool = False


@dataclass(frozen=True)
class DocumentSurface:
    """A header/detail list method: parent and child rowsets in one response."""

    service: str
    method: str
    header_key: str
    detail_key: str
    #: Header key the child rows reference — spec-verbatim (§3 rows 7-8).
    join_field: str
    watermark_capable: bool


#: canonical field <- source field per entity. Spec-verbatim names are marked
#: [V] in the module docstring; the rest are the reference's PascalCase
#: conventions, confirmed against the tenant's AgilityVersion at onboarding.
_FIELD_MAPS: dict[str, dict[str, str]] = {
    "items": {
        "item_no": "ItemCode",
        "description": "ItemDescription",
        "category": "ItemGroupMajor",
        "uom": "DisplayUOM",
        "item_status": "StockStatusCode",
    },
    "customers": {
        "customer_no": "CustomerID",
        "customer_name": "CustomerName",
        "credit_limit": "CreditLimit",
        "open_ar_amt": "OpenARAmount",
        "open_so_amt": "OpenSOAmount",
        "branch_code": "HomeBranch",
        "last_changed": "LastChanged",
    },
    "vendors": {
        "vendor_no": "SupplierID",
        "vendor_name": "SupplierName",
        "last_changed": "LastChanged",
    },
    "inventory_snapshots": {
        "item_no": "ItemCode",
        "on_hand_qty": "QuantityOnHand",
        "available_qty": "QuantityAvailable",
        "on_order_qty": "QuantityOnOrder",
        "committed_qty": "QuantityCommitted",
    },
}

#: Header context each line row inherits (canonical field <- header field).
_ORDER_HEADER_MAP: dict[str, str] = {
    "order_no": "OrderID",
    "order_date": "OrderDate",
    "customer_no": "CustomerID",
    "last_changed": "LastChanged",
}
_ORDER_DETAIL_MAP: dict[str, str] = {
    "line_no": "LineNo",
    "item_no": "ItemCode",
    "uom": "UOM",
    "ordered_qty": "QuantityOrdered",
    "filled_qty": "QuantityFilled",
    "unit_price": "UnitPrice",
    "order_status": "StatusCode",
}
_INVOICE_HEADER_MAP: dict[str, str] = {
    "invoice_no": "InvoiceNumber",
    "invoice_date": "InvoiceDate",
    "customer_no": "CustomerID",
}
_INVOICE_DETAIL_MAP: dict[str, str] = {
    "line_no": "LineNo",
    "item_no": "ItemCode",
    "uom": "UOM",
    "invoiced_qty": "QuantityInvoiced",
    "unit_price": "UnitPrice",
}

_FIELD_MAPS["sales_order_lines"] = {**_ORDER_HEADER_MAP, **_ORDER_DETAIL_MAP}
_FIELD_MAPS["invoice_lines"] = {**_INVOICE_HEADER_MAP, **_INVOICE_DETAIL_MAP}

#: entity -> AgilityPublic surface (spec §3 rows 1-10). Entities absent here
#: are plan-only: the spec verifies the methods do NOT exist.
_ENTITY_SURFACES: dict[str, ListSurface | DocumentSurface] = {
    "items": ListSurface(
        service="Inventory",
        method="ItemsInChunksList",
        rows_key="dtItemsInChunksListResponse",
        watermark_capable=False,
    ),
    "customers": ListSurface(
        service="Customer",
        method="CustomersList",
        rows_key="dtCustomersListResponse",
        watermark_capable=True,
    ),
    "vendors": ListSurface(
        service="Supplier",
        method="SuppliersList",
        rows_key="dtSuppliersListResponse",
        watermark_capable=True,
    ),
    "inventory_snapshots": ListSurface(
        service="Inventory",
        method="ItemsInChunksList",
        rows_key="dtItemsInChunksListResponse",
        watermark_capable=False,
        include_quantity_data=True,
    ),
    "sales_order_lines": DocumentSurface(
        service="Orders",
        method="SalesOrderList",
        header_key="dtOrder",
        detail_key="dtOrderDetail",
        join_field="OrderID",
        watermark_capable=True,
    ),
    "invoice_lines": DocumentSurface(
        service="AccountsReceivable",
        method="InvoicesList",
        header_key="dtInvoicesListResponse",
        detail_key="dtInvoiceDetailResponse",
        join_field="InvoiceNumber",
        watermark_capable=False,
    ),
}

#: Plan-only surfaces — the spec verifies these methods DO NOT exist on
#: AgilityPublic (§3 rows 9/11); extraction stays a documented plan rather
#: than improvising a bulk path the API does not offer.
_PLAN_SURFACES: dict[str, str] = {
    "purchase_order_lines": (
        "AgilityPublic REST: Purchasing · PurchaseOrderGet (single PurchaseOrderID "
        "per call — no list method exists, spec §3 row 9); bulk extraction needs "
        "an ID source outside the API (received-shipment/EDI history, the "
        "dealer's Data Warehouse, or a DMSi report extract)"
    ),
    "gl_entries": (
        "No AgilityPublic service (spec §3 row 11): GL rides the hybrid "
        "vendor-mediated channel — Agility's embedded Data Warehouse, report "
        "exports, or BInformed FTP (spec §1/§4/§7)"
    ),
}

#: Per-entity extraction-plan caveats from the integration spec (art_4U49qR1B).
_ENTITY_NOTES: dict[str, str] = {
    "items": (
        "Pure master walk: IncludePriceData off — ItemsList prices are computed "
        "against the API user's default customer (spec §7), so pricing extracts "
        "only through the customer-scoped pricing methods, and the ItemAuditResults "
        "review rule applies there. Dimension items expand as dtItemDimensionResponse "
        "child rows keyed by ItemCode (spec §3 row 1) — modeled at onboarding "
        "validation. No changed-since filter: full chunk walk per run."
    ),
    "customers": (
        "FetchOnlyChangedSince watermark on LastChanged (spec §4 — one of the "
        "exactly eight methods that document it). Visibility is bounded by the "
        "integration user's data allocations — an incomplete pull may be a "
        "permissions problem, not a bug (spec §7)."
    ),
    "vendors": (
        "SuppliersList watermark (spec §4); remit-to and ship-from ride their own "
        "lists keyed to the supplier (spec §3 row 6)."
    ),
    "sales_order_lines": (
        "Customer-scoped: CustomerID required and <all> is not a bulk path (spec "
        "§3 row 7); headers and lines join on OrderID (dtOrder/dtOrderDetail). "
        "IncludeOpenOrders/InvoicedOrders/CanceledOrders all sent — status "
        "transitions ride LastChanged edits; a date-range safety net is a per-site "
        "option (spec §4). Typed inputs are sent explicitly, never omitted (spec §4). "
        "A chunk boundary that splits a document's rows fails closed (quarantine) — "
        "tune RecordFetchLimit at onboarding."
    ),
    "invoice_lines": (
        "Customer-scoped with sliding InvoiceDateRangeStart/End windows — no "
        "changed-since filter exists (spec §4); ShiptoSequence=0 covers all "
        "ship-tos and IncludeOnlyOpenInvoices=false includes closed history "
        "(spec §6). Headers join dtInvoiceDetailResponse on InvoiceNumber. Close "
        "windows only after a later full-window pull reconciles against "
        "BalancesList totals (spec §4)."
    ),
    "inventory_snapshots": (
        "Snapshot semantics only (spec §4): a quantity-inclusive item walk "
        "(IncludeQuantityData on) stored as a dated snapshot — extract and "
        "key-scan stamp the extraction date, so the anti-join compares "
        "same-snapshot keys. List methods return the logged-in branch's data, so "
        "branch_code is the session's InitialBranch; multi-branch inventory needs "
        "one context per branch. Quantities carry source UOM unconverted — never "
        "sum across items without UOM normalization (spec §7); the canonical "
        "conversion-factor column is flagged for the formalization task "
        "(todo_m58JUGS6)."
    ),
    "purchase_order_lines": (
        "PLAN ONLY — no AgilityPublic list method exists (spec §3 row 9, verified "
        "against the full method inventory): PurchaseOrderGet reads one PO per "
        "call, so bulk extraction needs an ID source outside the API. Expect this "
        "domain to be the backfill's long pole (spec §6)."
    ),
    "gl_entries": (
        "PLAN ONLY — no AgilityPublic service (spec §3 row 11, absence verified): "
        "GL rides the hybrid vendor-mediated channel — Agility's embedded Data "
        "Warehouse, report exports, or BInformed FTP (spec §1/§4). File cadence "
        "is a per-dealer negotiation with DMSi, not a documented standard."
    ),
}


class DmsiAgilityConnector(BaseConnector):
    """DMSi AgilityPublic adapter. Credential-gated; dry-runs need no network."""

    erp_id = "dmsi_agility"
    maturity = ConnectorMaturity.IMPLEMENTED  # coded — but see UNEXERCISED note above
    #: Watermark-incremental or full-refresh per entity — never a wholesale
    #: replacement — so deletes surface only through the scheduled anti-join.
    full_snapshot = False
    delete_handling = DeleteSemantics.ANTI_JOIN
    extraction_notes = (
        "AgilityPublic REST with Session/Login context (ContextId/Branch headers; "
        "contexts expire unused — 4 h default, 24 h max, dealer-configurable — so "
        "one rejection re-logins and restarts the chunk walk from pointer 0, and a "
        "second rejection fails the run), chunk-pointer paging (ChunkStartPointer "
        "<- NextChunkStartPointer until MoreResultsAvailable is false; "
        "RecordFetchLimit defaults to the dealer's System Config and starts at "
        "500), all-or-nothing error semantics (ReturnCode 0/1/2 with first-error "
        "MessageText — no partial processing), and a sequential governor against "
        "the dealer's production OpenEdge database (heavy pulls scheduled off-peak; "
        "no published rate limits, so the governor is ours — spec §5/§7). "
        "Watermarks are delete-blind: the scheduled anti-join reconciliation "
        "tombstones vanished keys. Not yet exercised against a live dealer; "
        "validate rowset keys, field names, chunk ceilings, and allocation scoping "
        "against the tenant's AgilityVersion at onboarding."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "customers": ("customer_no",),
        "vendors": ("vendor_no",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch_code", "item_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "gl_entries": ("journal_no",),
    }
    required_settings: ClassVar[tuple[str, ...]] = (
        # Per-dealer API URL tied to the dealer's database (spec §1/§2) — HTTPS
        # only; the URL is available from the customer.
        "api_url",
        # A dealer-created Agility integration user maintained by the dealer's
        # system manager — no API keys or OAuth exist (spec §2).
        "login_id",
        "password",
        # CustomerID list scoping order/invoice pulls — <all> is not a bulk path
        # (spec §3 rows 7-8); pinned at onboarding, fail-closed on empty.
        "customers",
        # The dealer's Agility go-live date bounds transaction backfills
        # (spec §6 step 4: "date ranges from dealer go-live forward").
        "go_live_date",
    )
    MAX_RETRIES = 5
    BACKOFF_SECONDS = 2.0

    def __init__(
        self, source: SourceConfig, store: ControlPlaneStore, config: ControlPlaneConfig
    ) -> None:
        super().__init__(source, store, config)
        self._http_client: httpx.Client | None = None
        self._context_id: str | None = None
        self._branch: str | None = None
        self._max_incremental_seen: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    def validate_config(self) -> list[str]:
        missing = [f for f in self.required_settings if not self.source.settings.get(f)]
        if missing:
            return [
                f"missing required DMSi Agility settings: {', '.join(missing)} "
                "(see .env.example; source stays disabled until configured)"
            ]
        problems: list[str] = []
        api_url = self.source.settings["api_url"]
        if not api_url.startswith("https://"):
            problems.append(
                "setting 'api_url' must be an https:// URL — AgilityPublic rejects "
                "HTTP outright (spec §5)"
            )
        if not self._customers():
            problems.append(
                "setting 'customers' resolves to no usable customer ids — provide a "
                "comma-separated list of Agility CustomerIDs (order/invoice "
                "extraction is customer-scoped and <all> is not a bulk path, spec "
                "§3 row 7; the list is pinned at onboarding)"
            )
        go_live = self._go_live_date()
        if go_live is None:
            problems.append("setting 'go_live_date' must be an ISO date (YYYY-MM-DD)")
        elif go_live > _today():
            problems.append(
                "setting 'go_live_date' is in the future — backfill windows need a past date"
            )
        for name in ("record_fetch_limit", "invoice_window_days"):
            raw = self.source.settings.get(name) or ""
            if raw and (not raw.isdigit() or int(raw) <= 0):
                problems.append(f"setting '{name}' must be a positive integer")
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
                    f"entity '{entity}' has no DMSi Agility mapping; known: "
                    f"{', '.join(sorted(self.natural_key_fields))}"
                )
            return ExtractionPlan(entity=entity, surface=planned, incremental_key=None, notes=notes)
        if isinstance(surface, ListSurface):
            text = (
                f"AgilityPublic REST: {surface.service} · {surface.method} "
                f"(chunk-pointer walk, RecordFetchLimit={self.record_fetch_limit})"
            )
            if surface.include_quantity_data:
                text += " with IncludeQuantityData on (dated snapshot)"
            else:
                text += " — pure master (IncludePriceData off)"
        else:
            text = (
                f"AgilityPublic REST: {surface.service} · {surface.method} per "
                f"configured customer ({len(self._customers())} configured) — "
                f"{surface.header_key} + {surface.detail_key} joined on "
                f"{surface.join_field}"
            )
        if surface.watermark_capable:
            incremental = "LastChanged (FetchOnlyChangedSince — spec §4)"
        elif entity == "invoice_lines":
            incremental = "invoice date window (no changed-since — spec §4)"
        else:
            incremental = None
        return ExtractionPlan(entity=entity, surface=text, incremental_key=incremental, notes=notes)

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        self._require_config()
        key_fields = self.natural_key_fields[entity]
        seen: set[str] = set()
        for record in self._iter_entity_records(entity, mode, watermark):
            natural_id = natural_id_for(key_fields, record)
            if natural_id in seen:
                # A re-login mid-walk restarts the entity's chunk walk from
                # pointer 0 (spec §4's resume hazard) — the restart must never
                # double-stage a row.
                continue
            seen.add(natural_id)
            yield record

    def _iter_entity_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        surface = _ENTITY_SURFACES.get(entity)
        if surface is None:
            raise ConnectorNotImplemented(
                f"dmsi_agility extraction for '{entity}' stays a documented plan: "
                f"{_PLAN_SURFACES[entity]}"
            )
        if isinstance(surface, ListSurface):
            yield from self._walk_list(entity, surface, mode, watermark)
            return
        yield from self._walk_documents(entity, surface, mode, watermark)

    def _walk_list(
        self,
        entity: str,
        surface: ListSurface,
        mode: ExtractionMode,
        watermark: str | None,
    ) -> Iterator[dict[str, object]]:
        body: dict[str, object] = {
            "RecordFetchLimit": self.record_fetch_limit,
            "IncludePriceData": False,
        }
        if surface.include_quantity_data:
            body["IncludeQuantityData"] = True
        if surface.watermark_capable:
            # Always SENT on the methods that document it — the typed inputs
            # "require a value due to data type" (spec §4); backfill sends the
            # epoch-equivalent (spec §6 step 2).
            body["FetchOnlyChangedSince"] = self._changed_since(mode, watermark)
        for payload in self._payload_pages(entity, surface, body):
            rows = self._rows_of(entity, surface.rows_key, payload)
            if surface.watermark_capable:
                self._observe_incremental(entity, rows)
            if entity == "inventory_snapshots":
                for row in rows:
                    record: dict[str, object] = {
                        canon: row.get(source) for canon, source in _FIELD_MAPS[entity].items()
                    }
                    # Snapshot semantics (spec §4): the snapshot date and the
                    # logged-in branch are stamped at extraction time.
                    record["snapshot_date"] = _today().isoformat()
                    record["branch_code"] = self._branch
                    yield record
            else:
                for row in rows:
                    yield {canon: row.get(source) for canon, source in _FIELD_MAPS[entity].items()}

    def _walk_documents(
        self,
        entity: str,
        surface: DocumentSurface,
        mode: ExtractionMode,
        watermark: str | None,
    ) -> Iterator[dict[str, object]]:
        header_map, detail_map = self._document_maps(entity)
        for customer_id in self._customers():
            for start, end in self._request_windows(entity, mode):
                body: dict[str, object] = {
                    "RecordFetchLimit": self.record_fetch_limit,
                    "CustomerID": customer_id,
                }
                if entity == "sales_order_lines":
                    body.update(
                        {
                            # Full-history posture; status transitions ride
                            # LastChanged edits (spec §4).
                            "IncludeOpenOrders": True,
                            "IncludeInvoicedOrders": True,
                            "IncludeCanceledOrders": True,
                            # Typed inputs are sent explicitly, never omitted (spec §4).
                            "FetchOnlyChangedSince": self._changed_since(mode, watermark),
                        }
                    )
                    if mode is ExtractionMode.BACKFILL:
                        # spec §6 step 4: date ranges from dealer go-live forward.
                        body["OrderDateRangeStart"] = self._go_live_date().isoformat()
                else:  # invoice_lines — no changed-since; date windows only (spec §4)
                    body.update(
                        {
                            "ShiptoSequence": 0,  # 0 = all ship-tos (spec §3 row 8)
                            "IncludeOnlyOpenInvoices": False,  # closed history included
                            "InvoiceDateRangeStart": start,
                            "InvoiceDateRangeEnd": end,
                        }
                    )
                for payload in self._payload_pages(entity, surface, body):
                    headers = self._rows_of(entity, surface.header_key, payload)
                    details = self._rows_of(entity, surface.detail_key, payload)
                    if surface.watermark_capable:
                        self._observe_incremental(entity, headers)
                    yield from self._join_documents(
                        entity, surface, payload, headers, details, header_map, detail_map
                    )

    def _join_documents(
        self,
        entity: str,
        surface: DocumentSurface,
        payload: dict[str, object],
        headers: list[dict[str, object]],
        details: list[dict[str, object]],
        header_map: dict[str, str],
        detail_map: dict[str, str],
    ) -> Iterator[dict[str, object]]:
        """Join one page's header/detail rowsets on the spec-verbatim join field."""
        by_join: dict[str, dict[str, object]] = {}
        for row in headers:
            join_value = row.get(surface.join_field)
            if join_value is not None:
                by_join.setdefault(str(join_value), row)
        for detail in details:
            join_value = detail.get(surface.join_field)
            header = by_join.get(str(join_value)) if join_value is not None else None
            if header is None:
                # A detail row whose header is not in the same payload would
                # silently drop the document context — quarantine fail-closed
                # (a chunk boundary split a document's rows; tune the fetch
                # limit at onboarding).
                self._quarantine_page(
                    entity,
                    f"{surface.service}/{surface.method}",
                    json.dumps(payload, default=str).encode("utf-8"),
                    RC_MALFORMED_PAGE,
                    f"{surface.detail_key} row references {surface.join_field}="
                    f"{join_value!r} absent from {surface.header_key}",
                )
                raise ConnectorError(
                    f"unjoinable DMSi Agility page for {entity}: a {surface.detail_key} "
                    f"row references {surface.join_field}={join_value!r} absent from "
                    f"{surface.header_key} — chunk boundary split a document's rows"
                )
            record: dict[str, object] = {
                canon: header.get(source) for canon, source in header_map.items()
            }
            record.update({canon: detail.get(source) for canon, source in detail_map.items()})
            if record.get("line_no") is None:
                continue  # header without lines data — not a line row
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
            or not isinstance(surface, (ListSurface, DocumentSurface))
            or not surface.watermark_capable
        ):
            return watermark_before
        # Cross-customer checkpoint: the max LastChanged observed across every
        # configured customer's pages — never a per-customer max (the D365
        # cross-company precedent). The base persists it only after success.
        return self._max_incremental_seen.get(entity, watermark_before)

    # ------------------------------------------------------------------
    # AgilityPublic plumbing (session, chunk paging, quarantine)
    # ------------------------------------------------------------------

    def _payload_pages(
        self,
        entity: str,
        surface: ListSurface | DocumentSurface,
        body: dict[str, object],
    ) -> Iterator[dict[str, object]]:
        """Yield one response payload per chunk pointer (spec §4).

        Chunk state is server-side per method run: a re-login mid-walk restarts
        from pointer 0 (resume-from-pointer after re-login needs re-validation,
        spec §4) — once per walk, then the failure is honest. A pointer that
        does not advance is a contract violation, not a spin loop.
        """
        label = f"{surface.service}/{surface.method}"
        pointer = 0
        restarted = False
        while True:
            try:
                payload = self._call(entity, surface, {**body, "ChunkStartPointer": pointer})
            except SessionExpired:
                if restarted:
                    raise
                restarted = True
                self._context_id = None
                self._branch = None
                self._login()
                pointer = 0
                continue
            yield payload
            more, next_pointer = self._chunk_fields(entity, label, payload)
            if not more:
                return
            if next_pointer <= pointer:
                raise ConnectorError(
                    f"DMSi Agility chunk pointer did not advance on {label} "
                    f"({pointer} -> {next_pointer}) — refusing to spin"
                )
            pointer = next_pointer

    def _changed_since(self, mode: ExtractionMode, watermark: str | None) -> str:
        if mode is ExtractionMode.INCREMENTAL and watermark:
            return watermark
        return EPOCH_CHANGED_SINCE

    def _request_windows(self, entity: str, mode: ExtractionMode) -> list[tuple[str, str]]:
        """InvoiceDateRangeStart/End windows (spec §4).

        Incremental re-pulls the trailing ``invoice_window_days``; backfill
        slices go-live -> today into bounded windows so closed history is
        captured without unbounded single pulls.
        """
        if entity != "invoice_lines":
            return [("", "")]
        today = _today()
        span = dt.timedelta(days=self.invoice_window_days - 1)
        if mode is ExtractionMode.INCREMENTAL:
            return [
                (
                    (today - dt.timedelta(days=self.invoice_window_days)).isoformat(),
                    today.isoformat(),
                )
            ]
        windows: list[tuple[str, str]] = []
        cursor = self._go_live_date()
        while cursor <= today:
            end = min(cursor + span, today)
            windows.append((cursor.isoformat(), end.isoformat()))
            cursor = end + dt.timedelta(days=1)
        return windows

    def _document_maps(self, entity: str) -> tuple[dict[str, str], dict[str, str]]:
        if entity == "sales_order_lines":
            return _ORDER_HEADER_MAP, _ORDER_DETAIL_MAP
        if entity == "invoice_lines":
            return _INVOICE_HEADER_MAP, _INVOICE_DETAIL_MAP
        raise ConnectorError(f"entity '{entity}' is not a document-surface DMSi Agility entity")

    def _observe_incremental(self, entity: str, rows: list[dict[str, object]]) -> None:
        """Track the max LastChanged seen — across all pages and customers."""
        for row in rows:
            observed = row.get("LastChanged")
            if observed is None:
                continue
            text = str(observed)
            current = self._max_incremental_seen.get(entity)
            if current is None or _dmsi_timestamp(text) > _dmsi_timestamp(current):
                self._max_incremental_seen[entity] = text

    def _call(
        self,
        entity: str,
        surface: ListSurface | DocumentSurface,
        body: dict[str, object],
    ) -> dict[str, object]:
        """One chunked method call: context headers, 429/503 backoff, session
        rejection as :class:`SessionExpired`, envelope validation, quarantine
        fail-closed on malformed bodies."""
        if not self._context_id or not self._branch:
            self._login()
        label = f"{surface.service}/{surface.method}"
        delay = self.BACKOFF_SECONDS
        for attempt in range(1, self.MAX_RETRIES + 1):
            response = self._client().post(
                self._method_url(surface.service, surface.method),
                content=json.dumps(body),
                headers=self._headers(),
            )
            if response.status_code in (429, 503):
                # No rate limits are published (spec §5) — this governor is
                # ours: back off honoring Retry-After, then give up honestly.
                if attempt == self.MAX_RETRIES:
                    raise ConnectorError(
                        f"DMSi Agility returned {response.status_code} on {label} "
                        f"after {self.MAX_RETRIES} backoff attempts"
                    )
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else delay
                time.sleep(delay)
                delay *= 2
                continue
            if response.status_code in (401, 403):
                raise SessionExpired(
                    f"DMSi Agility rejected the ContextId on {label} "
                    f"({response.status_code}) — contexts expire unused (4 h "
                    "default, 24 h max; spec §2)"
                )
            try:
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise ConnectorError(f"DMSi Agility call failed: {exc}") from exc
            try:
                payload: object = response.json()
            except ValueError as exc:
                self._quarantine_page(
                    entity, label, response.content, RC_UNPARSEABLE_JSON, str(exc)
                )
                raise ConnectorError(
                    f"malformed DMSi Agility response on {label}: body is not JSON"
                ) from exc
            if not isinstance(payload, dict):
                self._quarantine_page(
                    entity,
                    label,
                    json.dumps(payload, default=str).encode("utf-8"),
                    RC_MALFORMED_PAGE,
                    "response must be a JSON object",
                )
                raise ConnectorError(f"malformed DMSi Agility response on {label}: not an object")
            self._validate_envelope(entity, label, payload)
            return payload
        raise ConnectorError("unreachable: retry loop must return or raise")  # pragma: no cover

    def _validate_envelope(self, entity: str, label: str, payload: dict[str, object]) -> None:
        """All-or-nothing semantics: ``we do not partially process any request``
        (spec §5) — a non-zero ReturnCode fails the run with MessageText."""
        return_code = payload.get("ReturnCode")
        if return_code not in (0, "0"):
            message = payload.get("MessageText")
            raise ConnectorError(
                f"DMSi Agility {label} failed with ReturnCode {return_code}: "
                f"{message or '(no MessageText)'}"
            )

    def _rows_of(
        self, entity: str, rows_key: str, payload: dict[str, object]
    ) -> list[dict[str, object]]:
        rows = payload.get(rows_key)
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            self._quarantine_page(
                entity,
                rows_key,
                json.dumps(payload, default=str).encode("utf-8"),
                RC_MALFORMED_PAGE,
                f"'{rows_key}' must be a list of objects",
            )
            raise ConnectorError(
                f"malformed DMSi Agility page for {entity}: '{rows_key}' must be a list of objects"
            )
        return rows

    def _chunk_fields(
        self, entity: str, label: str, payload: dict[str, object]
    ) -> tuple[bool, int]:
        """The chunking contract (spec §4): MoreResultsAvailable + NextChunkStartPointer."""
        more = payload.get("MoreResultsAvailable")
        if not isinstance(more, bool):
            self._quarantine_page(
                entity,
                label,
                json.dumps(payload, default=str).encode("utf-8"),
                RC_MALFORMED_PAGE,
                "'MoreResultsAvailable' must be a boolean",
            )
            raise ConnectorError(
                f"malformed DMSi Agility page for {entity} at {label}: "
                "'MoreResultsAvailable' must be a boolean"
            )
        if not more:
            return False, 0
        next_pointer = payload.get("NextChunkStartPointer")
        if not isinstance(next_pointer, int) or isinstance(next_pointer, bool):
            self._quarantine_page(
                entity,
                label,
                json.dumps(payload, default=str).encode("utf-8"),
                RC_MALFORMED_PAGE,
                "'NextChunkStartPointer' must be an integer when MoreResultsAvailable is true",
            )
            raise ConnectorError(
                f"malformed DMSi Agility page for {entity} at {label}: "
                "'NextChunkStartPointer' must be an integer when "
                "'MoreResultsAvailable' is true"
            )
        return True, next_pointer

    def _login(self) -> None:
        """Session/Login (spec §2): LoginID/Password in the JSON body; the
        returned SessionContextId + InitialBranch ride every subsequent call as
        the ContextId/Branch headers. Contexts expire unused (4 h default, 24 h
        max, dealer-configurable) — no TTL is assumed; rejection re-logins."""
        settings = self.source.settings
        response = self._client().post(
            self._method_url(LOGIN_SERVICE, LOGIN_METHOD),
            content=json.dumps({"LoginID": settings["login_id"], "Password": settings["password"]}),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise ConnectorError(f"DMSi Agility login failed: {exc}") from exc
        try:
            payload: object = response.json()
        except ValueError as exc:
            raise ConnectorError("DMSi Agility login response is not JSON") from exc
        if not isinstance(payload, dict):
            raise ConnectorError("DMSi Agility login response is not a JSON object")
        context_id = payload.get("SessionContextId")
        branch = payload.get("InitialBranch")
        # Fail closed on an empty context — never authenticate with nothing.
        if not isinstance(context_id, str) or not context_id:
            raise ConnectorError(
                "DMSi Agility login response carried no SessionContextId — "
                "refusing to authenticate with an empty context"
            )
        if not isinstance(branch, str) or not branch:
            raise ConnectorError(
                "DMSi Agility login response carried no InitialBranch — the Branch "
                "header is required on every data call (spec §2)"
            )
        self._context_id = context_id
        self._branch = branch

    def _quarantine_page(
        self, entity: str, label: str, body: bytes, reason_code: str, detail: str
    ) -> None:
        """Persist an unreadable API page with a machine-readable reason
        (csv_sftp/NetSuite/P21/BC parity): the run fails, but the offending
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
        if not self._context_id or not self._branch:
            raise ConnectorError("DMSi Agility data calls require an active Session/Login context")
        return {
            "ContextId": self._context_id,
            "Branch": self._branch,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _method_url(self, service: str, method: str) -> str:
        base = self.source.settings["api_url"].rstrip("/")
        return f"{base}/{service}/{method}"

    def _customers(self) -> list[str]:
        """Configured customer ids, in order, deduplicated — the CustomerID
        list that scopes every order/invoice pull (spec §3 rows 7-8)."""
        customers: list[str] = []
        for token in self.source.settings.get("customers", "").split(","):
            customer_id = token.strip()
            if customer_id and customer_id not in customers:
                customers.append(customer_id)
        return customers

    def _go_live_date(self) -> dt.date | None:
        raw = self.source.settings.get("go_live_date") or ""
        try:
            return dt.date.fromisoformat(raw)
        except ValueError:
            return None

    @property
    def record_fetch_limit(self) -> int:
        raw = self.source.settings.get("record_fetch_limit") or ""
        if raw.isdigit() and int(raw) > 0:
            return int(raw)
        return DEFAULT_RECORD_FETCH_LIMIT

    @property
    def invoice_window_days(self) -> int:
        raw = self.source.settings.get("invoice_window_days") or ""
        if raw.isdigit() and int(raw) > 0:
            return int(raw)
        return DEFAULT_INVOICE_WINDOW_DAYS

    def _client(self) -> httpx.Client:
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=60.0)
        return self._http_client
