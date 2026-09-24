"""epicor_p21 — Epicor Prophet 21 extraction (OData v4 Data Services, cloud).

STATUS: implemented against the documented P21 OData v4 Data Services surface
(middleware token auth — ``POST /api/security/token/v2`` with JSON credentials,
defensively parsed because some middleware answers XML even when JSON is
requested, cached until near expiry because tokens live ~24 h and the token
endpoint throttles re-minting — explicit ``$top`` paging — P21 does not emit
OData continuation links, so this connector pages with ``$top`` + ``$skip`` and
ALWAYS sets ``$top`` per the documented behavior — soft-delete flags filtered
server-side and client-side so a soft-delete-heavy site cannot mass-tombstone
(spec §6.4) — header pages ``$orderby``'d on each entity's stable key so
OFFSET windows are deterministic on active tables (spec §6.3), and malformed
API pages quarantined with machine-readable reasons (fail-closed) — watermark
on ``date_last_modified``) but NOT exercised against a live tenant: no fabricated
API behavior ships as tested. Validate entity-set names and field maps against
the tenant's ``$metadata`` at onboarding; ``plan --source <id>`` (dry-run)
needs no network.

Extraction notes (ERP landscape research, art_NKUrngnG; posture art_7DIRx9Nu):
- P21 ships SQL Server backends exposed through documented views (on-prem) and
  an OData REST API (P21 Cloud). This connector implements the cloud OData
  path; on-prem sites should use connectors.sqlserver (read-only replica)
  instead of pointing extraction at the transactional primary.
- Core tables: inv_mast (items), supplier, customer, inv_loc (inventory by
  location), oe_hdr/oe_line, invoice_hdr/invoice_line, po_hdr/po_line.
- Line entities are read header-driven: page the header table incrementally on
  ``date_last_modified``, then fetch each header's lines with an explicit
  ``$filter=<fk> eq <uid>``. N+1 by design — batch per-site tuning at
  onboarding (bounded line fetches, parallel pages).
- ``date_last_modified`` is P21's universal maintenance timestamp — every
  entity's incremental key per the documented surface.
"""

from __future__ import annotations

import datetime as dt
import json
import time
import uuid
import xml.etree.ElementTree as ET
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
    ExtractionMode,
    ExtractionPlan,
    natural_id_for,
)
from control_plane.config import ControlPlaneConfig
from control_plane.models import QuarantineRecord, SourceConfig
from control_plane.store import ControlPlaneStore

#: The middleware token endpoint (spec §2.2): v2 takes credentials in the JSON
#: body — never headers.
TOKEN_PATH = "/api/security/token/v2"

#: iPaaS-documented token lifetime (~24 h) when the response carries no
#: expiry hint.
DEFAULT_TOKEN_TTL_SECONDS = 24 * 60 * 60

#: Reason codes recorded on quarantined API pages (csv_sftp/NetSuite parity).
RC_MALFORMED_PAGE = "MALFORMED_PAGE"
RC_UNPARSEABLE_JSON = "UNPARSEABLE_JSON"

#: entity -> (header table, canonical header map, lines table, fk, line map).
#: Field names follow the documented P21 surface; reviewed against $metadata
#: at onboarding (per-tenant UDFs are appended to the canonical set there).
_ENTITY_TABLES: dict[str, dict[str, object]] = {
    "items": {"table": "inv_mast"},
    "customers": {"table": "customer"},
    "vendors": {"table": "supplier"},
    "inventory_snapshots": {"table": "inv_loc"},
    "sales_order_lines": {"table": "oe_hdr", "lines_table": "oe_line", "fk": "oe_hdr_uid"},
    "invoice_lines": {
        "table": "invoice_hdr",
        "lines_table": "invoice_line",
        "fk": "invoice_hdr_uid",
    },
    "purchase_order_lines": {"table": "po_hdr", "lines_table": "po_line", "fk": "po_hdr_uid"},
}

_FIELD_MAPS: dict[str, dict[str, str]] = {
    "items": {
        "item_no": "item_id",
        "description": "item_desc",
        "category": "product_group_id",
        "unit_cost": "avg_cost",
        "list_price": "price_1",
    },
    "customers": {
        "customer_no": "customer_id",
        "customer_name": "customer_name",
        "address1": "addr1",
        "city": "city",
        "state": "state",
        "postal_code": "zip",
    },
    "vendors": {"vendor_no": "supplier_id", "vendor_name": "supplier_name"},
    "inventory_snapshots": {
        "location_id": "location_id",
        "item_no": "item_id",
        "on_hand_qty": "qty_on_hand",
        "allocated_qty": "qty_allocated",
        "on_order_qty": "qty_on_order",
    },
    "sales_order_lines": {
        "order_no": "order_no",
        "line_no": "line_no",
        "item_no": "item_id",
        "ordered_qty": "qty_ordered",
        "filled_qty": "qty_filled",
        "unit_price": "unit_price",
        "order_status": "oe_status",
    },
    "invoice_lines": {
        "invoice_no": "invoice_no",
        "line_no": "line_no",
        "item_no": "item_id",
        "invoiced_qty": "qty_invoiced",
        "unit_price": "unit_price",
    },
    "purchase_order_lines": {
        "po_no": "po_no",
        "line_no": "line_no",
        "item_no": "item_id",
        "ordered_qty": "qty_ordered",
        "received_qty": "qty_received",
        "unit_cost_actual": "unit_cost",
    },
}

#: header context each line row inherits (canonical field <- header field).
_HEADER_CONTEXT: dict[str, dict[str, str]] = {
    "sales_order_lines": {"order_no": "order_no"},
    "invoice_lines": {"invoice_no": "invoice_no"},
    "purchase_order_lines": {"po_no": "po_no"},
}


@dataclass(frozen=True)
class SoftDelete:
    """A table's soft-delete flag (spec §3 cross-cutting quirk: per-table and
    inconsistent). ``deleted_value`` marks a soft-deleted row; the extraction
    predicate filters the active value server-side, and both extraction and
    the key-inventory scan drop ``deleted_value`` rows client-side so the
    anti-join compares live-to-live."""

    column: str
    deleted_value: str
    active_value: str


#: Only tables the spec documents with a soft-delete flag get a filter — a
#: filter on a column a table does not have 404s the whole request (spec §3).
#: The row_status_flag tables (price_page, customer_salesrep) are not in this
#: connector's entity set.
_SOFT_DELETE: dict[str, SoftDelete] = {
    "items": SoftDelete(column="delete_flag", deleted_value="Y", active_value="N"),
    "customers": SoftDelete(column="delete_flag", deleted_value="Y", active_value="N"),
    "vendors": SoftDelete(column="delete_flag", deleted_value="Y", active_value="N"),
}


def _is_soft_deleted(soft: SoftDelete | None, row: dict[str, object]) -> bool:
    """True only on an EXPLICIT deleted flag — a missing/None column means the
    flag is absent on this tenant, not that the row is dead.

    Polarity per the spec's operative §6.4/§7 semantics ('Y' = deleted); §3.1's
    "Y = active" parenthetical contradicts them, so per-site verification at
    onboarding is mandatory before trusting tombstones.
    """
    return soft is not None and row.get(soft.column) == soft.deleted_value


#: Stable server-side order per entity (spec §6.3 backfill mechanics: $top +
#: $orderby on a stable key). Where the key is not unique per row, ties may
#: interleave mid-scan — re-runs stay idempotent by natural key (NetSuite
#: parity); partition long histories into watermark windows at onboarding.
_ORDER_BY: dict[str, str] = {
    "items": "item_id",
    "customers": "customer_id",
    "vendors": "supplier_id",
    "inventory_snapshots": "inv_mast_uid, location_id",
    "sales_order_lines": "order_no",
    "invoice_lines": "invoice_no",
    "purchase_order_lines": "po_no",
}


def _parse_token_payload(text: str) -> tuple[str, float]:
    """Parse ``(AccessToken, lifetime_seconds)`` from a token-endpoint response.

    Some middleware answers XML even when JSON is requested (spec §2.2), so
    JSON is tried first and an XML body scanned for an ``AccessToken`` element.
    Both failing is a hard error — never a silent empty token.
    """
    try:
        payload: object = json.loads(text)
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        token = payload.get("AccessToken") or payload.get("access_token")
        if isinstance(token, str) and token:
            lifetime = DEFAULT_TOKEN_TTL_SECONDS
            expiry = payload.get("ExpiresIn") or payload.get("expires_in")
            if isinstance(expiry, (int, float)) and expiry > 0:
                lifetime = float(expiry)
            return token, lifetime
        raise ConnectorError(
            "Prophet 21 token response was JSON but carried no AccessToken — "
            "refusing to authenticate with an empty token"
        )
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise ConnectorError(
            "Prophet 21 token response is neither JSON nor XML — cannot "
            "extract an AccessToken from it"
        ) from exc
    for element in root.iter():
        if (
            element.tag.rsplit("}", 1)[-1] == "AccessToken"
            and element.text
            and element.text.strip()
        ):
            return element.text.strip(), DEFAULT_TOKEN_TTL_SECONDS
    raise ConnectorError("Prophet 21 token response carried no AccessToken element")


class EpicorP21Connector(BaseConnector):
    """Prophet 21 OData v4 adapter. Credential-gated; dry-runs need no network."""

    erp_id = "epicor_p21"
    maturity = ConnectorMaturity.IMPLEMENTED  # coded — but see UNEXERCISED note above
    extraction_notes = (
        "OData v4 Data Services with explicit $top + $skip paging (no "
        "continuation links — $top is always set), middleware token auth "
        "(POST /api/security/token/v2 with JSON credentials; tokens live ~24 h, "
        "are cached until near expiry, and one 401 triggers a single "
        "re-mint-and-retry), soft-delete flags filtered server-side "
        "(delete_flag eq 'N' on the tables the spec documents it for) and "
        "client-side in both extraction and the key-inventory scan so a "
        "soft-delete-heavy site cannot mass-tombstone (polarity follows the "
        "spec's operative §6.4/§7 semantics — 'Y' = deleted — per-site "
        "verified at onboarding), and watermark on date_last_modified. Header "
        "pages are ORDERed by each entity's stable key so OFFSET windows are "
        "deterministic on active tables, and malformed pages quarantine with a "
        "machine-readable reason and fail the run. Not yet "
        "exercised against a live tenant; validate entity sets, field maps, "
        "and flag polarity against $metadata at onboarding. On-prem P21 should "
        "use the SQL Server connector against a read-only replica instead."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "customers": ("customer_no",),
        "vendors": ("vendor_no",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "location_id", "item_no"),
    }
    required_settings: ClassVar[tuple[str, ...]] = (
        "odata_base_url",
        "odata_user",
        "odata_password",
    )
    PAGE_SIZE = 500
    MAX_RETRIES = 5
    BACKOFF_SECONDS = 2.0

    def __init__(
        self, source: SourceConfig, store: ControlPlaneStore, config: ControlPlaneConfig
    ) -> None:
        super().__init__(source, store, config)
        self._http_client: httpx.Client | None = None
        self._max_incremental_seen: dict[str, str] = {}
        self._cached_token: str | None = None
        self._token_expiry = 0.0

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    def validate_config(self) -> list[str]:
        missing = [f for f in self.required_settings if not self.source.settings.get(f)]
        if missing:
            return [
                f"missing required Prophet 21 settings: {', '.join(missing)} "
                "(see .env.example; source stays disabled until configured)"
            ]
        return []

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        spec = self._entity_spec(entity)
        table = spec["table"]
        surface = f"OData v4: GET {table}?$top={self.PAGE_SIZE}"
        if spec.get("lines_table"):
            surface += f" then {spec['lines_table']}?$filter={spec['fk']} eq <uid>"
        return ExtractionPlan(
            entity=entity,
            surface=surface,
            incremental_key="date_last_modified",
            notes=self.extraction_notes,
        )

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        self._require_config()
        spec = self._entity_spec(entity)
        field_map = _FIELD_MAPS[entity]
        soft = _SOFT_DELETE.get(entity)
        if not spec.get("lines_table"):
            snapshot_date = self.source.settings.get("as_of_date") or dt.date.today().isoformat()
            for row in self._paged(entity, spec["table"], mode, watermark):
                if _is_soft_deleted(soft, row):
                    continue
                # inv_loc is a live balance table; the nightly snapshot date is
                # assigned at extraction time (documented in the module notes).
                record = {canon: row.get(source) for canon, source in field_map.items()}
                if entity == "inventory_snapshots" and record.get("snapshot_date") is None:
                    record["snapshot_date"] = snapshot_date
                yield record
            return
        context_map = _HEADER_CONTEXT[entity]
        for header in self._paged(entity, spec["table"], mode, watermark):
            if _is_soft_deleted(soft, header):
                continue  # a soft-deleted header's lines are never fetched
            fk_value = header.get(spec["fk"])
            if fk_value is None:
                continue
            for line in self._fetch_lines(entity, spec, fk_value):
                record: dict[str, object] = {
                    canon: header.get(source) for canon, source in context_map.items()
                }
                record.update(
                    {
                        canon: line.get(source)
                        for canon, source in field_map.items()
                        if canon not in context_map
                    }
                )
                if record.get("line_no") is None:
                    continue
                yield record

    def source_key_inventory(self, entity: str) -> set[str]:
        """Key scan of the entity set — the anti-join's source side."""
        self._require_config()
        key_fields = self.natural_key_fields[entity]
        spec = self._entity_spec(entity)
        field_map = _FIELD_MAPS[entity]
        soft = _SOFT_DELETE.get(entity)
        if spec.get("lines_table"):
            return {
                natural_id_for(key_fields, record)
                for record in self._iter_records(entity, ExtractionMode.BACKFILL, None)
            }
        keys: set[str] = set()
        for row in self._paged(entity, spec["table"], None, None):
            if _is_soft_deleted(soft, row):
                continue
            keys.add(
                natural_id_for(
                    key_fields,
                    {c: row.get(s) for c, s in field_map.items() if c in key_fields},
                )
            )
        return keys

    def current_watermark(
        self, entity: str, mode: ExtractionMode, watermark_before: str | None
    ) -> str | None:
        if mode is not ExtractionMode.INCREMENTAL:
            return watermark_before
        return self._max_incremental_seen.get(entity, watermark_before)

    # ------------------------------------------------------------------
    # OData plumbing (explicit $top/$skip paging, 429 backoff, token auth)
    # ------------------------------------------------------------------

    def _paged(self, entity: str, table: str, mode: ExtractionMode | None, watermark: str | None):
        """Yield rows across explicit $top/$skip pages (P21 has no nextLink).

        Pages are $orderby'd on the entity's stable key (spec §6.3) so OFFSET
        windows are deterministic; on an active table rows can still shift
        between pages mid-scan — re-runs are idempotent by natural key.
        """
        params = self._page_params(entity, mode, watermark)
        offset = 0
        while True:
            page_params = dict(params, **{"$skip": str(offset)})
            rows = self._request_rows(entity, self._table_url(table), page_params)
            self._observe_incremental(entity, rows)
            yield from rows
            if len(rows) < self.PAGE_SIZE:
                return
            offset += self.PAGE_SIZE

    def _page_params(
        self, entity: str, mode: ExtractionMode | None, watermark: str | None
    ) -> dict[str, str]:
        params: dict[str, str] = {
            "$top": str(self.PAGE_SIZE),
            "$orderby": _ORDER_BY[entity],
        }
        filters: list[str] = []
        if mode is ExtractionMode.INCREMENTAL and watermark:
            filters.append(f"date_last_modified gt {watermark}")
        soft = _SOFT_DELETE.get(entity)
        if soft is not None:
            filters.append(f"{soft.column} eq '{soft.active_value}'")
        if filters:
            params["$filter"] = " and ".join(filters)
        return params

    def _fetch_lines(
        self, entity: str, spec: dict[str, object], fk_value: object
    ) -> list[dict[str, object]]:
        """Fetch one header's lines (explicit $filter on the FK uid)."""
        lines_table = spec["lines_table"]
        fk = spec["fk"]
        lines: list[dict[str, object]] = []
        offset = 0
        while True:
            page = self._request_rows(
                entity,
                self._table_url(str(lines_table)),
                {
                    "$top": str(self.PAGE_SIZE),
                    "$skip": str(offset),
                    "$filter": f"{fk} eq '{fk_value}'",
                },
            )
            lines.extend(page)
            if len(page) < self.PAGE_SIZE:
                return lines
            offset += self.PAGE_SIZE

    def _observe_incremental(self, entity: str, rows: list[dict[str, object]]) -> None:
        for row in rows:
            observed = row.get("date_last_modified")
            if observed is None:
                continue
            text = str(observed)
            current = self._max_incremental_seen.get(entity)
            if current is None or text > current:
                self._max_incremental_seen[entity] = text

    def _request_rows(
        self, entity: str, url: str, params: dict[str, str]
    ) -> list[dict[str, object]]:
        """GET one page: bearer auth (one re-mint on 401), documented-limits
        backoff on 429/503, quarantine on malformed bodies.

        A 200 whose body is not ``{"value": [object, ...]}`` is quarantined
        with a machine-readable reason and fails the run — never a silent
        drop, never a partial promote (csv_sftp/NetSuite parity).
        """
        delay = self.BACKOFF_SECONDS
        backoff_attempts = 0
        reauthorized = False
        while True:
            response = self._client().get(url, params=params, headers=self._headers())
            if response.status_code in (429, 503):
                backoff_attempts += 1
                if backoff_attempts >= self.MAX_RETRIES:
                    raise ConnectorError(
                        f"Prophet 21 returned {response.status_code} on {url} after "
                        f"{self.MAX_RETRIES} backoff attempts"
                    )
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else delay
                time.sleep(delay)
                delay *= 2
                continue
            if response.status_code == 401 and not reauthorized:
                # A ~24 h token can lapse mid-run: mint once and retry — the
                # re-mint does not count against the backoff budget.
                reauthorized = True
                self._cached_token = None
                self._token_expiry = 0.0
                continue
            try:
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise ConnectorError(f"Prophet 21 call failed: {exc}") from exc
            try:
                payload: object = response.json()
            except ValueError as exc:
                self._quarantine_page(entity, url, response.content, RC_UNPARSEABLE_JSON, str(exc))
                raise ConnectorError(
                    f"malformed Prophet 21 response on {url}: body is not JSON"
                ) from exc
            rows = payload.get("value") if isinstance(payload, dict) else None
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                self._quarantine_page(
                    entity,
                    url,
                    json.dumps(payload, default=str).encode("utf-8"),
                    RC_MALFORMED_PAGE,
                    "'value' must be a list of objects",
                )
                raise ConnectorError(
                    f"malformed Prophet 21 page for {entity} at {url}: "
                    "'value' must be a list of objects"
                )
            return rows

    def _quarantine_page(
        self, entity: str, url: str, body: bytes, reason_code: str, detail: str
    ) -> None:
        """Persist an unreadable API page with a machine-readable reason
        (csv_sftp/NetSuite parity): the run fails, but the offending body is
        preserved for inspection instead of being dropped silently."""
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
                detail=f"{detail} (from {url})",
                quarantine_path=str(target),
                quarantined_at=datetime.now(UTC),
            )
        )

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token()}", "Accept": "application/json"}

    def _token(self) -> str:
        """Middleware token, cached until near expiry (spec §2.2).

        Tokens live ~24 h and the token endpoint throttles re-minting, so the
        token is minted once and reused across data calls; a mid-run 401
        clears the cache (``_get_json`` retries the request).
        """
        if self._cached_token and self._token_expiry > time.time() + 60:
            return self._cached_token
        settings = self.source.settings
        response = self._client().post(
            self._token_url(),
            content=json.dumps(
                {"username": settings["odata_user"], "password": settings["odata_password"]}
            ),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise ConnectorError(f"Prophet 21 token request failed: {exc}") from exc
        token, lifetime = _parse_token_payload(response.text)
        self._cached_token = token
        self._token_expiry = time.time() + lifetime
        return token

    def _require_config(self) -> None:
        """Fail closed BEFORE any network attempt — inert without credentials."""
        problems = self.validate_config()
        if problems:
            raise ConnectorNotConfigured(
                f"source {self.source.source_id} ({self.erp_id}) is not configurable: "
                + "; ".join(problems)
            )

    def _token_url(self) -> str:
        base = self.source.settings["odata_base_url"].rstrip("/")
        return f"{base}{TOKEN_PATH}"

    def _table_url(self, table: str) -> str:
        base = self.source.settings["odata_base_url"].rstrip("/")
        return f"{base}/odataservice/odata/table/{table}"

    def _entity_spec(self, entity: str) -> dict[str, object]:
        spec = _ENTITY_TABLES.get(entity)
        if spec is None:
            raise ConnectorError(
                f"entity '{entity}' has no Prophet 21 mapping yet; known: "
                f"{', '.join(sorted(_ENTITY_TABLES))}"
            )
        return spec

    def _client(self) -> httpx.Client:
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=60.0)
        return self._http_client
