"""epicor_p21 — Epicor Prophet 21 extraction (OData v4 Data Services, cloud).

STATUS: implemented against the documented P21 OData v4 Data Services surface
(bearer-token auth, explicit ``$top`` paging — P21 does not emit OData
continuation links, so this connector pages with ``$top`` + ``$skip`` and
ALWAYS sets ``$top`` per the documented behavior — watermark on
``date_last_modified``) but NOT exercised against a live tenant: no fabricated
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
import time
from collections.abc import Iterator
from typing import ClassVar

import httpx

from connectors.base import (
    BaseConnector,
    ConnectorError,
    ConnectorMaturity,
    ExtractionMode,
    ExtractionPlan,
    natural_id_for,
)

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


class EpicorP21Connector(BaseConnector):
    """Prophet 21 OData v4 adapter. Credential-gated; dry-runs need no network."""

    erp_id = "epicor_p21"
    maturity = ConnectorMaturity.IMPLEMENTED  # coded — but see UNEXERCISED note above
    extraction_notes = (
        "OData v4 Data Services with explicit $top + $skip paging (no "
        "continuation links — $top is always set), bearer-token auth, and "
        "watermark on date_last_modified (spec §5). Not yet exercised against a "
        "live tenant; validate entity sets and field maps against $metadata at "
        "onboarding. On-prem P21 should use the SQL Server connector against a "
        "read-only replica instead."
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
    required_settings: ClassVar[tuple[str, ...]] = ("odata_base_url",)
    PAGE_SIZE = 500
    MAX_RETRIES = 5
    BACKOFF_SECONDS = 2.0

    def __init__(self, source, store, config):
        super().__init__(source, store, config)
        self._http_client: httpx.Client | None = None
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
        spec = self._entity_spec(entity)
        field_map = _FIELD_MAPS[entity]
        if not spec.get("lines_table"):
            snapshot_date = self.source.settings.get("as_of_date") or dt.date.today().isoformat()
            for row in self._paged(entity, spec["table"], mode, watermark):
                # inv_loc is a live balance table; the nightly snapshot date is
                # assigned at extraction time (documented in the module notes).
                record = {canon: row.get(source) for canon, source in field_map.items()}
                if entity == "inventory_snapshots" and record.get("snapshot_date") is None:
                    record["snapshot_date"] = snapshot_date
                yield record
            return
        context_map = _HEADER_CONTEXT[entity]
        for header in self._paged(entity, spec["table"], mode, watermark):
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
        key_fields = self.natural_key_fields[entity]
        spec = self._entity_spec(entity)
        field_map = _FIELD_MAPS[entity]
        if spec.get("lines_table"):
            return {
                natural_id_for(key_fields, record)
                for record in self._iter_records(entity, ExtractionMode.BACKFILL, None)
            }
        keys: set[str] = set()
        for row in self._paged(entity, spec["table"], None, None):
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
    # OData plumbing (explicit $top/$skip paging, 429 backoff, auth)
    # ------------------------------------------------------------------

    def _paged(self, entity: str, table: str, mode: ExtractionMode | None, watermark: str | None):
        """Yield rows across explicit $top/$skip pages (P21 has no nextLink)."""
        params: dict[str, str] = {"$top": str(self.PAGE_SIZE)}
        if mode is ExtractionMode.INCREMENTAL and watermark:
            params["$filter"] = f"date_last_modified gt {watermark}"
        offset = 0
        while True:
            page_params = dict(params, **{"$skip": str(offset)})
            payload = self._get_json(self._table_url(table), page_params)
            rows = payload.get("value", [])
            self._observe_incremental(entity, rows)
            yield from rows
            if len(rows) < self.PAGE_SIZE:
                return
            offset += self.PAGE_SIZE

    def _fetch_lines(
        self, entity: str, spec: dict[str, object], fk_value: object
    ) -> list[dict[str, object]]:
        """Fetch one header's lines (explicit $filter on the FK uid)."""
        lines_table = spec["lines_table"]
        fk = spec["fk"]
        lines: list[dict[str, object]] = []
        offset = 0
        while True:
            payload = self._get_json(
                self._table_url(str(lines_table)),
                {
                    "$top": str(self.PAGE_SIZE),
                    "$skip": str(offset),
                    "$filter": f"{fk} eq '{fk_value}'",
                },
            )
            page = payload.get("value", [])
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

    def _get_json(self, url: str, params: dict[str, str]) -> dict[str, object]:
        """GET with auth headers and documented-limits backoff (429/503)."""
        delay = self.BACKOFF_SECONDS
        for attempt in range(1, self.MAX_RETRIES + 1):
            response = self._client().get(url, params=params, headers=self._headers())
            if response.status_code in (429, 503):
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else delay
                if attempt == self.MAX_RETRIES:
                    raise ConnectorError(
                        f"Prophet 21 returned {response.status_code} on {url} after "
                        f"{self.MAX_RETRIES} backoff attempts"
                    )
                time.sleep(delay)
                delay *= 2
                continue
            try:
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise ConnectorError(f"Prophet 21 call failed: {exc}") from exc
            data: dict[str, object] = response.json()
            return data
        raise ConnectorError("unreachable: retry loop must return or raise")  # pragma: no cover

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        bearer = self.source.settings.get("bearer_token")
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        api_key = self.source.settings.get("api_key")
        if api_key:
            headers["X-Api-Key"] = api_key
        return headers

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
