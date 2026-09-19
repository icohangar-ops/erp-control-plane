"""netsuite — SuiteQL/SuiteAnalytics extraction adapter (CODED, UNEXERCISED).

STATUS: implemented against NetSuite's documented REST record-query surface
(SuiteQL via ``POST /services/rest/query/v1/suiteql``, token-based auth
TBA/OAuth1) but NOT exercised against a live tenant — per the locked decisions,
no fabricated API behavior ships as tested. Enable only after sandbox-tenant
discovery; validate with ``python -m connectors.cli plan --source
netsuite_template`` (dry-run, no network) before any live call.

Extraction notes (research doc, art_5rlAUYBI / art_NKUrngnG):
- SuiteQL returns flat rows over the transaction/transaction_line model;
  SuiteAnalytics workbooks are the alternative for large extracts.
- Sales/invoice history lives on transaction + transaction_line joined to
  item, entity (customer/vendor), and location.
- Inventory by location from ItemLocationLocationMap style records or a
  SuiteAnalytics workbook; GL from TransactionAccountingLine.
- Pagination via ``offset`` on SuiteQL (rowsPerPage caps at 10k for some
  workbooks; keep pages modest); rate limits are modest — throttle.

TODO(per-tenant): confirm subsidiary scoping, accounting-book selection,
and the tenant's custom segment/field names before first live run.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import urllib.parse
import uuid
from collections.abc import Iterator
from typing import ClassVar

import httpx

from connectors.base import (
    BaseConnector,
    ConnectorError,
    ConnectorMaturity,
    ExtractionMode,
    ExtractionPlan,
)

SUITEQL_PATH = "/services/rest/query/v1/suiteql"

#: entity -> SuiteQL table + column mapping (documented NetSuite schema; the
#: SQL below is intentionally simple and reviewed at onboarding).
_ENTITY_SQL: dict[str, str] = {
    "sales_order_lines": """
        SELECT tl.id AS line_id, tl.transaction AS txn_id, t.tranid AS order_no,
               t.trandate AS order_date, t.entityid AS customer_no,
               tl.locationid AS branch_code, tl.itemid AS item_no,
               tl.quantityordered AS ordered_qty, tl.quantitybilled AS filled_qty,
               tl.rate AS unit_price, tl.memo
        FROM transaction_line tl
        JOIN transaction t ON t.id = tl.transaction
        WHERE t.type = 'SalesOrd' AND t.trandate >= {watermark}
    """,
    "invoice_lines": """
        SELECT tl.id AS line_id, tl.transaction AS txn_id, t.tranid AS invoice_no,
               t.trandate AS invoice_date, t.entityid AS customer_no,
               tl.locationid AS branch_code, tl.itemid AS item_no,
               tl.quantity AS invoiced_qty, tl.rate AS unit_price, tl.memo
        FROM transaction_line tl
        JOIN transaction t ON t.id = tl.transaction
        WHERE t.type = 'CustInvc' AND t.trandate >= {watermark}
    """,
}

_ENTITY_TABLES: dict[str, str] = {
    "items": "item",
    "customers": "customer",
    "vendors": "vendor",
    "sales_order_lines": "transaction_line (SalesOrd)",
    "invoice_lines": "transaction_line (CustInvc)",
    "inventory_snapshots": "item_location_map (per-tenant workbook)",
    "gl_entries": "transaction_accounting_line",
}


class NetsuiteConnector(BaseConnector):
    """SuiteQL adapter. Credential-gated; ``dry_run()`` needs no network."""

    erp_id = "netsuite"
    maturity = ConnectorMaturity.IMPLEMENTED  # coded — but see UNEXERCISED note above
    extraction_notes = (
        "SuiteQL REST queries over the transaction/transaction_line model with "
        "TBA token auth. Not yet exercised against a live tenant; confirm "
        "subsidiary scoping and custom segments at onboarding."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "customers": ("customer_no",),
        "vendors": ("vendor_no",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch_code", "item_no"),
        "gl_entries": ("journal_no", "line_no"),
    }

    REQUIRED_SETTINGS: ClassVar[tuple[str, ...]] = (
        "account_id",
        "consumer_key",
        "consumer_secret",
        "token_id",
        "token_secret",
    )

    PAGE_SIZE = 200

    # ------------------------------------------------------------------
    # Contract surface
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    def validate_config(self) -> list[str]:
        missing = [f for f in self.REQUIRED_SETTINGS if not self.source.settings.get(f)]
        if missing:
            return [
                f"missing required NetSuite credential settings: {', '.join(missing)} "
                "(see .env.example; source stays disabled until configured)"
            ]
        return []

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        if entity not in _ENTITY_TABLES:
            raise ConnectorError(
                f"entity '{entity}' has no SuiteQL mapping yet; known: "
                f"{', '.join(sorted(_ENTITY_TABLES))}"
            )
        return ExtractionPlan(
            entity=entity,
            surface=f"SuiteQL POST {SUITEQL_PATH} over {_ENTITY_TABLES[entity]}",
            incremental_key="t.trandate (last successful watermark) — confirm with tenant accounting",
            notes=self.extraction_notes,
        )

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        if entity not in _ENTITY_SQL:
            raise ConnectorError(
                f"entity '{entity}' has no SuiteQL query yet; first-wave mapping covers: "
                f"{', '.join(sorted(_ENTITY_SQL))}"
            )
        sql = _ENTITY_SQL[entity].format(
            watermark=f"'{watermark}'" if watermark else "DATE '1900-01-01'"
        )
        offset = 0
        while True:
            payload = {"q": f"{sql} LIMIT {self.PAGE_SIZE} OFFSET {offset}"}
            response = self._suiteql(payload)
            rows = response.get("items", [])
            if not rows:
                return
            yield from rows
            if len(rows) < self.PAGE_SIZE:
                return
            offset += self.PAGE_SIZE

    # ------------------------------------------------------------------
    # NetSuite REST plumbing (token-based auth, OAuth1 HMAC-SHA256)
    # ------------------------------------------------------------------

    def _suiteql(self, payload: dict[str, object]) -> dict[str, object]:
        url = (
            f"https://{self.source.settings['account_id']}.suitetalk.api.netsuite.com{SUITEQL_PATH}"
        )
        body = json.dumps(payload)
        authorization = self._oauth1_header(url, "POST")
        try:
            response = httpx.post(
                url,
                content=body,
                headers={
                    "Authorization": authorization,
                    "Content-Type": "application/json",
                    "Prefer": "transient",
                },
                timeout=60.0,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise ConnectorError(f"SuiteQL call failed: {exc}") from exc
        data: dict[str, object] = response.json()
        return data

    def _oauth1_header(self, url: str, method: str) -> str:
        settings = self.source.settings
        realm = settings.get("realm") or settings["account_id"].upper()
        params = {
            "oauth_consumer_key": settings["consumer_key"],
            "oauth_nonce": uuid.uuid4().hex,
            "oauth_signature_method": "HMAC-SHA256",
            "oauth_timestamp": str(int(time.time())),
            "oauth_token": settings["token_id"],
            "oauth_version": "1.0",
        }
        base_url, _, query = url.partition("?")
        encoded = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        base_string = "&".join(
            (method.upper(), urllib.parse.quote(base_url, safe=""), urllib.parse.quote(encoded))
        )
        if query:  # SuiteQL posts carry no query string; kept for correctness
            base_string += "&" + urllib.parse.quote(query, safe="")
        signing_key = "&".join(
            (
                urllib.parse.quote(settings["consumer_secret"], safe=""),
                urllib.parse.quote(settings["token_secret"], safe=""),
            )
        )
        signature = base64.b64encode(
            hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha256).digest()
        ).decode()
        params["oauth_signature"] = signature
        header = ", ".join(f'{k}="{urllib.parse.quote(v, safe="")}"' for k, v in params.items())
        return f'OAuth {header}, realm="{realm}"'
