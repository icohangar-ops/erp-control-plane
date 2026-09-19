"""bistrack — Epicor BisTrack extraction (SKELETON, dual mode).

Extraction notes (ERP landscape research, art_NKUrngnG):
- BisTrack is Epicor's lumber/construction-supplies dealer ERP. Two deployment
  shapes decide the extraction surface:
    * on-prem: MS SQL Server database — read via a read-only ODBC login
      (pyodbc, server-side cursors, prefer a log-shipped replica over the
      transactional primary). Direct SQL is the highest-fidelity surface.
    * cloud/hosted: BisTrack Web Smart View API — REST-style reporting API
      exposed by the BisTrack Web add-on; use when the tenant cannot grant
      direct DB access.
- Dealer back offices vary heavily: confirm which modules the tenant runs
  (POS, dispatch, service) before finalizing entity coverage.
- Sales/invoice history: order and invoice header/line tables joined to
  product, customer, and branch masters.

TODO(per-tenant): determine deployment shape (SQL vs Smart View), request a
read-only SQL login or Smart View API key, inventory actual table/column
names on the tenant version, and pick the incremental keys below.

This module is a SKELETON: mode-aware plans are implemented, row extraction
is not (raises ConnectorNotImplemented) until tenant discovery completes.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import ClassVar

from connectors.base import (
    ConnectorError,
    ConnectorNotImplemented,
    ExtractionMode,
    ExtractionPlan,
)
from connectors.stubs.skeleton import SkeletonConnector

ODBC_MODE = "odbc"
SMARTVIEW_MODE = "smartview"

#: per-mode, per-entity extraction surfaces (where the data lives)
_ODBC_SURFACE: dict[str, str] = {
    "items": "SQL Server ODBC read-only: product/inventory master tables",
    "customers": "SQL Server ODBC read-only: customer master tables",
    "vendors": "SQL Server ODBC read-only: vendor master tables",
    "sales_order_lines": "SQL Server ODBC read-only: order header + line tables",
    "invoice_lines": "SQL Server ODBC read-only: invoice header + line tables",
    "purchase_order_lines": "SQL Server ODBC read-only: PO header + line tables",
    "inventory_snapshots": "SQL Server ODBC read-only: stock by branch (snapshot job)",
    "gl_entries": "SQL Server ODBC read-only: GL detail tables",
}

_SMARTVIEW_SURFACE: dict[str, str] = {
    "items": "BisTrack Web Smart View API: product reporting endpoints",
    "customers": "BisTrack Web Smart View API: customer reporting endpoints",
    "vendors": "BisTrack Web Smart View API: vendor reporting endpoints",
    "sales_order_lines": "BisTrack Web Smart View API: order reporting endpoints",
    "invoice_lines": "BisTrack Web Smart View API: invoice reporting endpoints",
    "purchase_order_lines": "BisTrack Web Smart View API: PO reporting endpoints",
    "inventory_snapshots": "BisTrack Web Smart View API: stock reporting endpoints",
}


class BisTrackConnector(SkeletonConnector):
    erp_id = "bistrack"
    extraction_notes = (
        "Epicor BisTrack: on-prem SQL Server via read-only ODBC, or BisTrack "
        "Web Smart View API where direct DB access is not granted. Skeleton "
        "pending deployment-shape discovery at the tenant."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "items": ("item_no",),
        "customers": ("customer_no",),
        "vendors": ("vendor_no",),
        "sales_order_lines": ("order_no", "line_no"),
        "invoice_lines": ("invoice_no", "line_no"),
        "purchase_order_lines": ("po_no", "line_no"),
        "inventory_snapshots": ("snapshot_date", "branch_code", "item_no"),
        "gl_entries": ("journal_no", "line_no"),
    }
    extraction_surface: ClassVar[dict[str, str]] = {}  # resolved per mode below
    incremental_keys: ClassVar[dict[str, str]] = {
        "items": "last-maintained timestamp (confirm on tenant)",
        "customers": "last-maintained timestamp (confirm on tenant)",
        "vendors": "last-maintained timestamp (confirm on tenant)",
        "sales_order_lines": "order date",
        "invoice_lines": "invoice date",
        "purchase_order_lines": "PO date",
        "inventory_snapshots": "snapshot date",
        "gl_entries": "GL entry date",
    }
    required_settings: ClassVar[tuple[str, ...]] = ("mode",)

    # ------------------------------------------------------------------
    # Mode-aware overrides
    # ------------------------------------------------------------------

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    @property
    def mode(self) -> str:
        mode = self.source.settings.get("mode", ODBC_MODE)
        if mode not in (ODBC_MODE, SMARTVIEW_MODE):
            raise ConnectorError(
                f"bistrack mode '{mode}' is invalid; expected '{ODBC_MODE}' or '{SMARTVIEW_MODE}'"
            )
        return mode

    def validate_config(self) -> list[str]:
        problems: list[str] = []
        try:
            mode = self.mode
        except ConnectorError as exc:
            return [str(exc)]
        if mode == ODBC_MODE:
            for setting in ("odbc_dsn", "db_user", "db_password"):
                if not self.source.settings.get(setting):
                    problems.append(
                        f"bistrack odbc mode requires '{setting}' (read-only login; "
                        "prefer a replica over the transactional primary)"
                    )
        else:
            for setting in ("smartview_base_url", "smartview_api_key"):
                if not self.source.settings.get(setting):
                    problems.append(f"bistrack smartview mode requires '{setting}'")
        return problems

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        if entity not in self.natural_key_fields:
            raise ConnectorError(
                f"entity '{entity}' is not declared by bistrack; declared: "
                f"{', '.join(sorted(self.natural_key_fields))}"
            )
        surface = _ODBC_SURFACE[entity] if self.mode == ODBC_MODE else _SMARTVIEW_SURFACE[entity]
        return ExtractionPlan(
            entity=entity,
            surface=surface,
            incremental_key=self.incremental_keys.get(entity),
            notes=self.extraction_notes,
        )

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        raise ConnectorNotImplemented(
            f"bistrack extraction is not implemented for mode '{self.mode}' until "
            f"per-tenant discovery completes (see docs/CONNECTOR_GUIDE.md). Planned "
            f"surface for '{entity}': {self.describe_extraction(entity).surface}"
        )
