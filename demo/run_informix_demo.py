#!/usr/bin/env python3
"""Run the seeded Ridgeline dealer through the demo Informix path (GenBI demo).

The demo tenant's ERP is stood in, not the dealer SFTP drop: the same seeded
export (``seed/dealer_export/``) is loaded into a local DuckDB database shaped
like the Informix source (``ifx_*`` tables with the connector's watermark
columns), then extracted through the **real** ``InformixConnector`` from the
legacy connector pack (spec §5 row 1) — watermark SQL, provenance stamping,
Parquet promotion, and the §6 anti-join delete reconciliation all run for
real. Only the pyodbc transport is stood in (a live Informix is not part of
the demo; the connector's injectable connection factory is the designed
seam, the same one the fixture tests use).

Steps:

1. Build the stand-in Informix database (idempotent: rebuilt deterministically
   from the seed CSVs on every run).
2. Extract all nine canonical entities through ``InformixConnector`` ->
   Parquet, run the anti-join reconciliation for every entity.
3. ``dbt build`` against the same DuckDB analytics target, reading the
   Informix-extracted staging (``--vars '{"demo_source": "informix"}'``).
4. Read the headline KPI mart and print a report.

Exit code is non-zero if any step fails.
"""

from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from connectors.base import ExtractionMode  # noqa: E402
from connectors.legacy.informix import InformixConnector  # noqa: E402
from connectors.legacy.schemas import CANONICAL_ENTITY_COLUMNS  # noqa: E402
from connectors.registry import load_source_configs  # noqa: E402
from control_plane.config import ControlPlaneConfig  # noqa: E402
from control_plane.store import open_store  # noqa: E402

DEMO_SOURCE_ID = "informix_demo"
STANDIN_DB_RELATIVE = "data/demo_informix/ifx_demo.duckdb"
SEED_DIR = REPO_ROOT / "seed" / "dealer_export"

# Table name per entity, straight from InformixConnector.entity_sources —
# kept in lockstep by the demo (a drift here fails the extraction loudly).
# Deterministic watermark value for the stand-in source.
LAST_MODIFIED = "2026-09-19 00:00:00"

# canonical kind -> DuckDB cast (the CSV registry's value kinds).
_KIND_CASTS = {
    "string": "VARCHAR",
    "integer": "BIGINT",
    "decimal": "DECIMAL(18,4)",
    "date": "DATE",
}


def step(title: str) -> None:
    print(f"\n=== {title} " + "=" * max(0, 62 - len(title)))


def build_standin_database(db_path: Path) -> None:
    """Create the stand-in Informix database from the seeded dealer export."""
    step("STEP 1 · stand-in Informix database (seed export -> ifx_* tables)")
    import duckdb

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()  # deterministic rebuild from the seed CSVs

    con = duckdb.connect(str(db_path))
    try:
        for entity, columns in CANONICAL_ENTITY_COLUMNS.items():
            table = InformixConnector.entity_sources[entity].table
            watermark = InformixConnector.entity_sources[entity].incremental_column
            casts = ", ".join(
                f"CAST({name} AS {_KIND_CASTS[kind]}) AS {name}" for name, kind in columns
            )
            csv_path = SEED_DIR / f"{entity}.csv"
            if not csv_path.exists():
                raise FileNotFoundError(f"seed export missing for entity {entity}: {csv_path}")
            con.execute(
                f"CREATE TABLE {table} AS SELECT {casts} FROM "
                f"read_csv_auto('{csv_path}', header=true)"
            )
            # Add the connector's watermark column where the source table does
            # not already carry one (inventory uses snapshot_date, GL entry_date).
            if watermark not in {name for name, _ in columns}:
                con.execute(
                    f"ALTER TABLE {table} ADD COLUMN {watermark} TIMESTAMP;"
                    f"UPDATE {table} SET {watermark} = TIMESTAMP '{LAST_MODIFIED}'"
                )
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:>5} rows (watermark: {watermark})")
    finally:
        con.close()
    print(f"  stand-in database: {db_path}")


def extract(config: ControlPlaneConfig) -> int:
    step("STEP 2 · extraction through InformixConnector (legacy pack -> Parquet)")
    store = open_store(config)
    source = next((s for s in load_source_configs() if s.source_id == DEMO_SOURCE_ID), None)
    if source is None:
        print(f"ERROR: {DEMO_SOURCE_ID} is not declared in connectors/sources.yml")
        return 1
    # Ships disabled next to the templates (CI keeps csvsftp_ridgeline the
    # only enabled source); the demo runner enables it in memory.
    source = replace(source, enabled=True)

    def duckdb_factory(settings: dict[str, str]):
        # The pyodbc transport stood in by the local DB-API connection —
        # the same injectable seam the fixture tests use. Read-only: the
        # extractor never writes to a source.
        import duckdb

        return duckdb.connect(str(REPO_ROOT / STANDIN_DB_RELATIVE), read_only=True)

    connector = InformixConnector(source, store, config, connection_factory=duckdb_factory)
    connector.register()
    total_rows = 0
    for entity in connector.entities():
        result = connector.extract(entity, ExtractionMode.BACKFILL)
        total_rows += result.rows_extracted
        print(
            f"  {source.source_id}/{entity}: "
            f"{result.rows_extracted:>6} rows -> {result.parquet_path}"
        )
    print(f"  total newly extracted rows: {total_rows}")

    step("STEP 2b · §6 anti-join delete reconciliation (per entity)")
    for entity in connector.entities():
        recon = connector.reconcile_deletes(entity)
        print(
            f"  {entity}: source keys {recon.source_key_count}, "
            f"warehouse keys {recon.warehouse_key_count}, "
            f"tombstoned {len(recon.tombstoned_keys)}"
        )
    return 0


def dbt_build(config: ControlPlaneConfig) -> int:
    step("STEP 3 · dbt build (informix staging -> canonical -> marts + tests)")
    config.analytics_duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    completed = subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
            "--target",
            "demo",
            "--vars",
            '{"demo_source": "informix"}',
            # The CSV-path staging models read the csv_sftp Parquet, which a
            # fresh Informix-only demo has not produced; canonical lineage runs
            # through the informix staging (demo_staging macro), so exclude the
            # csvsftp folder rather than fabricate its inputs.
            "--exclude",
            "models/staging/csvsftp_ridgeline",
        ],
        cwd=REPO_ROOT,
        text=True,
    )
    if completed.returncode != 0:
        print("ERROR: dbt build failed — see output above")
        return completed.returncode
    print(f"  dbt build finished in {time.monotonic() - started:.1f}s")
    return 0


def kpi_report(config: ControlPlaneConfig) -> int:
    step("STEP 4 · headline KPIs (main_marts.kpi_headline, loaded via Informix path)")
    import duckdb

    con = duckdb.connect(str(config.analytics_duckdb_path), read_only=True)
    try:
        cursor = con.execute("SELECT * FROM main_marts.kpi_headline")
        columns = [d[0] for d in cursor.description]
        row = dict(zip(columns, cursor.fetchone(), strict=True))
    finally:
        con.close()
    print(
        f"  window: {row['window_start']} -> {row['window_end']} ({int(row['window_days'])} days)\n"
    )
    print(
        f"  GMROI: {row['gmroi']:.2f} · turns: {row['inventory_turns']:.2f} · "
        f"gross margin: {row['gross_margin_pct'] * 100:.1f}% · "
        f"line fill: {row['line_fill_rate'] * 100:.1f}%"
    )
    return 0


def main() -> int:
    config = ControlPlaneConfig.from_env()
    print(
        "construction-supplies-erp-control-plane · GenBI demo run "
        "(seeded Ridgeline dealer through the demo Informix path)"
    )
    build_standin_database(REPO_ROOT / STANDIN_DB_RELATIVE)
    if extract(config) != 0:
        return 1
    if dbt_build(config) != 0:
        return 2
    if kpi_report(config) != 0:
        return 3
    step("GENBI INFORMIX DEMO COMPLETE")
    print(f"{STANDIN_DB_RELATIVE}       — stand-in Informix source database")
    print("data/lake/parquet/informix_demo/  — InformixConnector-extracted staging Parquet")
    print(str(config.analytics_duckdb_path) + "     — DuckDB analytics engine (canonical marts)")
    print("re-run any time (deterministic, idempotent)\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
