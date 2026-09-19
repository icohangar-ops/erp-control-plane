"""Single source of the READ_ONLY DuckDB URI (GenBI spec §2.3, same-options rule).

DuckDB permits one read-write process or many read-only processes per file, and
applies instance options at first connection — a later reader requesting
different options is rejected. Both readers (Superset, WrenAI) must therefore
use the *same* URI, generated here and never hand-typed per service:

    duckdb:///<analytics.duckdb path>?access_mode=READ_ONLY

Callers:
    - ``analytics/superset/build_dashboard.py`` (Superset database connection)
    - ``genbi/mdl_gen`` / the Compose bootstrap (WrenAI connection settings)

The Compose file carries the container-path default of the same URI; a unit
test pins the two to identical values so they cannot drift.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath, PureWindowsPath

URI_QUERY = "access_mode=READ_ONLY"

# Path the analytics.duckdb file is mounted at inside every GenBI container.
# The host path is configurable via GENBI_ANALYTICS_DUCKDB_HOST_PATH in Compose;
# this container-side path is what the WrenAI connection settings reference.
GENBI_CONTAINER_DUCKDB_PATH = "/data/analytics/analytics.duckdb"


def read_only_duckdb_uri(duckdb_path: str | Path) -> str:
    """Build the canonical READ_ONLY DuckDB URI for ``duckdb_path``.

    Pure string shaping — the file need not exist. Relative paths keep their
    form (SQLAlchemy resolves them against the process working directory);
    absolute paths produce the four-slash SQLAlchemy form. Windows paths are
    converted to POSIX form so the URI stays portable into Linux containers.
    """
    raw = str(duckdb_path)
    # Strip a leading "./" so ./data/x and data/x produce the same URI —
    # the same-options rule requires byte-identical URIs across services.
    if raw.startswith("./"):
        raw = raw[2:]
    if PureWindowsPath(raw).drive:  # e.g. C:\data\analytics.duckdb
        posix = PurePosixPath(PureWindowsPath(raw).as_posix())
        path = str(posix)
    else:
        path = raw
    return f"duckdb:///{path}?{URI_QUERY}"


CATALOG_STEM = "analytics"


def wren_container_duckdb_uri() -> str:
    """The URI configured into WrenAI connection settings (container mount path)."""
    return read_only_duckdb_uri(GENBI_CONTAINER_DUCKDB_PATH)


def wren_attach_sql(catalog: str = CATALOG_STEM) -> str:
    """The ATTACH statement WrenAI runs as its DuckDB initSql (bootstrap-generated).

    Same file and same READ_ONLY option as the URI above — the two forms must
    stay in lockstep because they are the same connection to the same process
    pool the engine opens (spec §2.3 same-options rule).
    """
    return f"ATTACH '{GENBI_CONTAINER_DUCKDB_PATH}' AS {catalog} (READ_ONLY);"
