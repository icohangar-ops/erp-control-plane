"""Environment-driven GenBI promotion settings (12-factor, like control_plane/config.py).

Every knob comes from the environment; nothing here holds credentials beyond the
demo defaults already public in ``analytics/superset/build_dashboard.py``. Paths
default under the gitignored ``data/`` tree — runtime state, not versioned content.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from genbi.connection import read_only_duckdb_uri

REPO_ROOT = Path(__file__).resolve().parents[2]

# Demo default mirrors the v0.1 runbook's local Compose Superset; production
# deployments override every credential via environment.
DEMO_SUPERSET_PASSWORD = "RidgelineDemo2026!"


@dataclass(frozen=True)
class GenbiSettings:
    """Deployment settings for the answer-promotion loop (spec §4.2)."""

    superset_url: str
    superset_user: str
    superset_password: str
    # Pre-registered READ_ONLY DuckDB database id in Superset — the ONLY database
    # the NL path may persist datasets against (spec §4.2.6). None = not configured.
    superset_readonly_database_id: int | None
    # Canonical READ_ONLY DuckDB URI from genbi.connection (spec §2.3 same-options rule).
    duckdb_uri: str
    marts_schema: str
    dashboard_slug: str
    dashboard_title: str
    row_cap: int
    statement_timeout_seconds: float
    audit_path: Path
    coverage_path: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> GenbiSettings:
        env = dict(os.environ if env is None else env)
        duckdb_path = env.get("GENBI_ANALYTICS_DUCKDB_PATH", "data/analytics/analytics.duckdb")
        state_dir = Path(env.get("GENBI_STATE_DIR", str(REPO_ROOT / "data" / "genbi")))
        return cls(
            superset_url=env.get("GENBI_SUPERSET_URL", "http://localhost:8088"),
            superset_user=env.get("GENBI_SUPERSET_USER", "admin"),
            superset_password=env.get("GENBI_SUPERSET_PASSWORD", DEMO_SUPERSET_PASSWORD),
            superset_readonly_database_id=(
                int(env["GENBI_SUPERSET_READONLY_DATABASE_ID"])
                if env.get("GENBI_SUPERSET_READONLY_DATABASE_ID")
                else None
            ),
            duckdb_uri=read_only_duckdb_uri(duckdb_path),
            marts_schema=env.get("GENBI_MARTS_SCHEMA", "main_marts"),
            dashboard_slug=env.get("GENBI_DASHBOARD_SLUG", "genbi-ask-save"),
            dashboard_title=env.get("GENBI_DASHBOARD_TITLE", "GenBI — Ask → Save"),
            row_cap=int(env.get("GENBI_ROW_CAP", "10000")),
            statement_timeout_seconds=float(env.get("GENBI_STATEMENT_TIMEOUT_SECONDS", "30")),
            audit_path=Path(env.get("GENBI_AUDIT_PATH", str(state_dir / "audit.jsonl"))),
            coverage_path=Path(
                env.get("GENBI_COVERAGE_PATH", str(state_dir / "coverage_requests.jsonl"))
            ),
        )


class NotConfigured(Exception):
    """Promotion was invoked without the required Superset configuration."""
