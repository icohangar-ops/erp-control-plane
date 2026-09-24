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
DEMO_SUPERSET_PASSWORD = ""


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
    # CHP gate (consensus-hardening-protocol): the decision ledger path, the
    # golden set used for promotion-time parity evidence, and whether the CHP
    # human lock is mandatory for every promotion.
    chp_decisions_path: Path
    golden_path: Path
    chp_require_human_lock: bool
    # Deployment mode for fail-closed governance defaults ("local" permits the
    # documented dev receipt key; "production" refuses to run without a real one).
    environment: str
    # Tool-approval receipts (cubiczan-chp-mcp): the ledger is stored alongside
    # the CHP decision ledger; the signing key is env-provided (None = unset,
    # which resolves to the dev default locally and a refusal in production).
    approval_receipts_path: Path
    approval_receipt_key: str | None
    # Acquisition data rooms ([Data] P3): the Qdrant surface and the SINGLE
    # uncached policy source for document-level ACLs. The audit trail is
    # stored in the runtime-state tree alongside the other GenBI ledgers.
    qdrant_url: str
    qdrant_api_key: str | None
    qdrant_timeout_seconds: float
    data_room_collection: str
    data_room_policy_path: Path
    data_room_audit_path: Path
    # Protocol-level health probes ([SecOps] P3): the wren-mcp MCP endpoint,
    # probe timeout, and the pinned tool-schema baseline for drift alarms.
    wren_mcp_url: str
    mcp_health_timeout_seconds: float
    mcp_health_baseline_path: Path

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
            chp_decisions_path=Path(
                env.get("GENBI_CHP_DECISIONS_PATH", str(state_dir / "chp_decisions.jsonl"))
            ),
            golden_path=Path(
                env.get(
                    "GENBI_GOLDEN_PATH", str(REPO_ROOT / "analytics" / "evals" / "golden_qa.yaml")
                )
            ),
            chp_require_human_lock=env.get("GENBI_CHP_REQUIRE_HUMAN_LOCK", "").lower()
            in {"1", "true", "yes"},
            environment=env.get("ENVIRONMENT", "local").strip().lower(),
            approval_receipts_path=Path(
                env.get("GENBI_APPROVAL_RECEIPTS_PATH", str(state_dir / "approval_receipts.jsonl"))
            ),
            approval_receipt_key=env.get("GENBI_APPROVAL_RECEIPT_KEY", "").strip() or None,
            qdrant_url=env.get("GENBI_QDRANT_URL", "http://localhost:6333"),
            qdrant_api_key=env.get("GENBI_QDRANT_API_KEY", "").strip() or None,
            qdrant_timeout_seconds=float(env.get("GENBI_QDRANT_TIMEOUT_SECONDS", "10")),
            data_room_collection=env.get("GENBI_DATA_ROOM_COLLECTION", "acquisition_data_rooms"),
            data_room_policy_path=Path(
                env.get("GENBI_DATA_ROOM_POLICY_PATH", str(state_dir / "data_room_acl.json"))
            ),
            data_room_audit_path=Path(
                env.get("GENBI_DATA_ROOM_AUDIT_PATH", str(state_dir / "data_room_audit.jsonl"))
            ),
            wren_mcp_url=env.get("GENBI_WREN_MCP_URL", "http://localhost:8907/mcp"),
            mcp_health_timeout_seconds=float(env.get("GENBI_MCP_HEALTH_TIMEOUT_SECONDS", "5")),
            mcp_health_baseline_path=Path(
                env.get(
                    "GENBI_MCP_HEALTH_BASELINE_PATH", str(state_dir / "mcp_health_baseline.json")
                )
            ),
        )


class NotConfigured(Exception):
    """Promotion was invoked without the required Superset configuration."""
