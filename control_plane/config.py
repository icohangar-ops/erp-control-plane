"""Control-plane configuration: environment-driven settings, no hardcoded credentials.

Every deployment knob comes from the environment (12-factor). ``.env.example``
documents the full surface; the compose stack injects the same variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from dotenv import load_dotenv

SQLITE_BACKEND = "sqlite"
POSTGRES_BACKEND = "postgres"


class MissingConfiguration(Exception):
    """A required environment variable is absent — fail loudly, never silently default credentials."""


def require_env(key: str, env: Mapping[str, str]) -> str:
    value = env.get(key)
    if not value:
        raise MissingConfiguration(
            f"Required environment variable {key} is not set. "
            f"Copy .env.example to .env and configure it (no real credentials are shipped with this repo)."
        )
    return value


@dataclass(frozen=True)
class ControlPlaneConfig:
    """Deployment-wide paths and backend selection."""

    backend: str
    sqlite_path: Path | None
    control_plane_dsn: str | None
    lake_root: Path
    analytics_duckdb_path: Path
    quarantine_root: Path
    environment: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ControlPlaneConfig:
        load_dotenv()  # no-op when .env is absent; must run before snapshotting os.environ
        env = dict(os.environ if env is None else env)
        backend = env.get("CONTROL_PLANE_BACKEND", SQLITE_BACKEND)
        if backend not in (SQLITE_BACKEND, POSTGRES_BACKEND):
            raise MissingConfiguration(
                f"CONTROL_PLANE_BACKEND must be '{SQLITE_BACKEND}' or '{POSTGRES_BACKEND}', got '{backend}'"
            )
        sqlite_path = (
            Path(env["CONTROL_PLANE_SQLITE_PATH"]) if env.get("CONTROL_PLANE_SQLITE_PATH") else None
        )
        if backend == SQLITE_BACKEND and sqlite_path is None:
            sqlite_path = Path("./data/control_plane.db")  # sane demo default; overridable via .env
        dsn = env.get("CONTROL_PLANE_DSN")
        if backend == POSTGRES_BACKEND and not dsn:
            raise MissingConfiguration("CONTROL_PLANE_DSN is required when CONTROL_PLANE_BACKEND=postgres")
        return cls(
            backend=backend,
            sqlite_path=sqlite_path,
            control_plane_dsn=dsn,
            lake_root=Path(env.get("LAKE_ROOT", "./data/lake")),
            analytics_duckdb_path=Path(
                env.get("ANALYTICS_DUCKDB_PATH", "./data/analytics/analytics.duckdb")
            ),
            quarantine_root=Path(env.get("QUARANTINE_ROOT", "./data/quarantine")),
            environment=env.get("ENVIRONMENT", "local"),
        )

    def parquet_root(self, source_id: str) -> Path:
        """Directory holding this source's canonical staging Parquet."""
        return self.lake_root / "parquet" / source_id
