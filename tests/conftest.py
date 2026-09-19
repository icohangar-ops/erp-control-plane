"""Shared fixtures: tmp control-plane config, store, and the seeded demo source."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:  # run pytest from anywhere
    sys.path.insert(0, str(REPO_ROOT))

from connectors.registry import load_source_configs  # noqa: E402
from control_plane.config import ControlPlaneConfig  # noqa: E402
from control_plane.models import SourceConfig  # noqa: E402
from control_plane.store import SqliteControlPlaneStore  # noqa: E402

SEED_EXPORT = REPO_ROOT / "seed" / "dealer_export"


@pytest.fixture()
def cp_config(tmp_path: Path) -> ControlPlaneConfig:
    """Control-plane config pointed entirely at a tmp sandbox."""
    return ControlPlaneConfig(
        backend="sqlite",
        sqlite_path=tmp_path / "control_plane.db",
        control_plane_dsn=None,
        lake_root=tmp_path / "lake",
        analytics_duckdb_path=tmp_path / "analytics.duckdb",
        quarantine_root=tmp_path / "quarantine",
        environment="test",
    )


@pytest.fixture()
def store(tmp_path: Path) -> SqliteControlPlaneStore:
    s = SqliteControlPlaneStore(tmp_path / "control_plane.db")
    s.initialize()
    return s


@pytest.fixture()
def ridgeline_config() -> SourceConfig:
    """The enabled csv_sftp demo source as declared in sources.yml."""
    matches = [s for s in load_source_configs() if s.source_id == "csvsftp_ridgeline"]
    assert len(matches) == 1, "csvsftp_ridgeline must be declared exactly once"
    return matches[0]


@pytest.fixture()
def drop_copy(tmp_path: Path) -> Path:
    """A mutable copy of the seeded dealer export (tests may tamper with it)."""
    copy = tmp_path / "dealer_export"
    shutil.copytree(SEED_EXPORT, copy)
    return copy
