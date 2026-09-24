from __future__ import annotations

from pathlib import Path

import httpx

from connectors.base import ExtractionMode
from connectors.essbase.connector import EssbaseConnector
from control_plane.config import ControlPlaneConfig
from control_plane.models import SourceConfig
from control_plane.store import SqliteControlPlaneStore


def _connector(tmp_path: Path) -> EssbaseConnector:
    config = ControlPlaneConfig(
        backend="sqlite",
        sqlite_path=tmp_path / "cp.db",
        control_plane_dsn=None,
        lake_root=tmp_path / "lake",
        analytics_duckdb_path=tmp_path / "analytics.duckdb",
        quarantine_root=tmp_path / "quarantine",
        environment="test",
    )
    store = SqliteControlPlaneStore(tmp_path / "cp.db")
    store.initialize()
    source = SourceConfig(
        source_id="essbase_fixture",
        erp="essbase",
        description="fixture",
        settings={
            "base_url": "https://essbase.fixture/essbase/rest/v1",
            "username": "fixture-user",
            "password": "fixture-password",
        },
        enabled=False,
    )
    return EssbaseConnector(source, store, config)


def test_essbase_metadata_snapshot_and_basic_auth(tmp_path: Path) -> None:
    connector = _connector(tmp_path)
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path.endswith("/applications"):
            return httpx.Response(
                200,
                json={
                    "items": [{"name": "Finance", "owner": "admin", "status": "running"}],
                    "totalResults": 1,
                },
            )
        if request.url.path.endswith("/applications/Finance/databases"):
            return httpx.Response(
                200,
                json={"items": [{"name": "Plan1", "type": "BSO"}], "totalResults": 1},
            )
        return httpx.Response(404)

    connector._http_client = httpx.Client(
        transport=httpx.MockTransport(handler),
        auth=("fixture-user", "fixture-password"),
    )
    result = connector.extract("cubes", ExtractionMode.BACKFILL)

    assert result.rows_extracted == 1
    assert calls[0].headers["authorization"].startswith("Basic ")
    assert calls[0].url.params["limit"] == "100"
