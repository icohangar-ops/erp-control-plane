"""Connector contract suite — every registered connector passes these.

New connectors must satisfy this suite before they can be added to
sources.yml. See docs/CONNECTOR_GUIDE.md.
"""

from __future__ import annotations

import pytest

from connectors.base import ConnectorMaturity, ConnectorNotImplemented, config_fingerprint
from connectors.registry import build_registry_connectors, load_source_configs
from control_plane.config import ControlPlaneConfig
from control_plane.store import SqliteControlPlaneStore


def _connectors(tmp_path):
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
    return build_registry_connectors(config=config, store=store)


@pytest.fixture(scope="module")
def all_connectors(tmp_path_factory):
    return _connectors(tmp_path_factory.mktemp("contract"))


def test_every_source_in_sources_yml_builds_a_connector(all_connectors):
    sources = load_source_configs()
    assert len(all_connectors) == len(sources) == 8
    assert {c.source.source_id for c in all_connectors} == {s.source_id for s in sources}


def test_demo_source_is_the_only_enabled_connector(all_connectors):
    enabled = [c for c in all_connectors if c.source.enabled]
    assert [c.source.source_id for c in enabled] == ["csvsftp_ridgeline"]
    assert enabled[0].maturity is ConnectorMaturity.IMPLEMENTED
    templates = [c for c in all_connectors if not c.source.enabled]
    assert len(templates) == 7
    by_id = {c.source.source_id: c for c in templates}
    # NetSuite is coded but credential-gated (never exercised against a tenant);
    # the remaining six are documented skeletons that never fabricate data.
    assert by_id["netsuite_template"].maturity is ConnectorMaturity.IMPLEMENTED
    skeletons = [c for c in templates if c.source.source_id != "netsuite_template"]
    assert all(c.maturity is ConnectorMaturity.SKELETON for c in skeletons)


def test_contract_entities_and_natural_keys(all_connectors):
    for connector in all_connectors:
        entities = connector.entities()
        assert entities, f"{connector.source.source_id} declares no entities"
        for entity in entities:
            keys = connector.natural_key_fields.get(entity)
            assert keys, f"{connector.erp_id} missing natural_key_fields for '{entity}'"


def test_contract_extraction_plans_are_documented(all_connectors):
    for connector in all_connectors:
        for entity in connector.entities():
            plan = connector.describe_extraction(entity)
            assert plan.entity == entity
            assert plan.surface, f"{connector.erp_id}/{entity}: empty extraction surface"
            assert plan.notes, f"{connector.erp_id}/{entity}: missing extraction notes"


def test_contract_skeletons_never_fabricate_data(all_connectors):
    for connector in all_connectors:
        if connector.maturity is ConnectorMaturity.IMPLEMENTED:
            continue
        with pytest.raises(ConnectorNotImplemented):
            connector.extract(connector.entities()[0])


def test_contract_provenance_stamp_is_applied_to_every_record(all_connectors):
    """The IMPLEMENTED connector's records all carry provenance columns."""
    import duckdb

    csv_connector = next(c for c in all_connectors if c.source.source_id == "csvsftp_ridgeline")
    result = csv_connector.extract("items")
    assert result.rows_extracted > 0
    table = duckdb.read_parquet(result.parquet_path)
    assert {"source_system", "source_id", "loaded_at"}.issubset(set(table.columns))
    assert table.aggregate("count(DISTINCT source_system)").fetchone()[0] == 1


def test_contract_config_fingerprint_is_order_insensitive():
    a = {"account_id": "123", "token": "abc"}
    b = {"token": "abc", "account_id": "123"}
    assert config_fingerprint(a) == config_fingerprint(b)
    assert config_fingerprint(a) != config_fingerprint({**a, "token": "xyz"})
