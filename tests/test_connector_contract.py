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
    # 21 first-wave sources + the demo Informix tenant (GenBI demo path).
    assert len(all_connectors) == len(sources) == 22
    assert {c.source.source_id for c in all_connectors} == {s.source_id for s in sources}


def test_demo_source_is_the_only_enabled_connector(all_connectors):
    enabled = [c for c in all_connectors if c.source.enabled]
    assert [c.source.source_id for c in enabled] == ["csvsftp_ridgeline"]
    assert enabled[0].maturity is ConnectorMaturity.IMPLEMENTED
    templates = [c for c in all_connectors if not c.source.enabled]
    assert len(templates) == 21
    by_id = {c.source.source_id: c for c in templates}
    # Coded-but-unexercised connectors: never run against a live tenant/site,
    # so they stay credential-gated templates (dlt resources exist, fixtures
    # verify the ingestion shape in CI). The legacy SQL pack is in the same
    # boat: implemented and fixture-tested, driver/credential discovery still
    # pending per site.
    implemented_templates = {
        "netsuite_template",
        "d365_bc_template",
        "cloud_erp_rest_template",
        "epicor_p21_template",
        # BisTrack ODBC path is coded (keyset-paged document scans, per-type
        # numbering watermarks) and fixture-tested offline via injected
        # connection factories — like the pack above, it has never run against
        # a live BisTrack site; Smart View stays a documented skeleton mode.
        "bistrack_template",
        # DMSi AgilityPublic path is coded (Session/Login context headers,
        # chunk-pointer paging, customer-scoped orders/invoices, dated
        # inventory snapshots) and fixture-tested offline via MockTransport —
        # like the pack above, it has never run against a live dealer; GL and
        # PO lists stay documented hybrid-channel plans (the spec verifies no
        # AgilityPublic methods exist for them).
        "dmsi_agility_template",
        # ECI Spruce / RockSolid MAX dealer-mediated file-drop path is coded
        # (manifest-gated promotion with stale/regenerated refusal, RSM
        # group/section normalization, scoped full-file anti-join) and
        # fixture-tested offline — it has never run against a live dealer;
        # the SOAP Ecommerce API stays NDA-gated future work (spec §2/§8).
        "eci_spruce_template",
        # Epicor Eclipse REST path is coded (session-token auth over
        # POST /Sessions + /SessionRefresh, per-tenant-pinned query-param
        # paging, updatedAfter watermarks, GLInquiryDetail GL, dated inventory
        # sweeps) and fixture-tested offline via MockTransport — like the pack
        # above, it has never run against a live tenant; the session/paging
        # wire contract is per-tenant [D] pins that fail closed on empty, and
        # invoice lines stay a documented hybrid-channel plan (the spec
        # verifies no /Invoices endpoint exists).
        "epicor_eclipse_template",
        # Demo Informix tenant: enabled only by the GenBI demo runner, still
        # fixture-driven (the pyodbc transport is stood in by the demo).
        "informix_demo",
        "informix_template",
        "db2_luw_template",
        "db2_iseries_template",
        "oracle_template",
        "sqlserver_template",
        "postgresql_template",
        "mysql_template",
        "mariadb_template",
        "sybase_ase_template",
        "openedge_template",
        "sap_hana_template",
        "essbase_template",
    }
    for source_id in implemented_templates:
        assert by_id[source_id].maturity is ConnectorMaturity.IMPLEMENTED
    # Every first-wave template is now coded and fixture-tested; no
    # documented skeletons remain in the registry.
    skeletons = [c for c in templates if c.source.source_id not in implemented_templates]
    assert {c.source.source_id for c in skeletons} == set()
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
