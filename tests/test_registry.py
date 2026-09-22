"""Registry behavior: sources.yml loading, env interpolation, template gating."""

from __future__ import annotations

import pytest

from connectors.base import ConnectorError
from connectors.legacy.informix import InformixConnector
from connectors.registry import load_source_configs

FIRST_WAVE = {
    "csvsftp_ridgeline",
    "netsuite_template",
    "bistrack_template",
    "dmsi_agility_template",
    "epicor_p21_template",
    "epicor_eclipse_template",
    "eci_spruce_template",
    "d365_bc_template",
    # Demo Informix tenant (GenBI end-to-end demo; runner enables it in memory).
    "informix_demo",
    "cloud_erp_rest_template",
    # Legacy database connector pack (spec §5 rows 1-9; MariaDB shares MySQL's row).
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
}


def test_all_first_wave_sources_are_declared():
    sources = load_source_configs()
    assert {s.source_id for s in sources} == FIRST_WAVE


def test_env_interpolation_falls_back_to_declared_default(monkeypatch):
    monkeypatch.delenv("CSV_SFTP_DROP_ROOT", raising=False)
    ridgeline = next(s for s in load_source_configs() if s.source_id == "csvsftp_ridgeline")
    assert ridgeline.settings["drop_root"] == "seed/dealer_export"


def test_env_interpolation_prefers_environment(monkeypatch):
    monkeypatch.setenv("CSV_SFTP_DROP_ROOT", "/tmp/some-other-drop")
    ridgeline = next(s for s in load_source_configs() if s.source_id == "csvsftp_ridgeline")
    assert ridgeline.settings["drop_root"] == "/tmp/some-other-drop"


def test_templates_are_disabled_and_carry_no_literal_secrets():
    credentialish = ("token", "secret", "password", "api_key", "consumer_key", "dsn")
    for source in load_source_configs():
        if source.source_id.endswith("_template"):
            assert source.enabled is False, f"{source.source_id} must ship disabled"
            for key, value in source.settings.items():
                if any(marker in key.lower() for marker in credentialish):
                    assert "${" in value or value == "", (
                        f"{source.source_id}.{key} must be env-interpolated or empty, not a literal"
                    )


def test_duplicate_source_ids_are_rejected(tmp_path):
    yml = tmp_path / "sources.yml"
    yml.write_text(
        """
version: 1
sources:
  - source_id: dup
    erp: csv_sftp
    description: first
    settings: {}
  - source_id: dup
    erp: csv_sftp
    description: second
    settings: {}
""",
        encoding="utf-8",
    )
    with pytest.raises(ConnectorError, match="duplicate source_id"):
        load_source_configs(yml)


def test_informix_documentation_keeps_the_registry_contract():
    """All-flavors coverage is documentation-only — the registry contract holds.

    Per-flavor coverage (spec art_wGXFbs3x §7/§11) must not change the coded
    path: required_settings stays the standalone ODBC trio, both informix
    registrations ship disabled-first, and settings keep the ODBC shape (the
    CP4D JDBC variant is gated on discovery item [D-1], not implemented).
    """

    assert InformixConnector.required_settings == (
        "odbc_dsn",
        "db_user",
        "db_password",
    )
    by_id = {s.source_id: s for s in load_source_configs()}
    for source_id in ("informix_demo", "informix_template"):
        source = by_id[source_id]
        assert source.enabled is False, f"{source_id} must ship disabled-first"
        assert source.erp == "informix"
        assert set(source.settings) == {"odbc_dsn", "db_user", "db_password"}
