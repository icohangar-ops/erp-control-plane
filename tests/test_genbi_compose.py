"""GenBI Compose profile and curated-MDL wiring tests.

Structural validation without a Docker daemon: the Compose profile file must
stay additive (every service profile-gated, images digest-pinned, no API keys
committed), the shared DuckDB connection must stay in lockstep across
genbi.connection / the Compose file / the bootstrap, and the curated MDL must
remain the single input the bootstrap serves to WrenAI.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from genbi import connection  # noqa: E402

sys.path.insert(0, str(REPO_ROOT / "docker" / "compose" / "genbi" / "bootstrap"))
import init as bootstrap  # noqa: E402

GENBI_COMPOSE = REPO_ROOT / "docker" / "compose" / "genbi" / "wrenai.yml"
BASE_COMPOSE = REPO_ROOT / "docker-compose.yml"


@pytest.fixture(scope="module")
def genbi_compose() -> dict:
    return yaml.safe_load(GENBI_COMPOSE.read_text())


@pytest.fixture(scope="module")
def base_compose() -> dict:
    return yaml.safe_load(BASE_COMPOSE.read_text())


def test_every_genbi_service_is_profile_gated(genbi_compose: dict) -> None:
    """No service may start without an explicit profile — the base deployment
    and its resource footprint must not change (spec §2)."""
    services = genbi_compose["services"]
    assert services, "genbi profile defines no services"
    for name, service in services.items():
        assert service.get("profiles"), f"{name} has no profiles gate"


def test_genbi_services_are_digest_pinned(genbi_compose: dict) -> None:
    for name, service in genbi_compose["services"].items():
        image = service["image"]
        assert "@sha256:" in image, f"{name} image {image} is not digest-pinned"


def test_genbi_profile_exports_expected_services(genbi_compose: dict) -> None:
    expected = {
        "wren-postgres",
        "qdrant",
        "wren-engine",
        "ibis-server",
        "wren-ai-service",
        "wren-ui",
        "genbi-bootstrap",
    }
    assert expected <= set(genbi_compose["services"])
    # Optional additions ship disabled behind their own profiles.
    assert "genbi-offline" in genbi_compose["services"]["ollama"]["profiles"]
    assert "genbi-mcp" in genbi_compose["services"]["wren-mcp"]["profiles"]


def test_base_compose_includes_genbi_additively(base_compose: dict) -> None:
    includes = base_compose.get("include") or []
    assert "docker/compose/genbi/wrenai.yml" in includes


def test_no_llm_api_key_in_compose_files(genbi_compose: dict) -> None:
    """The key may only appear as env interpolation, never as a literal value."""
    ai_service = genbi_compose["services"]["wren-ai-service"]["environment"]
    assert ai_service["OPENAI_API_KEY"] == "${OPENAI_API_KEY:-}"
    assert ai_service["OPENAI_BASE_URL"] == "${OPENAI_BASE_URL:-https://api.openai.com/v1}"
    for path in (GENBI_COMPOSE, BASE_COMPOSE):
        assert "sk-" not in path.read_text().lower(), f"{path} contains a literal API key"


def test_wren_engine_reads_analytics_read_only(genbi_compose: dict) -> None:
    for service in ("wren-engine", "ibis-server"):
        mounts = genbi_compose["services"][service]["volumes"]
        analytics = [m for m in mounts if m.split(":")[0].startswith("${GENBI_ANALYTICS")]
        assert analytics and analytics[0].endswith(":ro"), (
            f"{service} must mount analytics read-only"
        )


def test_duckdb_connection_stays_in_lockstep() -> None:
    """genbi.connection is the single source (spec §2.3 same-options rule):
    the URI, the ATTACH statement, the Compose env default, and the bootstrap
    constant must all describe the same file with the same READ_ONLY option."""
    uri_path = connection.wren_container_duckdb_uri()
    # Absolute container paths produce the four-slash SQLAlchemy form — pin the
    # helper against itself, not against a hand-typed slash count.
    assert uri_path == connection.read_only_duckdb_uri(connection.GENBI_CONTAINER_DUCKDB_PATH)
    assert uri_path.startswith("duckdb:///")
    assert uri_path.endswith(f"{connection.GENBI_CONTAINER_DUCKDB_PATH}?{connection.URI_QUERY}")
    attach = connection.wren_attach_sql()
    assert attach == (
        f"ATTACH '{connection.GENBI_CONTAINER_DUCKDB_PATH}' AS {connection.CATALOG_STEM} (READ_ONLY);"
    )
    assert bootstrap.DUCKDB_MOUNT_PATH == connection.GENBI_CONTAINER_DUCKDB_PATH
    assert bootstrap.DUCKDB_CATALOG == connection.CATALOG_STEM
    assert bootstrap.attach_sql() == attach
    compose_env = yaml.safe_load(GENBI_COMPOSE.read_text())["services"]["genbi-bootstrap"][
        "environment"
    ]
    assert compose_env["GENBI_DUCKDB_MOUNT_PATH"] == connection.GENBI_CONTAINER_DUCKDB_PATH
    assert compose_env["GENBI_DUCKDB_CATALOG"] == connection.CATALOG_STEM


def test_bootstrap_manifest_serves_curated_mdl() -> None:
    """The bootstrap must serve the curated MDL (not a regenerated draft):
    duckdb datasource, every curated model, every curated relationship,
    catalog-qualified refSql, and descriptions carried through."""
    mdl = bootstrap.load_curated_mdl(REPO_ROOT / "genbi" / "mdl")
    manifest = bootstrap.build_mcp_manifest(mdl)

    assert manifest["dataSource"] == "DUCKDB"
    curated_model_names = {m["name"] for m in mdl["models"]}
    manifest_model_names = {m["name"] for m in manifest["models"]}
    assert manifest_model_names == curated_model_names
    assert all(
        m["refSql"].startswith(f'SELECT * FROM "{connection.CATALOG_STEM}"')
        for m in manifest["models"]
    )
    assert all(m["description"] for m in manifest["models"]), (
        "curated models must carry descriptions"
    )
    assert all(m["columns"] for m in manifest["models"])
    assert len(manifest["relationships"]) == len(mdl["relationships"])
    # No staging/raw surfaces leak into the modeled boundary.
    assert all(
        "main_canonical" in m["refSql"] or "main_marts" in m["refSql"] for m in manifest["models"]
    )


def test_bootstrap_engine_config_replicates_upstream() -> None:
    assert "node.environment=production" in bootstrap.ENGINE_CONFIG_PROPERTIES
    assert "wren.experimental-enable-dynamic-fields=true" in bootstrap.ENGINE_CONFIG_PROPERTIES


def test_curated_mdl_tree_matches_serving_shape() -> None:
    mdl_dir = REPO_ROOT / "genbi" / "mdl"
    assert (mdl_dir / "wren_project.yml").exists()
    assert (mdl_dir / "relationships.yml").exists()
    models = sorted(p.name for p in (mdl_dir / "models").iterdir() if p.is_dir())
    assert models, "curated MDL defines no models"
    for model_dir in (mdl_dir / "models").iterdir():
        if model_dir.is_dir():
            assert (model_dir / "metadata.yml").exists(), f"{model_dir.name} lacks metadata.yml"
