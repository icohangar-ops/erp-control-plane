"""GenBI bootstrap — one-shot WrenAI initialization (spec §2, runs in genbi-bootstrap).

Idempotent; runs on every `docker compose --profile genbi up`:

  1. Writes the Wren engine config.properties into the shared engine volume,
     replicating the upstream wren-bootstrap container behavior.
  2. Emits the MCP config files (flat MDL manifest + DuckDB connection info)
     for the optional, disabled wren-mcp service.
  3. When no WrenAI project exists yet, registers the curated MDL with wren-ui
     over its GraphQL API: DuckDB datasource, models, relationships, model and
     column descriptions, then `deploy` (which builds the MDL context the AI
     service retrieves against). Human curation done afterwards in the UI is
     preserved — step 3 exits early once a project is registered.

GraphQL documents mirror the ones wren-ui's own client sends (see
wren-ui/src/apollo/client/graphql/*.ts at the pinned commit); input shapes are
taken from wren-ui/src/apollo/server/schema.ts. Stdlib + PyYAML only.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import yaml

logging.basicConfig(level=logging.INFO, format="genbi-bootstrap: %(message)s")
log = logging.getLogger("genbi-bootstrap")

GRAPHQL_URL = os.environ.get("WREN_UI_GRAPHQL_URL", "http://wren-ui:3000/api/graphql")
MDL_DIR = Path(os.environ.get("GENBI_MDL_DIR", "/mdl"))
ENGINE_ETC_DIR = Path(os.environ.get("GENBI_ENGINE_ETC_DIR", "/engine-etc"))
MCP_CONFIG_DIR = Path(os.environ.get("GENBI_MCP_CONFIG_DIR", "/mcp-config"))
PROJECT_DISPLAY_NAME = os.environ.get("GENBI_PROJECT_DISPLAY_NAME", "Construction Supplies ERP")
DUCKDB_MOUNT_PATH = os.environ.get("GENBI_DUCKDB_MOUNT_PATH", "/data/analytics/analytics.duckdb")
DUCKDB_CATALOG = os.environ.get("GENBI_DUCKDB_CATALOG", "analytics")

READINESS_TIMEOUT_SECONDS = 120
DEPLOY_TIMEOUT_SECONDS = 240
POLL_INTERVAL_SECONDS = 3

# Engine settings written by upstream's wren-bootstrap init container.
ENGINE_CONFIG_PROPERTIES = (
    "node.environment=production\nwren.experimental-enable-dynamic-fields=true\n"
)


# --- shared DuckDB connection (genbi/connection.py is the source of truth; the
# bootstrap container runs standalone, so the constants are pinned here and a
# unit test keeps them in lockstep with genbi.connection). ---


def attach_sql() -> str:
    return f"ATTACH '{DUCKDB_MOUNT_PATH}' AS {DUCKDB_CATALOG} (READ_ONLY);"


# --- curated MDL loading ---


class MdlError(Exception):
    """Raised when the curated MDL tree is missing or malformed."""


def load_curated_mdl(mdl_dir: Path) -> dict[str, Any]:
    """Parse the curated MDL tree into dicts; missing/empty files fail loudly."""
    project_path = mdl_dir / "wren_project.yml"
    if not project_path.exists():
        raise MdlError(f"missing {project_path}")
    project = yaml.safe_load(project_path.read_text()) or {}

    models: list[dict[str, Any]] = []
    models_dir = mdl_dir / "models"
    if not models_dir.is_dir():
        raise MdlError(f"missing {models_dir}")
    for model_dir in sorted(p for p in models_dir.iterdir() if p.is_dir()):
        meta_path = model_dir / "metadata.yml"
        if not meta_path.exists():
            raise MdlError(f"missing {meta_path}")
        model = yaml.safe_load(meta_path.read_text()) or {}
        if not model.get("name"):
            raise MdlError(f"{meta_path}: model has no name")
        models.append(model)

    rel_path = mdl_dir / "relationships.yml"
    relationships: list[dict[str, Any]] = []
    if rel_path.exists():
        rel_doc = yaml.safe_load(rel_path.read_text()) or {}
        relationships = rel_doc.get("relationships") or []

    models_by_name = {m["name"]: m for m in models}
    for rel in relationships:
        for name in rel.get("models") or []:
            if name not in models_by_name:
                raise MdlError(
                    f"relationship {rel.get('name')!r} references unknown model {name!r}"
                )

    return {"project": project, "models": models, "relationships": relationships}


# --- GraphQL client ---


def graphql_request(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    request = urllib.request.Request(
        GRAPHQL_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.loads(response.read())
    except urllib.error.HTTPError as err:
        raise RuntimeError(
            f"wren-ui GraphQL HTTP {err.code}: {err.read().decode(errors='replace')}"
        ) from err
    if body.get("errors"):
        raise RuntimeError(f"wren-ui GraphQL errors: {body['errors']}")
    return body.get("data") or {}


def wait_for_wren_ui() -> None:
    deadline = time.monotonic() + READINESS_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            graphql_request("query { settings { productVersion } }")
            log.info("wren-ui is ready")
            return
        except (RuntimeError, urllib.error.URLError) as err:
            last_error = err
            time.sleep(POLL_INTERVAL_SECONDS)
    raise RuntimeError(f"wren-ui not ready after {READINESS_TIMEOUT_SECONDS}s: {last_error}")


def is_project_registered() -> bool:
    data = graphql_request("query { settings { dataSource { type } } }")
    return bool((data.get("settings") or {}).get("dataSource"))


# --- step 1: engine config.properties ---


def write_engine_config() -> None:
    ENGINE_ETC_DIR.mkdir(parents=True, exist_ok=True)
    target = ENGINE_ETC_DIR / "config.properties"
    if target.exists() and target.read_text() == ENGINE_CONFIG_PROPERTIES:
        log.info("engine config.properties already current")
        return
    target.write_text(ENGINE_CONFIG_PROPERTIES)
    log.info("wrote engine config.properties")


# --- step 2: MCP config files (flat manifest + connection info) ---


def build_mcp_manifest(mdl: dict[str, Any]) -> dict[str, Any]:
    """Flat MDL Manifest JSON for wren-mcp (dataSource required by its loader)."""
    models = []
    for model in mdl["models"]:
        table_ref = model["table_reference"]
        # Models reference the DuckDB catalog the initSql ATTACHes, fully
        # qualified — the ibis connector connects in-memory.
        ref_sql = f'SELECT * FROM "{DUCKDB_CATALOG}"."{table_ref["schema"]}"."{table_ref["table"]}"'
        columns = []
        for column in model.get("columns") or []:
            column_entry: dict[str, Any] = {
                "name": column["name"],
                "type": column.get("type") or "string",
                "isCalculated": False,
            }
            description = (column.get("properties") or {}).get("description")
            if description:
                column_entry["description"] = description
            columns.append(column_entry)
        models.append(
            {
                "name": model["name"],
                "description": (model.get("properties") or {}).get("description"),
                "refSql": ref_sql,
                "columns": columns,
                "primaryKey": model.get("primary_key"),
                "cached": False,
                "refreshTime": "0",
                "properties": {"description": (model.get("properties") or {}).get("description")},
            }
        )
    relationships = [
        {
            "name": rel["name"],
            "models": rel["models"],
            "joinType": rel["join_type"],
            "condition": rel["condition"],
        }
        for rel in mdl["relationships"]
    ]
    project = mdl["project"]
    return {
        "dataSource": (project.get("data_source") or "duckdb").upper(),
        "catalog": project.get("catalog") or "wren",
        "schema": project.get("schema") or "public",
        "models": models,
        "relationships": relationships,
    }


def write_mcp_config(mdl: dict[str, Any]) -> None:
    MCP_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    manifest = build_mcp_manifest(mdl)
    (MCP_CONFIG_DIR / "mdl.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (MCP_CONFIG_DIR / "connection_info.json").write_text(
        json.dumps({"connectionUrl": f"duckdb://{DUCKDB_MOUNT_PATH}"}, indent=2) + "\n"
    )
    log.info("wrote MCP config files to %s", MCP_CONFIG_DIR)


# --- step 3: register the curated MDL with wren-ui ---


def register_curated_mdl(mdl: dict[str, Any]) -> None:
    init_sql = attach_sql()
    log.info("registering DuckDB datasource (initSql: %s)", init_sql)
    graphql_request(
        """
        mutation SaveDataSource($data: DataSourceInput!) {
          saveDataSource(data: $data) { type properties }
        }
        """,
        {
            "data": {
                "type": "DUCKDB",
                "properties": {
                    "displayName": PROJECT_DISPLAY_NAME,
                    "initSql": init_sql,
                    "extensions": [],
                    "configurations": {},
                },
            }
        },
    )

    log.info("creating %d models", len(mdl["models"]))
    for model in mdl["models"]:
        variables: dict[str, Any] = {
            "sourceTableName": model["table_reference"]["table"],
            "fields": [c["name"] for c in model.get("columns") or []],
        }
        if model.get("primary_key"):
            variables["primaryKey"] = model["primary_key"]
        graphql_request(
            "mutation CreateModel($data: CreateModelInput!) { createModel(data: $data) }",
            {"data": variables},
        )
        log.info("created model %s", model["name"])

    registry = graphql_request(
        "query { listModels { id referenceName fields { id referenceName } } }"
    )["listModels"]
    models_by_name = {m["referenceName"]: m for m in registry}

    relations = mdl["relationships"]
    log.info("creating %d relationships", len(relations))
    for rel in relations:
        left, right = rel["models"]
        left_side, right_side = rel["condition"].split("=", maxsplit=1)
        create_relation(
            models_by_name, rel["name"], left, left_side, right, right_side, rel["join_type"]
        )
        log.info("created relationship %s", rel["name"])

    log.info("applying descriptions to %d models", len(mdl["models"]))
    for model in mdl["models"]:
        model_id = models_by_name[model["name"]]["id"]
        columns_by_ref = {
            f["referenceName"]: f["id"] for f in models_by_name[model["name"]]["fields"]
        }
        column_updates = []
        for column in model.get("columns") or []:
            description = (column.get("properties") or {}).get("description")
            column_id = columns_by_ref.get(column["name"])
            if description and column_id:
                column_updates.append({"id": column_id, "description": description})
        graphql_request(
            """
            mutation UpdateModelMetadata($where: ModelWhereInput!, $data: UpdateModelMetadataInput!) {
              updateModelMetadata(where: $where, data: $data)
            }
            """,
            {
                "where": {"id": model_id},
                "data": {
                    "description": (model.get("properties") or {}).get("description"),
                    "columns": column_updates,
                },
            },
        )

    log.info("deploying (builds the MDL context for the AI service)")
    deployed = graphql_request("mutation Deploy { deploy }")
    if not deployed.get("deploy"):
        raise RuntimeError("wren-ui deploy returned false; check wren-ui logs")
    poll_deploy_status()


def create_relation(
    models_by_name: dict[str, Any],
    rel_name: str,
    left_model: str,
    left_side: str,
    right_model: str,
    right_side: str,
    join_type: str,
) -> None:
    def resolve(model_name: str, side: str) -> tuple[int, int]:
        model = models_by_name.get(model_name)
        if not model:
            raise MdlError(f"relationship {rel_name!r}: model {model_name!r} missing in wren-ui")
        column = side.strip().rsplit(".", maxsplit=1)[-1].strip()
        for field in model["fields"]:
            if field["referenceName"] == column:
                return model["id"], field["id"]
        raise MdlError(
            f"relationship {rel_name!r}: column {column!r} not found on model {model_name!r}"
        )

    from_model_id, from_column_id = resolve(left_model, left_side)
    to_model_id, to_column_id = resolve(right_model, right_side)
    graphql_request(
        "mutation CreateRelationship($data: RelationInput!) { createRelation(data: $data) }",
        {
            "data": {
                "fromModelId": from_model_id,
                "fromColumnId": from_column_id,
                "toModelId": to_model_id,
                "toColumnId": to_column_id,
                "type": join_type.upper(),
            }
        },
    )


def poll_deploy_status() -> None:
    deadline = time.monotonic() + DEPLOY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            data = graphql_request("query { modelSync { status } }")
            status = (data.get("modelSync") or {}).get("status")
        except RuntimeError as err:
            log.warning("modelSync status query failed (%s); continuing", err)
            return
        if status in (None, "FINISHED", "ERROR", "error"):
            if status == "ERROR" or status == "error":
                raise RuntimeError(f"wren-ui model sync ended with status {status!r}")
            log.info("deploy finished (modelSync status: %s)", status)
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    log.warning(
        "modelSync still pending after %ss; the context build continues in wren-ui",
        DEPLOY_TIMEOUT_SECONDS,
    )


def main() -> None:
    mdl = load_curated_mdl(MDL_DIR)
    log.info(
        "curated MDL: %d models, %d relationships",
        len(mdl["models"]),
        len(mdl["relationships"]),
    )
    write_engine_config()
    write_mcp_config(mdl)
    wait_for_wren_ui()
    if is_project_registered():
        log.info(
            "WrenAI project already registered; skipping MDL registration "
            "(human curation in the UI is preserved)"
        )
        return
    register_curated_mdl(mdl)
    log.info("GenBI bootstrap complete; WrenAI UI is served on the configured host port")


if __name__ == "__main__":
    main()
