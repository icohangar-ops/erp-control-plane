# GenBI profile — WrenAI stack

Additive Docker Compose profile that runs [WrenAI](https://github.com/Canner/WrenAI)
as the governed natural-language / semantic layer over the control-plane marts.
The base deployment (Dagster, dbt, Postgres, Superset) is unchanged: every
service here is profile-gated (`docker compose --profile genbi ...`).

## Versions

Pinned from the WrenAI **0.29.0** release's own `docker/.env.example`
(commit `4a0501247de55bebb42c2944f31b0aa8ae40775f`):

| Service | Image | Version |
|---|---|---|
| wren-ui | `ghcr.io/canner/wren-ui` | 0.31.3 |
| wren-ai-service | `ghcr.io/canner/wren-ai-service` | 0.29.0 |
| wren-engine | `ghcr.io/canner/wren-engine` | 0.21.3 |
| ibis-server | `ghcr.io/canner/wren-engine-ibis` | 0.21.3 |
| qdrant | `qdrant/qdrant` | v1.11.0 |
| wren-mcp deps | (vendored `mcp-server/`) | wren-engine @ `47ca29e` |

Digests are pinned in `wrenai.yml` (resolved from GHCR/Docker Hub 2026-09-19).

## Services

- **wren-ui** — the WrenAI web app (modeling, NL questioning, charts).
  Served on host port **3001** (`GENBI_WREN_UI_PORT`).
- **wren-engine** — the SQL engine; reads the analytics DuckDB **READ_ONLY**.
- **ibis-server** — Wren's Python connector layer (MCP path, external sources).
- **wren-ai-service** — NL-to-SQL via LiteLLM; configuration in `config.yaml`
  (per upstream: model config belongs in config.yaml, not env).
- **qdrant** — vector store for the AI service RAG pipelines.
- **wren-postgres** — WrenAI UI metadata store (DB_TYPE=postgres), separate
  from the control-plane Postgres.
- **genbi-bootstrap** — one-shot init (below).
- **ollama** — only in the `genbi-offline` profile.
- **wren-mcp** — only in the `genbi-mcp` profile (shipped disabled; below).

## DuckDB connection (same-options rule, spec §2.3)

DuckDB allows one read-write process or many read-only processes per file, and
instance options stick at first connection — so Superset and WrenAI must use
the **same** read-only URI. Both are generated from `genbi/connection.py`
(`read_only_duckdb_uri` / `wren_attach_sql`); the Compose file carries the
container-path defaults and a unit test pins them to the Python helper:

```
duckdb:////data/analytics/analytics.duckdb?access_mode=READ_ONLY
ATTACH '/data/analytics/analytics.duckdb' AS analytics (READ_ONLY);
```

The host path defaults to `./data/analytics` (`GENBI_ANALYTICS_DUCKDB_HOST_PATH`).

## LLM wiring

- Cloud (default): export `OPENAI_API_KEY` (or run Compose with the injected
  secret) and optionally `OPENAI_BASE_URL`. The key is **never** committed to
  any file or Compose YAML — it reaches the AI service through the
  `OPENAI_API_KEY` environment variable only.
- Offline: `docker compose --profile genbi --profile genbi-offline up` adds
  Ollama. Set in `.env`:
  `OPENAI_API_KEY=ollama`, `OPENAI_BASE_URL=http://ollama:11434/v1`,
  `GENBI_AI_CONFIG_PATH=/app/config-offline.yaml`, and pull the models
  (`ollama pull qwen2.5:7b-instruct` / `nomic-embed-text`).

## genbi-bootstrap

`bootstrap/init.py` runs on every `up` and is idempotent:

1. Writes the engine `config.properties` (same two properties upstream's
   wren-bootstrap container sets).
2. Emits `mdl.json` + `connection_info.json` for the optional wren-mcp service
   from the curated MDL.
3. If no WrenAI project exists: registers the DuckDB datasource, creates one
   model per curated MDL model, recreates the curated relationships, applies
   model/column descriptions, then calls `deploy` — which builds the MDL
   context the AI service retrieves against.

Once registered, human curation made in the WrenAI UI is preserved (the
bootstrap skips re-registration when a project exists). The GraphQL documents
mirror those of wren-ui's own client at the pinned commit.

## wren-mcp (optional, disabled, known limitation)

`mcp-server/` vendors the Wren MCP server from the wren-engine repo at the
commit pinned by WrenAI 0.29.0 (`47ca29e`); the service runs it with `uv run
--frozen`. It is gated behind the `genbi-mcp` profile and **off by default**
because, verified against the ibis-server source of this release, its DuckDB
connector connects in-memory and initializes only S3/MinIO/GCS object stores —
a file-backed `.duckdb` catalog (our deployment) is not queryable through it.
Enabling it today yields a running MCP server whose tool queries cannot reach
these marts; production MCP access is a Phase 2 item (object-store-backed
DuckDB, or an upstream ibis connector change).

## What is NOT registered in WrenAI

- `genbi/mdl/views/` — governed SQL surfaces stay repository/Superset-owned.
  WrenAI's `createView` mutation is for saved natural-language *answer* views,
  not importing governed SQL, so the curated views are served via Superset and
  documented as the substitute.
- `genbi/mdl/knowledge/business_terms.yml` — curated vocabulary that lives
  with the MDL as the curation reference; the terms' semantics are carried
  into WrenAI through the model/column descriptions. Loading them as WrenAI
  instructions is a Phase 2 item.

## Validation

```
docker compose --profile genbi config >/dev/null   # profile validates
python -m genbi.mdl_gen validate                   # curated MDL validates
pytest tests/test_genbi_compose.py                 # config/coupling tests
```
