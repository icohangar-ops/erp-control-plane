# Curated MDL — WrenAI semantic layer

This directory is the **curated** Modeling Definition Language project WrenAI serves
against the built analytics DuckDB (`data/analytics/analytics.duckdb`). It maps 1:1 to
the modeled dbt surface: the 8 canonical dimensions, 5 facts, and 8 KPI marts in
`main_canonical` / `main_marts`. Staging, seeds/reference, and the `crosswalk_`/`ref_`
integration plumbing are deliberately **not modeled** — they are not business surface.

## Layout

| Path                     | Contents                                                          |
|--------------------------|-------------------------------------------------------------------|
| `wren_project.yml`       | Project manifest (schema_version 5, data_source duckdb)            |
| `models/<name>/metadata.yml` | One model per dbt mart: table_reference, typed columns, PK, descriptions |
| `relationships.yml`      | Joins inferred from dbt relationship tests (surrogate-key, fact→dim) |
| `views/<name>/metadata.yml` | Governed read-only SELECT projections (region revenue, latest inventory) |
| `knowledge/`             | Business terms (verified KPI formulas) and sample questions        |
| `CURATION.md`            | The curation decisions and how this tree was produced              |

## How this MDL is produced

1. `python -m genbi.mdl_gen generate` emits a **first draft** into `genbi/draft/`
   (gitignored) from `dbt/target/manifest.json` + `catalog.json`. Drafts are never
   edited by hand and never shipped.
2. A human curates the draft into this tree: business descriptions, grain,
   relationship types, knowledge vocabulary, governed views.
3. `python -m genbi.mdl_gen validate --dbt-dir dbt/target \
   --duckdb data/analytics/analytics.duckdb` checks structure, the modeled-surface
   boundary, column/type/grain coupling to dbt (spec §3.3), and physical
   executability against the built DuckDB. CI runs this on every PR.

A dbt change that alters mart grain or a metric column **fails CI** until the
matching MDL model is updated in the same PR — that is the coupling gate in
`genbi/mdl_gen/validator.py::check_coupling`.

## How this MDL is served

WrenAI consumes the MDL through the `genbi` Compose profile (see
`docker/compose/genbi/README.md`):

1. `docker compose --profile genbi up` starts wren-ui, wren-ai-service, wren-engine,
   ibis-server, qdrant, and wren-postgres, plus the `genbi-bootstrap` one-shot
   service.
2. `genbi-bootstrap` (a `python:3.12-slim` container running
   `docker/compose/genbi/bootstrap/init.py`) posts this MDL project to the WrenAI
   UI API — the programmatic equivalent of `wren context build` — and exits 0 when
   the project is registered and its context is built. The bootstrap:
   - connects WrenAI to DuckDB using the **same READ_ONLY URI** Superset uses
     (`genbi/connection.py::duckdb_uri` — one config source, §2.3 same-options rule),
   - uploads `wren_project.yml`, `models/`, `relationships.yml`, `views/`, and
     `knowledge/` as the project MDL,
   - triggers context (dictionary/index) build so NL questions resolve business
     terms from `knowledge/`.
3. Ask questions at the wren-ui web app (port 3001 in the profile) or via the
   wren-ai-service API. Answers are governed SQL over the MDL: tables, charts, and
   dashboards generated inside the WrenAI UI.

If you run WrenAI interactively instead of via the bootstrap, the manual substitute
is: open wren-ui → New Data Source → DuckDB → paste the same READ_ONLY URI from
`python -c "from genbi.connection import duckdb_uri; print(duckdb_uri())"` →
New Project → import the MDL from this directory → Build Context.
