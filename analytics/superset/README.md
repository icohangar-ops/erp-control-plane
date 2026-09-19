# analytics/superset — Ridgeline Dealer KPI Dashboard

BI layer over the dbt-built DuckDB marts. The provisioning script is
idempotent: run it after every `dbt build` to (re)create the database
connection, datasets, charts, and dashboard in place.

## Contents

| File | Purpose |
|------|---------|
| `build_dashboard.py` | Idempotent REST provisioning of the full dashboard |
| `headline_kpis.sql` | KPI headline query (reference / SQL Lab) |
| `inventory_efficiency_by_category.sql` | Inventory efficiency query |
| `service_levels_by_branch.sql` | Branch service-level query |

## Runbook

```bash
# 1. Build the marts (writes dealer.duckdb)
dbt build --project-dir dbt

# 2. Start Superset (gunicorn -w 2 is fine — see DuckDB note below)
cd /home/user/superset-bi && \
  SUPERSET_CONFIG_PATH=$(pwd)/superset_config.py FLASK_APP=superset.app:create_app \
  .venv/bin/gunicorn -w 2 -t 120 -b 0.0.0.0:8088 "superset.app:create_app()"

# 3. Provision the dashboard
python analytics/superset/build_dashboard.py
```

The script verifies every chart's query end-to-end via
`POST /api/v1/chart/data` and exits non-zero if any chart returns no rows, so
a broken warehouse build surfaces before the demo does.

Dashboard: `http://localhost:8088/superset/dashboard/ridgeline-dealer-kpis/`

## Hard-won Superset constraints

1. **Layout nesting.** The v2 layout must be nested under `positions` inside
   `json_metadata`. `DashboardDAO.set_dash_metadata` syncs `dashboard.slices`
   and writes the canonical `position_json` column from that key. A top-level
   `position_json` alone leaves the dashboard empty and crashes the SPA.
2. **Container `meta` is mandatory.** Every `ROW` and `COLUMN` node needs a
   `meta` object (at minimum
   `{"background": "BACKGROUND_TRANSPARENT"}`). The SPA's grid components read
   `component.meta.background` while rendering; a `meta`-less node crashes
   with *Cannot read properties of undefined (reading 'background')*.
3. **DuckDB concurrency.** DuckDB permits exactly one read/write process.
   With `gunicorn -w 2`, a worker opened read-write blocks the other and chart
   queries fail with *Could not set lock on file*. The provisioning script
   sets `access_mode=READ_ONLY` on the Superset database connection so all
   workers can read concurrently. Corollary: stop Superset (or accept the
   failure) when rebuilding `dealer.duckdb` with dbt while Superset runs.

## Security note

The script defaults to the local demo credentials (`admin` /
`RidgelineDemo2026!`). Override via `SUPERSET_USER` / `SUPERSET_PASSWORD`
environment variables and rotate them before exposing Superset beyond
localhost.
