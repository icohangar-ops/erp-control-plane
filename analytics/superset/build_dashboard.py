#!/usr/bin/env python3
"""Provision the Ridgeline dealer KPI dashboard in Superset via the REST API.

Idempotent: every object (database, datasets, charts, dashboard) is looked up
first and updated in place when present. Data sources are the dbt-built DuckDB
marts (fictional Ridgeline seed data).

Runbook
-------
    # 1. build the marts (dbt writes dealer.duckdb)
    # 2. start Superset (single metadata DB, gunicorn -w 2 is fine)
    # 3. run this script:
    python build_dashboard.py

Two Superset-specific constraints this script encodes the hard way:

1. The dashboard layout (positions) MUST be nested under "positions" inside
   json_metadata — DashboardDAO.set_dash_metadata syncs dashboard.slices and
   writes the canonical position_json column from that key.
2. Every container node in the v2 layout (ROW, COLUMN) MUST carry a meta
   object, at minimum {"background": "BACKGROUND_TRANSPARENT"}. The SPA's
   grid components read component.meta.background while rendering
   (Column.jsx / Row.jsx / DynamicComponent.tsx) and crash with
   "Cannot read properties of undefined (reading 'background')" otherwise.
3. The DuckDB connection opens with access_mode=READ_ONLY so that multiple
   gunicorn workers can share the file — DuckDB allows only one read/write
   process, and a second worker otherwise fails chart queries with
   "Could not set lock on file ... Conflicting lock".

Environment overrides:
    SUPERSET_URL          default http://localhost:8088
    SUPERSET_USER         default admin
    SUPERSET_PASSWORD     default RidgelineDemo2026!
    DEALER_DUCKDB         default /home/user/superset-bi/dealer.duckdb
    SUPERSET_META_DB      default /home/user/superset-bi/superset-meta.db
"""
import json
import os
import sqlite3
import sys
import uuid as uuid_lib

import requests

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088")
DB_NAME = "Ridgeline Dealer Analytics (DuckDB)"
DEALER_DUCKDB = os.environ.get("DEALER_DUCKDB", "/home/user/superset-bi/dealer.duckdb")
SUPERSET_META_DB = os.environ.get("SUPERSET_META_DB", "/home/user/superset-bi/superset-meta.db")
# READ_ONLY lets every gunicorn worker open the file concurrently.
DB_URI = f"duckdb:///{DEALER_DUCKDB}?access_mode=READ_ONLY"
DASH_TITLE = "Ridgeline Lumber & Supply — Dealer KPI Overview"
DASH_SLUG = "ridgeline-dealer-kpis"

session = requests.Session()


def login():
    r = session.post(
        f"{BASE}/api/v1/security/login",
        json={
            "username": os.environ.get("SUPERSET_USER", "admin"),
            "password": os.environ.get("SUPERSET_PASSWORD", "RidgelineDemo2026!"),
            "provider": "db",
            "refresh": True,
        },
        timeout=30,
    )
    r.raise_for_status()
    token = r.json()["access_token"]
    csrf = session.get(
        f"{BASE}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    ).json()["result"]
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-CSRF-Token": csrf,
        "Referer": BASE,
    }


def api(method, path, **kw):
    r = session.request(method, BASE + path, headers=HEADERS, timeout=120, **kw)
    if not r.ok:
        print(f"FAIL {method} {path} -> {r.status_code}: {r.text[:600]}")
        sys.exit(1)
    body = r.text.strip()
    return json.loads(body) if body else {}


def find_one(path, col, value):
    q = json.dumps({"filters": [{"col": col, "opr": "eq", "value": value}]})
    res = api("GET", path, params={"q": q})
    items = res.get("result", [])
    return items[0] if items else None


# ---------------------------------------------------------------- database
def ensure_database():
    canonical = {
        "database_name": DB_NAME,
        "sqlalchemy_uri": DB_URI,
        "engine_parameters": {},
    }
    existing = find_one("/api/v1/database/", "database_name", DB_NAME)
    if existing:
        # Self-heal: older installs were created without access_mode=READ_ONLY
        # and then hit gunicorn-worker lock conflicts on every other request.
        if "access_mode=READ_ONLY" not in existing.get("sqlalchemy_uri", ""):
            api("PUT", f"/api/v1/database/{existing['id']}", json=canonical)
            print(f"database updated (read-only mode) id={existing['id']}")
        else:
            print(f"database exists id={existing['id']}")
        return existing["id"]
    payload = {
        **canonical,
        "allow_run_async": False,
        "allow_ctas": False,
        "expose_in_sqllab": False,
        "impersonate_user": False,
    }
    res = api("POST", "/api/v1/database/", json=payload)
    db_id = res["id"]
    print(f"database created id={db_id}")
    return db_id


# ---------------------------------------------------------------- datasets
INVOICE_BRANCH_SQL = """
select l.invoice_date, d.branch_name, d.region,
       l.revenue_amount, l.gross_margin_amount, l.invoiced_qty
from main_canonical.fact_invoice_line l
join main_canonical.dim_location d on l.location_key = d.location_key
""".strip()

INVOICE_CAT_SQL = """
select c.category, l.invoice_date,
       l.revenue_amount, l.gross_margin_amount
from main_canonical.fact_invoice_line l
join main_canonical.dim_item i on l.item_key = i.item_key
join main_canonical.dim_item_category c on i.category_key = c.category_key
""".strip()

INVENTORY_CAT_SQL = """
select category, inventory_value, backorder_qty
from (
    select c.category, s.inventory_value, s.backorder_qty,
           row_number() over (partition by s.item_key, s.location_key
                              order by s.snapshot_date desc) as rn
    from main_canonical.fact_inventory_snapshot s
    join main_canonical.dim_item i on s.item_key = i.item_key
    join main_canonical.dim_item_category c on i.category_key = c.category_key
) t
where rn = 1
""".strip()

SO_BRANCH_SQL = """
select d.branch_name, l.order_date, l.ordered_qty, l.filled_qty, l.is_backordered
from main_canonical.fact_sales_order_line l
join main_canonical.dim_location d on l.location_key = d.location_key
""".strip()

PO_VENDOR_SQL = """
select v.vendor_name, l.ordered_qty, l.received_qty, l.purchase_price_variance
from main_canonical.fact_purchase_order_line l
join main_canonical.dim_vendor v on l.vendor_key = v.vendor_key
""".strip()

KPI_LONG_SQL = """
select 1 seq, 'GMROI' kpi, cast(gmroi as double) as "value" from main_marts.kpi_headline
union all select 2, 'Inventory turns', inventory_turns from main_marts.kpi_headline
union all select 3, 'Days of inventory (days)', dio_days from main_marts.kpi_headline
union all select 4, 'Weeks of supply', weeks_of_supply from main_marts.kpi_headline
union all select 5, 'Gross margin %', gross_margin_pct from main_marts.kpi_headline
union all select 6, 'Line fill rate %', line_fill_rate from main_marts.kpi_headline
union all select 7, 'Order fill rate %', order_fill_rate from main_marts.kpi_headline
union all select 8, 'On-time delivery %', otd_pct from main_marts.kpi_headline
union all select 9, 'OTIF %', otif_pct from main_marts.kpi_headline
union all select 10, 'Backorder rate %', backorder_rate from main_marts.kpi_headline
union all select 11, 'Vendor fill rate %', vendor_fill_rate from main_marts.kpi_headline
union all select 12, 'Purchase price variance %', ppv_pct from main_marts.kpi_headline
union all select 13, 'DSO (days)', dso_days from main_marts.kpi_headline
union all select 14, 'DPO (days)', dpo_days from main_marts.kpi_headline
union all select 15, 'Cash conversion cycle (days)', ccc_days from main_marts.kpi_headline
union all select 16, 'Close cycle (days)', close_cycle_days from main_marts.kpi_headline
union all select 17, 'Same-branch revenue %', same_branch_revenue_pct from main_marts.kpi_headline
union all select 18, 'Organic revenue %', organic_revenue_pct from main_marts.kpi_headline
union all select 19, 'Acquired revenue %', acquired_revenue_pct from main_marts.kpi_headline
union all select 20, 'Sales per FTE (annualized)', sales_per_fte_annualized from main_marts.kpi_headline
union all select 21, 'Average ticket', avg_ticket from main_marts.kpi_headline
""".strip()

VIRTUAL_DATASETS = {
    "invoice_branch": INVOICE_BRANCH_SQL,
    "invoice_cat": INVOICE_CAT_SQL,
    "inventory_cat": INVENTORY_CAT_SQL,
    "so_branch": SO_BRANCH_SQL,
    "po_vendor": PO_VENDOR_SQL,
    "kpi_long": KPI_LONG_SQL,
}


def ensure_dataset(table_name, sql=None, schema=None):
    existing = find_one("/api/v1/dataset/", "table_name", table_name)
    if existing:
        print(f"dataset exists id={existing['id']} ({table_name})")
        return existing["id"]
    payload = {"database": DB_ID, "table_name": table_name}
    if sql:
        payload["sql"] = sql
    if schema:
        payload["schema"] = schema
    res = api("POST", "/api/v1/dataset/", json=payload)
    print(f"dataset created id={res['id']} ({table_name})")
    return res["id"]


# ---------------------------------------------------------------- charts
def simple(col, agg, label):
    return {"expressionType": "SIMPLE", "column": {"column_name": col}, "aggregate": agg, "label": label}


def sql_expr(expr, label):
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}


def tile_params(ds_id, metric, fmt):
    return {
        "datasource": f"{ds_id}__table",
        "viz_type": "big_number_total",
        "metrics": [metric],
        "groupby": [],
        "time_range": "No filter",
        "y_axis_format": fmt,
        "subheader": "trailing 90-day window",
    }


def line_params(ds_id, time_col, metric, fmt="SMART_NUMBER"):
    return {
        "datasource": f"{ds_id}__table",
        "viz_type": "echarts_timeseries_line",
        "x_axis": time_col,
        "granularity_sqla": time_col,
        "time_grain_sqla": "P1M",
        "metrics": [metric],
        "groupby": [],
        "time_range": "No filter",
        "y_axis_format": fmt,
        "row_limit": 100,
    }


def bar_params(ds_id, x_col, metric, fmt="SMART_NUMBER", limit=50):
    return {
        "datasource": f"{ds_id}__table",
        "viz_type": "echarts_timeseries_bar",
        "x_axis": x_col,
        "metrics": [metric],
        "groupby": [],
        "time_range": "No filter",
        "y_axis_format": fmt,
        "order_desc": True,
        "row_limit": limit,
    }


CHARTS = [
    ("GMROI", "big_number_total", "kpi_headline", tile_params(0, simple("gmroi", "AVG", "GMROI"), ".2f")),
    ("Inventory turns", "big_number_total", "kpi_headline", tile_params(0, simple("inventory_turns", "AVG", "Inventory turns"), ".2f")),
    ("Gross margin %", "big_number_total", "kpi_headline", tile_params(0, simple("gross_margin_pct", "AVG", "Gross margin %"), ".1f")),
    ("Line fill rate %", "big_number_total", "kpi_headline", tile_params(0, simple("line_fill_rate", "AVG", "Line fill rate %"), ".1f")),
    ("Monthly invoiced revenue", "echarts_timeseries_line", "fact_invoice_line",
     line_params(0, "invoice_date", simple("revenue_amount", "SUM", "Revenue"))),
    ("Revenue by branch", "echarts_timeseries_bar", "invoice_branch",
     bar_params(0, "branch_name", simple("revenue_amount", "SUM", "Revenue"))),
    ("Gross margin % by category", "echarts_timeseries_bar", "invoice_cat",
     bar_params(0, "category", sql_expr("SUM(gross_margin_amount) / NULLIF(SUM(revenue_amount), 0) * 100", "Gross margin %"), ".1f")),
    ("Inventory value by category", "echarts_timeseries_bar", "inventory_cat",
     bar_params(0, "category", simple("inventory_value", "SUM", "Inventory value"))),
    ("Open-order fill % by branch", "echarts_timeseries_bar", "so_branch",
     bar_params(0, "branch_name", sql_expr("SUM(filled_qty) / NULLIF(SUM(ordered_qty), 0) * 100", "Fill %"), ".1f")),
    ("Vendor fill rate %", "echarts_timeseries_bar", "po_vendor",
     bar_params(0, "vendor_name", sql_expr("SUM(received_qty) / NULLIF(SUM(ordered_qty), 0) * 100", "Vendor fill %"), ".1f", limit=8)),
    ("All headline KPIs", "table", "kpi_long",
     {"datasource": "0__table", "viz_type": "table", "query_mode": "raw",
      "all_columns": ["kpi", "value"], "time_range": "No filter", "row_limit": 100}),
]


def ensure_charts(ds_map):
    ids = {}
    for name, viz, ds_key, params in CHARTS:
        ds_id = ds_map[ds_key]
        params["datasource"] = f"{ds_id}__table"
        existing = find_one("/api/v1/chart/", "slice_name", name)
        payload = {
            "slice_name": name,
            "viz_type": viz,
            "datasource_id": ds_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        }
        if existing:
            api("PUT", f"/api/v1/chart/{existing['id']}", json=payload)
            ids[name] = existing["id"]
            print(f"chart updated id={existing['id']} ({name})")
        else:
            res = api("POST", "/api/v1/chart/", json=payload)
            ids[name] = res["id"]
            print(f"chart created id={res['id']} ({name})")
    return ids


# ---------------------------------------------------------------- dashboard
def build_layout(ids, uuids):
    """Layout in the exact shape the Superset SPA saves.

    Every container node MUST carry meta (at least meta.background) — the SPA's
    grid components read component.meta.background while rendering
    (Column.jsx/Row.jsx/DynamicComponent.tsx) and crash with
    "Cannot read properties of undefined (reading 'background')" otherwise.
    Charts are wrapped in COLUMN nodes, as the SPA itself saves them.
    """
    BACKGROUND = "BACKGROUND_TRANSPARENT"

    def chart_node(chart_id, parents):
        return {
            "type": "CHART",
            "id": f"CHART-{chart_id}",
            "chartId": chart_id,
            "parents": parents,
            "meta": {
                "chartId": chart_id,
                "uuid": uuids[chart_id],
                "width": 4,
                "height": 50,
            },
        }

    rows = [
        ("ROW-1", [("GMROI", 3), ("Inventory turns", 3), ("Gross margin %", 3), ("Line fill rate %", 3)]),
        ("ROW-2", [("Monthly invoiced revenue", 7), ("Revenue by branch", 5)]),
        ("ROW-3", [("Gross margin % by category", 6), ("Inventory value by category", 6)]),
        ("ROW-4", [("Open-order fill % by branch", 6), ("Vendor fill rate %", 6)]),
        ("ROW-5", [("All headline KPIs", 12)]),
    ]
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"], "children": []},
    }
    for row_id, charts in rows:
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": [],
            "meta": {"background": BACKGROUND},
        }
        layout["GRID_ID"]["children"].append(row_id)
        for name, width in charts:
            cid = ids[name]
            col_id = f"COLUMN-{row_id.split('-')[1]}-{cid}"
            layout[col_id] = {
                "type": "COLUMN",
                "id": col_id,
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "children": [f"CHART-{cid}"],
                "meta": {"background": BACKGROUND, "width": width},
            }
            layout[row_id]["children"].append(col_id)
            layout[f"CHART-{cid}"] = chart_node(cid, ["ROOT_ID", "GRID_ID", row_id, col_id])
    return layout


def chart_uuids(ids):
    """Chart uuids come from the metadata DB — the REST API omits them in this
    deployment's response shapes; sqlite is the authoritative source."""
    conn = sqlite3.connect(SUPERSET_META_DB)
    raw = {row[0]: row[1] for row in conn.execute("SELECT id, uuid FROM slices")}
    conn.close()
    uuids = {}
    for cid in ids.values():
        val = raw[cid]
        if isinstance(val, bytes):  # stored as BLOB(16) in sqlite
            val = str(uuid_lib.UUID(bytes=val))
        uuids[cid] = val
    return uuids


def ensure_dashboard(ids):
    uuids = chart_uuids(ids)
    layout = build_layout(ids, uuids)
    # The layout must be nested under "positions" INSIDE json_metadata:
    # DashboardDAO.set_dash_metadata syncs dashboard.slices and writes the
    # canonical position_json column from this key. A top-level position_json
    # alone leaves the dashboard with no charts attached and crashes the SPA.
    json_metadata = {
        "refresh_frequency": 0,
        "default_filters": "{}",
        "native_filter_configuration": [],
        "chart_configuration": {},
        "color_scheme_domain": [],
        "label_colors": {},
        "shared_label_colors": {},
        "expanded_slices": {},
        "cross_filters_enabled": True,
        "positions": layout,
    }
    layout_payload = {"json_metadata": json.dumps(json_metadata), "published": True}
    existing = find_one("/api/v1/dashboard/", "slug", DASH_SLUG)
    if existing:
        dash_id = existing["id"]
        api("PUT", f"/api/v1/dashboard/{dash_id}", json=layout_payload)
        print(f"dashboard updated id={dash_id}")
    else:
        res = api("POST", "/api/v1/dashboard/", json={
            "dashboard_title": DASH_TITLE, "slug": DASH_SLUG, "published": True,
        })
        dash_id = res["id"]
        api("PUT", f"/api/v1/dashboard/{dash_id}", json=layout_payload)
        print(f"dashboard created id={dash_id}")
    return dash_id


# ---------------------------------------------------------------- verify
def verify_charts(ids, ds_map):
    """Execute every chart's query via the chart-data endpoint; this catches
    DuckDB lock/permission problems before a demo does."""
    failures = 0
    for name, _viz, ds_key, params in CHARTS:
        ds_id = ds_map[ds_key]
        if params.get("query_mode") == "raw":
            query = {"columns": params["all_columns"], "row_limit": params.get("row_limit", 100)}
        else:
            query = {
                "metrics": params["metrics"],
                "groupby": params.get("groupby", []),
                "time_range": params.get("time_range", "No filter"),
                "filters": [],
                "row_limit": params.get("row_limit", 100),
            }
            if "granularity_sqla" in params:
                query["granularity"] = params["granularity_sqla"]
                query["time_grain"] = params.get("time_grain_sqla", "P1D")
        res = api("POST", "/api/v1/chart/data", json={
            "datasource": {"id": ds_id, "type": "table"},
            "queries": [query],
            "result_format": "json",
            "result_type": "full",
        })
        status = res["result"][0].get("status")
        rows = res["result"][0].get("data") or []
        ok = status == "success" and rows
        failures += 0 if ok else 1
        sample = json.dumps(rows[0])[:120] if rows else "NO ROWS"
        print(f"{'OK  ' if ok else 'FAIL'} chart '{name}' status={status} rows={len(rows)} sample={sample}")
    if failures:
        sys.exit(f"{failures} chart queries failed")


def main():
    global HEADERS, DB_ID
    HEADERS = login()
    DB_ID = ensure_database()
    ds_map = {"kpi_headline": ensure_dataset("kpi_headline", schema="main_marts"),
              "fact_invoice_line": ensure_dataset("fact_invoice_line", schema="main_canonical")}
    for name, sql in VIRTUAL_DATASETS.items():
        ds_map[name] = ensure_dataset(name, sql=sql)
    ids = ensure_charts(ds_map)
    dash_id = ensure_dashboard(ids)
    verify_charts(ids, ds_map)
    print(json.dumps({"dashboard_id": dash_id, "dashboard_url": f"{BASE}/superset/dashboard/{DASH_SLUG}/"}, indent=2))


if __name__ == "__main__":
    main()
