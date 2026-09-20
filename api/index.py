"""Read-only demo API for the construction-supplies ERP control plane.

Serves the seeded Ridgeline Lumber & Supply dealer export (the same CSV
source data the offline ``make demo`` pipeline ingests) as JSON:

- ``GET /``             -- HTML landing page for browsers, service-info JSON otherwise
- ``GET /health``       -- liveness + data provenance
- ``GET /data/summary`` -- per-domain row counts, date spans, headcount
- ``GET /kpis``         -- the full 21-metric headline KPI set
- ``POST /api/v1/genbi/answers/promote``  -- persist an NL answer as a governed
  Superset dataset + chart (GenBI extension spec §4.2)
- ``GET/POST /api/v1/genbi/coverage-requests`` -- the "not modeled yet" queue
- ``GET /api/v1/genbi/audit``             -- question -> SQL -> latency -> outcome
- ``POST /api/v1/genbi/data-room/search`` -- fail-closed dual-stage authorized retrieval
  over the acquisition data rooms (Qdrant)
- ``GET /api/v1/genbi/health/protocols``  -- protocol-level WrenAI/Qdrant health with
  tool-schema drift alarms

KPIs are computed with DuckDB using the same definitions as the dbt marts
in ``dbt/models/marts/`` (kpi_window, kpi_inventory, kpi_service,
kpi_finance, kpi_growth, kpi_purchasing, kpi_productivity) applied to
fact-equivalent views over the in-repo seed CSVs.
``tests/test_kpi_api.py`` pins every value to the dbt-built
``main_marts.kpi_headline`` output, so drift between this mirror and the
authoritative pipeline fails CI.

The heavy pipeline (Dagster, dbt, Superset, Postgres) is intentionally not
deployed serverless -- see docs/ARCHITECTURE.md for the container topology.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from string import Template
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

from api.genbi.routes import router as genbi_router
from control_plane.badges import (
    MOCK_KPI_MARKER,
    ProvenanceBadge,
    assert_real,
    resolve_badge,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEALER_EXPORT_DIR = REPO_ROOT / "seed" / "dealer_export"
DBT_SEEDS_DIR = REPO_ROOT / "dbt" / "seeds"

SOURCE_SYSTEM = "csvsftp_ridgeline"
SEED_DEALER = "Ridgeline Lumber & Supply"

# dealer export CSV file stem -> view name
DEALER_CSV_VIEWS: dict[str, str] = {
    "customers": "customers",
    "gl_entries": "gl_entries",
    "inventory_snapshots": "inventory_snapshots",
    "invoice_lines": "invoice_lines",
    "items": "items",
    "purchase_order_lines": "purchase_order_lines",
    "sales_order_lines": "sales_order_lines",
    "salespeople": "salespeople",
    "vendors": "vendors",
}

# dbt seed CSV file stem -> view name
DBT_SEED_VIEWS: dict[str, str] = {
    "seed_branches": "seed_branches",
    "seed_close_calendar": "seed_close_calendar",
    "seed_coa_mapping": "seed_coa_mapping",
}

# Fact-equivalent views mirroring dbt/models/canonical/*.sql for the models
# the KPI marts consume. Surrogate-key and crosswalk plumbing is skipped
# (single demo source; keys unused by the KPI SQL); measure and flag logic
# is copied 1:1 from the model files.
FACT_VIEW_SQL: list[str] = [
    # fact_invoice_line
    """
    create view fact_invoice_line as
    select
        invoice_no,
        cast(invoice_date as date) as invoice_date,
        order_no,
        customer_no,
        branch_code,
        item_no,
        invoiced_qty,
        unit_price,
        unit_cost,
        invoiced_qty * unit_price as revenue_amount,
        invoiced_qty * unit_cost as cogs_amount,
        (invoiced_qty * unit_price) - (invoiced_qty * unit_cost) as gross_margin_amount
    from invoice_lines
    """,
    # fact_sales_order_line
    """
    create view fact_sales_order_line as
    select
        order_no,
        line_no,
        customer_no,
        branch_code,
        item_no,
        cast(promised_date as date) as promised_date,
        cast(shipped_date as date) as shipped_date,
        order_status,
        ordered_qty,
        filled_qty,
        cancelled_qty,
        shipped_date is not null
            and cast(shipped_date as date) <= cast(promised_date as date) as is_on_time,
        filled_qty >= ordered_qty as is_line_complete,
        ordered_qty > 0
            and cancelled_qty < ordered_qty
            and filled_qty < ordered_qty as is_backordered
    from sales_order_lines
    """,
    # fact_inventory_snapshot
    """
    create view fact_inventory_snapshot as
    select
        cast(snapshot_date as date) as snapshot_date,
        branch_code,
        item_no,
        on_hand_qty,
        inventory_value
    from inventory_snapshots
    """,
    # ref_coa_mapping
    """
    create view ref_coa_mapping as
    select
        source_company_id,
        source_gl_account,
        target_consolidated_account,
        cast(effective_start_date as date) as effective_start_date,
        cast(effective_end_date as date) as effective_end_date,
        mapping_rule_type,
        cast(active_flag as boolean) as active_flag
    from seed_coa_mapping
    """,
    # fact_gl_transaction
    """
    create view fact_gl_transaction as
    select
        g.journal_no,
        g.line_no,
        cast(g.entry_date as date) as entry_date,
        g.branch_code,
        g.account as source_gl_account,
        coa.target_consolidated_account as consolidated_account,
        g.debit_amt,
        g.credit_amt,
        g.debit_amt - g.credit_amt as net_amount
    from gl_entries g
    left join ref_coa_mapping coa
        on coa.source_company_id = 'ridgeline_lumber'
       and coa.source_gl_account = g.account
       and coa.active_flag
       and cast(g.entry_date as date) >= coa.effective_start_date
       and cast(g.entry_date as date) <= coa.effective_end_date
    """,
    # fact_purchase_order_line
    """
    create view fact_purchase_order_line as
    select
        po_no,
        line_no,
        cast(po_date as date) as po_date,
        vendor_no,
        branch_code,
        item_no,
        ordered_qty,
        received_qty,
        unit_cost_actual,
        unit_cost_standard,
        (unit_cost_actual - unit_cost_standard) * received_qty as purchase_price_variance
    from purchase_order_lines
    """,
    # dim_location
    """
    create view dim_location as
    select
        branch_code,
        branch_name,
        region,
        cast(owned_since as date) as owned_since,
        fte_count
    from seed_branches
    """,
]

# kpi_window.sql -- the analysis window is derived from the invoiced data
# itself, so KPI annualization reproduces from any window of data.
WINDOW_SQL = """
select
    min(invoice_date) as window_start,
    max(invoice_date) as window_end,
    date_diff('day', min(invoice_date), max(invoice_date)) + 1 as window_days,
    365.0 / (date_diff('day', min(invoice_date), max(invoice_date)) + 1) as annualization_factor
from fact_invoice_line
"""

# kpi_inventory.sql -- turns / DIO / weeks of supply / GMROI, annualized to
# the derived window; avg inventory value is the mean of snapshot-date totals.
INVENTORY_SQL = f"""
with w as ({WINDOW_SQL}),
cogs as (
    select sum(cogs_amount) as cogs_window,
           sum(gross_margin_amount) as gm_window
    from fact_invoice_line
),
avg_inv as (
    select avg(month_value) as avg_inventory_value
    from (
        select snapshot_date, sum(inventory_value) as month_value
        from fact_inventory_snapshot
        group by 1
    )
)
select
    round(c.cogs_window * w.annualization_factor / a.avg_inventory_value, 2) as inventory_turns,
    round(365.0 / (c.cogs_window * w.annualization_factor / a.avg_inventory_value), 1) as dio_days,
    round(a.avg_inventory_value / (c.cogs_window / w.window_days * 7), 1) as weeks_of_supply,
    round(c.gm_window * w.annualization_factor / a.avg_inventory_value, 2) as gmroi
from w, cogs c, avg_inv a
"""

# kpi_finance.sql -- balances from the mapped GL (GRP-1100 AR, GRP-2000 AP).
FINANCE_SQL = f"""
with invoice_totals as (
    select
        sum(revenue_amount) as revenue_window,
        sum(cogs_amount) as cogs_window,
        sum(gross_margin_amount) as gm_window
    from fact_invoice_line
),
balances as (
    select
        sum(case when consolidated_account = 'GRP-1100' then net_amount end) as ar_balance,
        sum(case when consolidated_account = 'GRP-2000' then net_amount end) as ap_balance
    from fact_gl_transaction
),
close_cycle as (
    select avg(date_diff('day', cast(period_end_date as date), cast(close_completed_on as date))) as close_cycle_days
    from seed_close_calendar
),
inv as ({INVENTORY_SQL})
select
    round(i.gm_window / nullif(i.revenue_window, 0), 4) as gross_margin_pct,
    round(b.ar_balance / nullif(i.revenue_window, 0) * 365, 1) as dso_days,
    round(abs(b.ap_balance) / nullif(i.cogs_window, 0) * 365, 1) as dpo_days,
    round(
        inv.dio_days
        + b.ar_balance / nullif(i.revenue_window, 0) * 365
        - abs(b.ap_balance) / nullif(i.cogs_window, 0) * 365,
        1
    ) as ccc_days,
    round(c.close_cycle_days, 1) as close_cycle_days
from invoice_totals i
cross join balances b
cross join close_cycle c
cross join inv
"""

# kpi_service.sql -- line-grain metrics from active lines; order-grain
# metrics from the worst line on each order.
SERVICE_SQL = """
with lines as (
    select *
    from fact_sales_order_line
    where ordered_qty > 0
      and cancelled_qty < ordered_qty
),
line_metrics as (
    select
        round(
            sum(least(filled_qty, ordered_qty)) / nullif(sum(ordered_qty), 0),
            4
        ) as line_fill_rate,
        round(sum(case when is_backordered then 1 else 0 end) / count(*), 4) as backorder_rate,
        count(*) as active_line_count
    from lines
),
orders as (
    select
        order_no,
        min(cast(is_line_complete as integer)) as all_lines_complete,
        min(cast(is_on_time as integer)) as on_time_all_lines,
        bool_and(shipped_date is not null) as fully_shipped
    from lines
    group by order_no
),
order_metrics as (
    select
        count(*) as order_count,
        round(sum(all_lines_complete) / count(*), 4) as order_fill_rate,
        round(
            sum(case when fully_shipped and on_time_all_lines = 1 then 1 else 0 end)
            / nullif(sum(case when fully_shipped then 1 else 0 end), 0),
            4
        ) as otd_pct,
        round(
            sum(case when on_time_all_lines = 1 and all_lines_complete = 1 then 1 else 0 end)
            / count(*),
            4
        ) as otif_pct
    from orders
)
select
    l.line_fill_rate,
    l.backorder_rate,
    l.active_line_count,
    o.order_count,
    o.order_fill_rate,
    o.otd_pct,
    o.otif_pct
from line_metrics l
cross join order_metrics o
"""

# kpi_growth.sql -- same-branch revenue uses the customer's modal branch as
# the inferred home branch; organic = branch owned before the window start.
GROWTH_SQL = f"""
with w as ({WINDOW_SQL}),
customer_branch_revenue as (
    select customer_no, branch_code, sum(revenue_amount) as revenue_amount
    from fact_invoice_line
    group by 1, 2
),
customer_home_branch as (
    select customer_no, branch_code as home_branch
    from (
        select
            customer_no,
            branch_code,
            row_number() over (partition by customer_no order by revenue_amount desc) as rn
        from customer_branch_revenue
    )
    where rn = 1
),
invoice_joined as (
    select
        f.revenue_amount,
        f.branch_code = h.home_branch as is_same_branch,
        coalesce(loc.owned_since < (select window_start from w), false) as is_organic
    from fact_invoice_line f
    join customer_home_branch h on h.customer_no = f.customer_no
    left join dim_location loc on loc.branch_code = f.branch_code
)
select
    round(
        sum(case when is_same_branch then revenue_amount else 0 end)
        / nullif(sum(revenue_amount), 0),
        4
    ) as same_branch_revenue_pct,
    round(
        sum(case when is_organic then revenue_amount else 0 end)
        / nullif(sum(revenue_amount), 0),
        4
    ) as organic_revenue_pct,
    round(
        sum(case when not is_organic then revenue_amount else 0 end)
        / nullif(sum(revenue_amount), 0),
        4
    ) as acquired_revenue_pct
from invoice_joined
"""

# kpi_purchasing.sql -- vendor fill and purchase-price variance over the PO book.
PURCHASING_SQL = """
select
    round(sum(received_qty) / nullif(sum(ordered_qty), 0), 4) as vendor_fill_rate,
    round(sum(purchase_price_variance), 2) as ppv_total,
    round(
        sum(purchase_price_variance)
        / nullif(sum(unit_cost_standard * received_qty), 0),
        4
    ) as ppv_pct,
    count(*) as po_line_count
from fact_purchase_order_line
"""

# kpi_productivity.sql -- annualized revenue per FTE and average ticket.
PRODUCTIVITY_SQL = f"""
with w as ({WINDOW_SQL}),
invoice_totals as (
    select
        sum(revenue_amount) as revenue_window,
        count(distinct invoice_no) as invoice_count,
        count(distinct invoice_date) as active_days
    from fact_invoice_line
),
headcount as (
    select sum(fte_count) as total_fte from dim_location
)
select
    round(
        i.revenue_window * (select annualization_factor from w)
        / nullif(h.total_fte, 0),
        0
    ) as sales_per_fte_annualized,
    round(i.revenue_window / nullif(i.invoice_count, 0), 2) as avg_ticket
from invoice_totals i
cross join headcount h
"""

KPI_QUERIES: dict[str, str] = {
    "window": WINDOW_SQL,
    "inventory": INVENTORY_SQL,
    "finance": FINANCE_SQL,
    "service": SERVICE_SQL,
    "growth": GROWTH_SQL,
    "purchasing": PURCHASING_SQL,
    "productivity": PRODUCTIVITY_SQL,
}

_connection: duckdb.DuckDBPyConnection | None = None
_connection_lock = threading.Lock()


def build_connection() -> duckdb.DuckDBPyConnection:
    """Build an in-memory DuckDB with seed views + fact-equivalent views.

    Mirrors the demo pipeline's source layer: every view is derived from the
    CSVs committed under seed/dealer_export/ and dbt/seeds/.
    """
    con = duckdb.connect(":memory:")
    for stem, view in DEALER_CSV_VIEWS.items():
        path = DEALER_EXPORT_DIR / f"{stem}.csv"
        con.execute(f"create view {view} as select * from read_csv_auto('{path}')")
    for stem, view in DBT_SEED_VIEWS.items():
        path = DBT_SEEDS_DIR / f"{stem}.csv"
        con.execute(f"create view {view} as select * from read_csv_auto('{path}')")
    for statement in FACT_VIEW_SQL:
        con.execute(statement)
    return con


def get_connection() -> duckdb.DuckDBPyConnection:
    """Lazily build and cache the demo DuckDB connection (thread-safe)."""
    global _connection
    if _connection is None:
        with _connection_lock:
            if _connection is None:
                _connection = build_connection()
    return _connection


def _jsonable(value: Any) -> Any:
    """Convert DuckDB scalars (dates, decimals) to JSON-safe Python types."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "to_float"):
        return float(value)
    return value


def _rows_to_dicts(cur: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    names = [d[0] for d in cur.description]
    row = cur.fetchone()
    return {name: _jsonable(value) for name, value in zip(names, row or [], strict=False)}


def compute_kpis(con: duckdb.DuckDBPyConnection | None = None) -> dict[str, Any]:
    """Compute the full headline KPI set with the dbt mart definitions."""
    con = con or get_connection()
    kpis: dict[str, Any] = {}
    for _name, query in KPI_QUERIES.items():
        result = _rows_to_dicts(con.execute(query))
        for key, value in result.items():
            if key == "annualization_factor":
                continue  # internal to the window model, not a published KPI
            kpis[key] = value
    kpis["provenance"] = (
        "Computed at request time from the in-repo seed CSVs using the dbt "
        "mart definitions (dbt/models/marts/). Pinned to the dbt-built "
        "main_marts.kpi_headline values by tests/test_kpi_api.py."
    )
    return kpis


# --- honest KPI serving: LIVE -> CACHE, with provenance badges ---------------

# Served KPI sets are cached so a failed data layer degrades honestly to a
# labeled CACHE badge instead of an outage — never to a fabricated number.
KPI_CACHE_TTL_SECONDS = 300
_kpi_cache: dict[str, Any] | None = None
_kpi_cache_at = 0.0
_kpi_cache_lock = threading.Lock()


def _resolve_kpis() -> tuple[dict[str, Any], ProvenanceBadge]:
    """Resolve the KPI set with its provenance badge (LIVE -> CACHE -> fail).

    Fail-closed honesty: a value that cannot be certified (None) downgrades
    the badge to MOCK, and a data-layer failure with no cache raises — the
    surface never fabricates a number to fill a KPI slot.
    """
    global _kpi_cache, _kpi_cache_at
    try:
        kpis = compute_kpis()
    except Exception:
        with _kpi_cache_lock:
            cached, cached_at = _kpi_cache, _kpi_cache_at
        if cached is None:
            raise  # nothing ever computed — refuse rather than mock
        age = int(time.time() - cached_at)
        return cached, resolve_badge(
            cached=True,
            detail=f"live computation failed; serving the last real KPI set ({age}s old)",
        )
    null_keys = sorted(key for key, value in kpis.items() if key != "provenance" and value is None)
    if null_keys:
        # An uncertifiable value is MOCK and must never enter the cache — a
        # later outage would otherwise serve it as a trusted CACHE fallback.
        return kpis, resolve_badge(
            mock=True, detail=f"KPI value(s) {', '.join(null_keys)} could not be certified"
        )
    with _kpi_cache_lock:
        _kpi_cache = kpis
        _kpi_cache_at = time.time()
    return kpis, resolve_badge(
        live=True,
        detail="computed at request time from the seed CSVs (dbt mart definitions)",
    )


def _badge_payload(badge: ProvenanceBadge) -> dict[str, Any]:
    """The JSON badge attached to every served KPI surface."""
    return {
        "tier": badge.tier,
        "marker": badge.marker,
        "detail": badge.detail,
        "is_real": badge.is_real,
    }


def summarize_data(con: duckdb.DuckDBPyConnection | None = None) -> dict[str, Any]:
    """Row counts, date spans, and headcount for every seed domain."""
    con = con or get_connection()
    domains = {
        view: con.execute(f"select count(*) from {view}").fetchone()[0]
        for view in DEALER_CSV_VIEWS.values()
    }
    spans: dict[str, dict[str, Any]] = {}
    for name, view, column in [
        ("sales_orders", "sales_order_lines", "order_date"),
        ("invoices", "invoice_lines", "invoice_date"),
        ("purchase_orders", "purchase_order_lines", "po_date"),
        ("gl_entries", "gl_entries", "entry_date"),
    ]:
        row = con.execute(
            f"select min(cast({column} as date)), max(cast({column} as date)) from {view}"
        ).fetchone()
        spans[name] = {"start": _jsonable(row[0]), "end": _jsonable(row[1])}
    branch_row = con.execute("select count(*), sum(fte_count) from dim_location").fetchone()
    item_row = con.execute(
        "select count(*), sum(case when item_status = 'ACTIVE' then 1 else 0 end) from items"
    ).fetchone()
    return {
        "source_system": SOURCE_SYSTEM,
        "seed_dealer": SEED_DEALER,
        "total_rows": sum(domains.values()),
        "domains": domains,
        "date_spans": spans,
        "branches": {"count": branch_row[0], "total_fte": branch_row[1]},
        "items": {"count": item_row[0], "active": item_row[1]},
    }


app = FastAPI(
    title="Construction Supplies ERP Control Plane -- Demo API",
    version="0.1.0",
    description=(
        "Read-only demo surface over the seeded dealer dataset: data summary "
        "and the headline KPI set, mirroring the dbt marts of the "
        "control-plane repository."
    ),
)

app.include_router(genbi_router)


@app.get("/")
def root(request: Request) -> Any:
    """Service info: HTML landing page for browsers, JSON for API clients."""
    if "text/html" in request.headers.get("accept", "").lower():
        return HTMLResponse(_render_landing_page())
    return {
        "service": app.title,
        "version": app.version,
        "description": app.description,
        "endpoints": [
            "/health",
            "/data/summary",
            "/kpis",
            "/api/v1/genbi/answers/promote",
            "/api/v1/genbi/coverage-requests",
            "/api/v1/genbi/audit",
            "/api/v1/genbi/contracts",
            "/api/v1/genbi/approval-receipts",
            "/api/v1/genbi/data-room/search",
            "/api/v1/genbi/data-room/audit",
            "/api/v1/genbi/health/protocols",
        ],
        "source_repository": "construction-supplies-erp-control-plane",
    }


LANDING_PAGE_TEMPLATE = Template("""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ERP Control Plane — Demo API</title>
<style>
  * { box-sizing: border-box; margin: 0; }
  body {
    font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI",
      Roboto, "Helvetica Neue", Arial, sans-serif;
    background: #f6f7f5; color: #1c2430; min-height: 100vh;
    display: flex; align-items: center; justify-content: center; padding: 2rem;
  }
  main { max-width: 660px; width: 100%; }
  h1 { font-size: 1.3rem; letter-spacing: -0.01em; margin-bottom: 0.4rem; }
  p.lede { color: #5b6572; font-size: 0.95rem; line-height: 1.55;
           margin-bottom: 1.4rem; }
  .kpis { display: grid;
          grid-template-columns: repeat(auto-fit, minmax(112px, 1fr));
          gap: 0.6rem; margin-bottom: 1.5rem; }
  .kpi { background: #ffffff; border: 1px solid #e3e6e1;
         border-radius: 10px; padding: 0.7rem 0.85rem; }
  .kpi b { display: block; font-size: 1.2rem; margin-bottom: 0.15rem; }
  .kpi span { color: #5b6572; font-size: 0.7rem; text-transform: uppercase;
              letter-spacing: 0.05em; }
  ul.endpoints { list-style: none; padding: 0; }
  ul.endpoints li { margin: 0.5rem 0; color: #3d4653; }
  a { color: #2456c4; text-decoration: none; }
  a:hover { text-decoration: underline; }
  code { background: #eceee9; border-radius: 5px; padding: 0.1rem 0.35rem;
         font-size: 0.85em; }
  footer { margin-top: 1.6rem; color: #8a939e; font-size: 0.78rem;
           line-height: 1.5; }
</style>
</head>
<body>
<main>
  <h1>$heading</h1>
  <p class="lede">$lede</p>
  <div class="kpis">$kpi_tiles</div>
  <ul class="endpoints">
    <li><a href="/health"><code>/health</code></a> — liveness and data provenance</li>
    <li><a href="/data/summary"><code>/data/summary</code></a> — row counts, date spans, headcount</li>
    <li><a href="/kpis"><code>/kpis</code></a> — the full headline KPI set as JSON</li>
    <li><code>/api/v1/genbi/answers/promote</code> — save an NL answer as a governed Superset chart (<a href="/docs">docs</a>)</li>
    <li><code>/api/v1/genbi/coverage-requests</code> — the "not modeled yet" queue · <code>/api/v1/genbi/audit</code> — query audit trail</li>
  </ul>
  <footer>$footer</footer>
</main>
</body>
</html>
""")


def _kpi_tiles(kpis: dict[str, Any], badge: ProvenanceBadge) -> str:
    """Format the headline KPI tiles embedded in the landing page (badged)."""
    tiles = [
        ("GMROI", f"{kpis['gmroi']:.2f}"),
        ("Inv. turns", f"{kpis['inventory_turns']:.2f}"),
        ("Gross margin", f"{kpis['gross_margin_pct'] * 100:.1f}%"),
        ("Line fill", f"{kpis['line_fill_rate'] * 100:.1f}%"),
        ("Vendor fill", f"{kpis['vendor_fill_rate'] * 100:.1f}%"),
    ]
    rendered = []
    for name, value in tiles:
        # Tripwire: a MOCK-tier value must never render as a real KPI tile.
        assert_real(badge, context=f"landing KPI tile {name}")
        rendered.append(f'<div class="kpi"><b>{value}</b><span>{name} {badge.marker}</span></div>')
    return "".join(rendered)


def _render_landing_page() -> str:
    """Render the browser landing page with badged KPI values from the mart SQL."""
    try:
        kpis, badge = _resolve_kpis()
        tiles = _kpi_tiles(kpis, badge)
    except Exception:  # the page must render even if the data layer fails
        tiles = f'<div class="kpi"><b>—</b><span>{MOCK_KPI_MARKER} KPIs unavailable</span></div>'
    return LANDING_PAGE_TEMPLATE.substitute(
        heading="Construction Supplies ERP Control Plane",
        lede=(
            "Read-only demo API over the seeded Ridgeline Lumber &amp; Supply "
            "dealer dataset. Headline KPIs below are computed at request time "
            "with the same SQL definitions as the dbt marts, and every value "
            "carries a LIVE / CACHE / MOCK provenance badge — a mock number is "
            "never presented as a real KPI."
        ),
        kpi_tiles=tiles,
        footer=(
            f"{app.title} v{app.version} · read-only · authoritative KPIs are "
            "produced by the Dagster + dbt pipeline (make demo)"
        ),
    )


@app.get("/health")
def health() -> dict[str, Any]:
    try:
        summary = summarize_data()
    except Exception as exc:  # pragma: no cover - defensive surface for liveness
        raise HTTPException(status_code=503, detail=f"data layer unavailable: {exc}") from exc
    return {
        "status": "ok",
        "data_provenance": "seed/dealer_export/*.csv + dbt/seeds/*.csv (in-repo)",
        "total_seed_rows": summary["total_rows"],
        "note": (
            "Authoritative KPIs are produced by the Dagster + dbt pipeline "
            "(make demo); this API mirrors the mart definitions."
        ),
    }


@app.get("/data/summary")
def data_summary() -> dict[str, Any]:
    return summarize_data()


@app.get("/kpis")
def kpis() -> dict[str, Any]:
    """The headline KPI set with its honest provenance badge (LIVE/CACHE/MOCK)."""
    values, badge = _resolve_kpis()
    return {**values, "provenance_badge": _badge_payload(badge)}
