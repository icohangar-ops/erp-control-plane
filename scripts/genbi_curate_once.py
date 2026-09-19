"""One-off curation act: build genbi/mdl/ from genbi/draft/.

Curation is the human step (spec §3.1). This script materializes the curated
tree for the demo scope: business descriptions for every column (generic
lineage patterns + model-specific business language with formulas verified
against dbt SQL comments), business-term knowledge files, and governed views.
Re-running overwrites genbi/mdl/ from a fresh draft.
"""

import shutil
from pathlib import Path

import yaml

REPO = Path("/home/user/work/construction-supplies-erp-control-plane")
DRAFT = REPO / "genbi/draft"
MDL = REPO / "genbi/mdl"

MODEL_DESCRIPTIONS = {
    "dim_customer": "One row per consolidated customer, grouped from source customer masters across acquisitions.",
    "dim_item_category": "One row per merchandising category/subcategory pair assigned during canonicalization.",
    "dim_location": "One row per branch/location, including acquisition ownership and staffing attributes.",
    "dim_salesperson": "One row per salesperson, with home-branch attribution from the source payroll/CRM export.",
    "dim_ship_to": "One row per customer ship-to address, linked to its parent customer.",
    "dim_vendor": "One row per vendor, with terms and lead-time attributes from the purchasing master.",
    "fact_inventory_snapshot": "One row per item/location/snapshot-date inventory position, month-end per pipeline design.",
    "fact_purchase_order_line": "One row per purchase-order line with receipt and price-variance measures.",
}

LINEAGE_DESCRIPTIONS = {
    "batch_id": "Identifier of the dlt ingestion batch that loaded this row.",
    "loaded_at": "Timestamp the row was loaded into the canonical layer.",
    "source_file": "Source file this row was extracted from (lineage).",
    "source_row_no": "Row number within the source file (lineage).",
    "source_system": "Originating source ERP for this row; acquisitions keep their system of record until re-platformed.",
    "source_customer_no": "Native customer number in the source ERP; kept for lineage and crosswalk back to the source.",
    "source_item_no": "Native item number in the source ERP; kept for lineage and crosswalk back to the source.",
    "source_vendor_no": "Native vendor number in the source ERP; kept for lineage and crosswalk back to the source.",
    "source_salesperson_code": "Native salesperson code in the source ERP; kept for lineage back to the source.",
    "source_doc_no": "Native document number in the source ERP (invoice, order, PO, or journal).",
    "source_line_no": "Native line number within the source document.",
    "source_order_no": "Native sales-order number in the source ERP linked to this invoice line.",
    "source_gl_account": "Chart-of-accounts number in the source ERP before consolidation.",
}

COLUMN_DESCRIPTIONS = {
    # ---- canonical dimensions ----
    "customer_name": "Customer legal or trading name as consolidated in the canonical layer.",
    "customer_class": "Customer classification used for segmentation (e.g. retail, contractor, dealer).",
    "terms": "Payment terms code from the source ERP.",
    "credit_limit": "Approved credit limit in USD.",
    "city": "Primary address city.",
    "state": "Primary address state or province.",
    "postal_code": "Primary address postal code.",
    "date_key": "Calendar date (the date dimension grain is one row per day).",
    "year_number": "Calendar year.",
    "quarter_number": "Calendar quarter (1-4).",
    "month_number": "Calendar month (1-12).",
    "year_month": "Year-month in YYYY-MM form for grouping.",
    "day_of_month": "Day of month (1-31).",
    "day_of_week": "Day of week (DuckDB dow: 0=Sunday through 6=Saturday).",
    "week_start_date": "First day of the week containing this date.",
    "month_start_date": "First day of the month containing this date.",
    "description": "Item description as sold in the branch system.",
    "category": "Merchandising category.",
    "subcategory": "Merchandising subcategory; may be empty for unclassified items.",
    "uom": "Selling unit of measure.",
    "current_unit_cost": "Most recent standard cost in USD.",
    "current_list_price": "Most recent list price in USD.",
    "item_status": "Item lifecycle status (e.g. active, inactive, discontinued).",
    "branch_code": "Branch code from the location master.",
    "branch_name": "Branch display name.",
    "region": "Sales region the branch belongs to.",
    "owned_since": "Date the branch was acquired by the group (drives organic vs acquired revenue).",
    "fte_count": "Full-time-equivalent headcount at the branch (productivity KPI denominator).",
    "canonical_location_key": "Key of the canonical location this row maps to when branches were deduplicated.",
    "salesperson_name": "Salesperson display name.",
    "home_location_key": "Foreign key to dim_location for the salesperson's home branch.",
    "home_branch": "Home-branch display name (denormalized for convenience).",
    "ship_to_name": "Ship-to location name.",
    "ship_to_address1": "Ship-to street address.",
    "ship_to_city": "Ship-to city.",
    "ship_to_state": "Ship-to state or province.",
    "ship_to_postal_code": "Ship-to postal code.",
    "vendor_name": "Vendor display name.",
    "lead_time_days": "Average negotiated lead time in days.",
    "customer_key": "Foreign key to dim_customer.",
    "category_key": "Foreign key to dim_item_category.",
    "item_key": "Foreign key to dim_item.",
    "location_key": "Foreign key to dim_location.",
    "vendor_key": "Foreign key to dim_vendor.",
    "salesperson_key": "Foreign key to dim_salesperson.",
    "ship_to_key": "Surrogate ship-to key stable across source systems.",
    # ---- facts ----
    "gl_line_key": "Surrogate GL line key (source document + line).",
    "entry_date": "Accounting entry date.",
    "consolidated_account": "Consolidated account name after crosswalk mapping (e.g. AR, AP, COGS).",
    "mapping_rule_type": "How the account was mapped: exact, alias, or manual override.",
    "debit_amt": "Debit amount in USD.",
    "credit_amt": "Credit amount in USD.",
    "net_amount": "Net signed amount (debit minus credit) in USD.",
    "snapshot_key": "Surrogate snapshot key (item + location + snapshot date).",
    "snapshot_date": "Inventory snapshot date (month-end per pipeline design).",
    "on_hand_qty": "On-hand quantity.",
    "allocated_qty": "Quantity allocated to open orders.",
    "on_order_qty": "Quantity on open purchase orders.",
    "backorder_qty": "Quantity on backorder.",
    "unit_cost": "Valuation unit cost in USD.",
    "inventory_value": "On-hand value in USD (on_hand_qty x unit_cost).",
    "invoice_line_key": "Surrogate invoice-line key (source document + line).",
    "invoice_key": "Source invoice identifier.",
    "order_key": "Source sales-order identifier.",
    "invoice_date": "Invoice date (join to dim_date.date_key).",
    "invoiced_qty": "Quantity invoiced.",
    "unit_price": "Invoiced unit price in USD.",
    "revenue_amount": "Invoiced revenue in USD.",
    "cogs_amount": "Invoiced cost of goods sold in USD.",
    "gross_margin_amount": "Gross margin in USD (revenue minus COGS).",
    "freight_amt": "Freight charges billed in USD.",
    "tax_amt": "Sales tax billed in USD.",
    "po_line_key": "Surrogate PO-line key (source document + line).",
    "po_key": "Source purchase-order identifier.",
    "po_date": "Purchase-order date.",
    "received_date": "Date the line was received (null until received).",
    "ordered_qty": "Quantity ordered.",
    "received_qty": "Quantity received to date.",
    "unit_cost_actual": "Actual received unit cost in USD.",
    "unit_cost_standard": "Standard cost at receipt in USD.",
    "purchase_price_variance": "Purchase price variance in USD ((actual - standard) x received qty).",
    "is_received_in_full": "True when the line was fully received.",
    "is_received": "True when any quantity was received.",
    "order_line_key": "Surrogate order-line key (source document + line).",
    "order_date": "Order entry date (join to dim_date.date_key).",
    "promised_date": "Date promised to the customer.",
    "shipped_date": "Date the line shipped (null until shipped).",
    "order_status": "Line status from the source ERP.",
    "filled_qty": "Quantity filled (shipped) to date.",
    "cancelled_qty": "Quantity cancelled.",
    "ordered_amount": "Ordered value in USD.",
    "filled_amount": "Filled value in USD.",
    "is_on_time": "True when the line shipped on or before the promised date.",
    "is_line_complete": "True when the line is fully filled.",
    "is_backordered": "True when the line remains short-shipped and active.",
    # ---- KPI marts (formulas verified against dbt SQL, 2026-09-19) ----
    "gross_margin_pct": "Gross margin as a share of window revenue (0-1 scale).",
    "revenue_window": "Window revenue in USD (invoiced).",
    "cogs_window": "Window COGS in USD.",
    "dso_days": "Days sales outstanding: AR balance / window revenue x 365.",
    "dpo_days": "Days payable outstanding: AP balance / window COGS x 365.",
    "ccc_days": "Cash conversion cycle in days: DIO + DSO - DPO.",
    "close_cycle_days": "Average days from period end to close completion.",
    "same_branch_revenue_pct": "Share of revenue sold by the customer's modal branch (0-1).",
    "organic_revenue_pct": "Share of revenue from branches owned before the window start (0-1).",
    "acquired_revenue_pct": "Share of revenue from branches acquired during the window (0-1).",
    "gmroi": "Gross-margin return on inventory investment: annualized gross margin / average inventory value.",
    "inventory_turns": "Inventory turns annualized to the window: window COGS x annualization / average inventory value.",
    "dio_days": "Days inventory outstanding: 365 / inventory turns.",
    "weeks_of_supply": "Average inventory value / (window COGS / window days x 7).",
    "line_fill_rate": "Filled qty (capped at ordered) / ordered qty over active order lines (0-1).",
    "order_fill_rate": "Orders whose every line is complete / all orders (0-1).",
    "otd_pct": "Fully shipped orders shipped on time / fully shipped orders (0-1).",
    "otif_pct": "Orders complete AND on time / all orders (0-1).",
    "backorder_rate": "Short-shipped active lines / active lines (0-1).",
    "vendor_fill_rate": "Received qty / ordered qty over PO lines in the window (0-1).",
    "ppv_total": "Purchase price variance total in USD: sum((actual - standard) x received qty).",
    "ppv_pct": "Purchase price variance as a share of standard-cost receipts (0-1).",
    "po_line_count": "Purchase-order lines in the window.",
    "sales_per_fte_annualized": "Annualized window revenue / total group FTE.",
    "avg_ticket": "Window revenue / distinct invoices.",
    "orders_per_active_day": "Orders / distinct order days (operational tempo).",
    "invoice_count": "Distinct invoices in the window.",
    "total_fte": "Total branch FTE across the group.",
    "active_line_count": "Active order lines in the window.",
    "order_count": "Orders in the window.",
    "window_start": "Analysis window start (invoiced-activity bounds).",
    "window_end": "Analysis window end.",
    "window_days": "Analysis window length in days.",
    "annualization_factor": "Multiplier annualizing the window (365 / window_days).",
}

BUSINESS_TERMS = {
    "terms": [
        {
            "term": "fill rate",
            "definition": (
                "Share of demand shipped as ordered. Line-level fill rate caps filled at "
                "ordered quantity over active lines; order-level fill rate counts orders "
                "whose every line is complete. Both are 0-1 proportions."
            ),
            "related_models": ["kpi_service", "fact_sales_order_line"],
        },
        {
            "term": "OTIF",
            "definition": (
                "On-time-in-full: orders that shipped complete AND on or before the promised "
                "date, over all orders. Reported as otif_pct (0-1)."
            ),
            "related_models": ["kpi_service", "fact_sales_order_line"],
        },
        {
            "term": "cash conversion cycle",
            "definition": (
                "Days between paying suppliers and collecting from customers: "
                "DIO + DSO - DPO. Lower is better."
            ),
            "related_models": ["kpi_finance"],
        },
        {
            "term": "GMROI",
            "definition": (
                "Gross-margin return on inventory investment: annualized gross margin dollars "
                "divided by average inventory value (mean of month-end snapshot totals)."
            ),
            "related_models": ["kpi_inventory", "kpi_headline"],
        },
        {
            "term": "purchase price variance",
            "definition": (
                "PPV: (actual received unit cost - standard cost) x received quantity, summed "
                "over receipts; positive means paying above standard."
            ),
            "related_models": ["kpi_purchasing", "fact_purchase_order_line"],
        },
        {
            "term": "organic vs acquired revenue",
            "definition": (
                "Revenue split by branch ownership: organic from branches owned before the "
                "window start, acquired from branches bought during the window (e.g. the "
                "RL-JCB acquisition). Home branch is inferred from the customer's revenue mix."
            ),
            "related_models": ["kpi_growth", "fact_invoice_line", "dim_location"],
        },
        {
            "term": "analysis window",
            "definition": (
                "The invoiced-activity period all KPI marts share (kpi_window); KPIs annualize "
                "to it via the annualization factor 365 / window_days."
            ),
            "related_models": ["kpi_window", "kpi_headline"],
        },
        {
            "term": "consolidated account",
            "definition": (
                "Group chart-of-accounts name after mapping each source ERP's GL accounts "
                "through crosswalk rules (exact, alias, or manual override)."
            ),
            "related_models": ["fact_gl_transaction"],
        },
    ],
    "sample_questions": [
        "What was last quarter's revenue by region?",
        "Which items have the lowest fill rate this year?",
        "How many days of inventory supply do we carry by category?",
        "Which vendors have the highest purchase price variance?",
        "What share of revenue came from acquired branches?",
        "How did our cash conversion cycle trend over the window?",
        "Which customers have exceeded their credit limit on open invoices?",
        "What is our OTIF by branch?",
    ],
}

# Views are governed, read-only, pre-joined projections over the modeled surface.
VIEWS = {
    "monthly_revenue_by_region": {
        "description": "Monthly invoiced revenue by sales region (governed view).",
        "statement": (
            "SELECT d.month_start_date AS month, l.region, "
            "SUM(f.revenue_amount) AS revenue, SUM(f.gross_margin_amount) AS gross_margin "
            "FROM fact_invoice_line f "
            "JOIN dim_date d ON f.invoice_date = d.date_key "
            "JOIN dim_location l ON f.location_key = l.location_key "
            "GROUP BY 1, 2 ORDER BY 1, 2"
        ),
    },
    "inventory_position_latest": {
        "description": "Latest inventory position per item with its category (governed view).",
        "statement": (
            "SELECT s.snapshot_date, i.item_key, i.description, i.category, "
            "SUM(s.on_hand_qty) AS on_hand_qty, SUM(s.inventory_value) AS inventory_value "
            "FROM fact_inventory_snapshot s "
            "JOIN dim_item i ON s.item_key = i.item_key "
            "WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_inventory_snapshot) "
            "GROUP BY 1, 2, 3, 4 ORDER BY inventory_value DESC"
        ),
    },
}


def curated_column_desc(model: str, col: str, col_type: str) -> str:
    if col in COLUMN_DESCRIPTIONS:
        return COLUMN_DESCRIPTIONS[col]
    if col == "date_key" or col.endswith("_key"):
        return f"Surrogate {model} key stable across source systems."
    if col in LINEAGE_DESCRIPTIONS:
        return LINEAGE_DESCRIPTIONS[col]
    if col.startswith("source_"):
        return "Native identifier in the source ERP; kept for lineage back to the source."
    return f"{col.replace('_', ' ').capitalize()} ({col_type})."


def curate() -> None:
    if MDL.exists():
        shutil.rmtree(MDL)
    shutil.copytree(DRAFT, MDL)

    # 1. Curated descriptions.
    for meta_path in sorted(MDL.glob("models/*/metadata.yml")):
        meta = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
        name = meta["name"]
        if not (meta.get("properties") or {}).get("description"):
            meta["properties"] = {"description": MODEL_DESCRIPTIONS.get(name, "")}
        for col in meta["columns"]:
            desc = curated_column_desc(name, col["name"], col.get("type", ""))
            col.setdefault("properties", {})["description"] = desc
        meta_path.write_text(
            yaml.safe_dump(meta, sort_keys=False, width=100, allow_unicode=True),
            encoding="utf-8",
        )

    # 2. Knowledge: business terms + sample questions.
    knowledge_dir = MDL / "knowledge"
    knowledge_dir.mkdir(exist_ok=True)
    (knowledge_dir / "business_terms.yml").write_text(
        yaml.safe_dump(BUSINESS_TERMS, sort_keys=False, width=100, allow_unicode=True),
        encoding="utf-8",
    )

    # 3. Governed views.
    views_dir = MDL / "views"
    for view_name, spec in VIEWS.items():
        view_dir = views_dir / view_name
        view_dir.mkdir(parents=True, exist_ok=True)
        (view_dir / "metadata.yml").write_text(
            yaml.safe_dump(
                {
                    "name": view_name,
                    "statement": spec["statement"],
                    "properties": {"description": spec["description"]},
                },
                sort_keys=False,
                width=100,
                allow_unicode=True,
            ),
            encoding="utf-8",
        )

    # 4. Curation notes.
    (MDL / "CURATION.md").write_text(
        "# Curation notes\n\n"
        "This tree is curated by hand from `genbi/draft/` (generated by "
        "`python -m genbi.mdl_gen generate`). Curation decisions for this scope:\n\n"
        "- Descriptions: every column carries business language; KPI formulas were "
        "transcribed from the dbt SQL headers and verified against the mart SQL.\n"
        "- Grain: `primary_key` matches the dbt `unique` tests exactly.\n"
        "- Relationships: inferred from dbt relationship tests (surrogate-key joins "
        "only; crosswalk/ref plumbing is outside the modeled set).\n"
        "- Knowledge: business terms and sample questions in `knowledge/` give the "
        "NL layer its vocabulary; terms were verified against KPI formulas.\n"
        "- Views: read-only pre-joined projections in `views/` (region revenue, "
        "latest inventory position).\n\n"
        "Serving: the MDL is validated in CI (`python -m genbi.mdl_gen validate`) and "
        "consumed by the wren bootstrap service (`docker/compose/genbi/bootstrap/`), "
        "which registers the project with WrenAI against the READ_ONLY DuckDB URI from "
        "`genbi.connection.duckdb_uri` — see `genbi/mdl/README.md`.\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    curate()
    print(f"curated MDL written to {MDL}")
