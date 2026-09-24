"""Promote the demo's governed NL answers to Superset (spec §4.2 loop).

Same PromotionService the FastAPI route builds — the API route is the product
surface; this script drives it for the demo run. Requires
GENBI_SUPERSET_READONLY_DATABASE_ID pointing at the pre-registered READ_ONLY
DuckDB database in Superset.
"""

from __future__ import annotations

import json
import os
import sys

os.environ.setdefault("GENBI_SUPERSET_READONLY_DATABASE_ID", "1")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.genbi.routes import build_service
from api.genbi.viz import BackingSpec, MetricSpec, VizSpec

GMROI_SQL = 'SELECT "gmroi" FROM "main_marts"."kpi_headline" ORDER BY "window_end" DESC LIMIT 1'

CATEGORY_SQL = (
    'SELECT "dim_item"."category" AS "category", '
    'SUM("fact_invoice_line"."gross_margin_amount") AS "total_gross_margin_amount" '
    'FROM "main_canonical"."fact_invoice_line" '
    'JOIN "main_canonical"."dim_item" '
    'ON "fact_invoice_line"."item_key" = "dim_item"."item_key" '
    'GROUP BY "dim_item"."category"'
)


def main() -> int:
    service = build_service()

    print("=== PROMOTE 1 · GMROI (physical mart backing: main_marts.kpi_headline) ===")
    r1 = service.promote(
        question="What is our GMROI?",
        sql=GMROI_SQL,
        viz=VizSpec(
            viz_type="big_number_total",
            metrics=[MetricSpec(column="gmroi", aggregate="SUM", label="GMROI")],
        ),
        backing=BackingSpec(table="kpi_headline", table_schema="main_marts"),
    )
    print(json.dumps(r1, indent=2, default=str))

    print(
        "=== PROMOTE 2 · gross margin by category "
        "(virtual dataset over the validated answer SQL) ==="
    )
    r2 = service.promote(
        question="Show gross margin amount by item category",
        sql=CATEGORY_SQL,
        viz=VizSpec(
            viz_type="echarts_timeseries_bar",
            x_axis="category",
            metrics=[
                MetricSpec(
                    column="total_gross_margin_amount", aggregate="SUM", label="Gross margin"
                )
            ],
        ),
    )
    print(json.dumps(r2, indent=2, default=str))
    print("PROMOTION COMPLETE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
