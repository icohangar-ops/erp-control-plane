"""Viz request models and the chart ``params`` builder (spec §4.2.4).

Params shapes mirror the v0.1 runbook's builders (``tile_params`` /
``line_params`` / ``bar_params``) so GenBI charts are indistinguishable from
hand-provisioned ones. ``viz_type`` is an allowlist — the NL layer cannot
request arbitrary chart plugins.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

VizType = Literal[
    "echarts_timeseries_bar",
    "echarts_timeseries_line",
    "big_number_total",
    "table",
    "pie",
]

Aggregates = Literal["SUM", "AVG", "COUNT", "MIN", "MAX", "COUNT_DISTINCT"]


class BackingSpec(BaseModel):
    """Physical backing for the dataset: a governed mart table/view (spec §4.2.3).

    Omitted => the answer becomes a virtual dataset over its validated SQL.
    Serialized as ``{"schema": "...", "table": "..."}``.
    """

    model_config = ConfigDict(populate_by_name=True)

    table: str
    table_schema: str | None = Field(default=None, alias="schema")


class MetricSpec(BaseModel):
    """One measure: a simple column aggregate or a SQL expression metric."""

    column: str
    aggregate: Aggregates = "SUM"
    label: str | None = None
    sql_expression: str | None = Field(
        default=None,
        description="Advanced form: an inline SQL expression (runbook sql_expr shape).",
    )


class VizSpec(BaseModel):
    """The chart the NL layer wants for this answer."""

    viz_type: VizType
    x_axis: str | None = None
    metrics: list[MetricSpec] = Field(default_factory=list)
    groupby: list[str] = Field(default_factory=list)
    row_limit: int = Field(default=500, ge=1)
    width: int = Field(default=6, ge=1, le=16, description="Grid units of 16 (runbook grid).")
    height: int = Field(default=50, ge=1, description="Chart height in grid rows (runbook: 50).")

    def bounded_row_limit(self, row_cap: int) -> int:
        return max(1, min(self.row_limit, row_cap))


def _metric_json(metric: MetricSpec) -> dict[str, object]:
    if metric.sql_expression:
        return {
            "expressionType": "SQL",
            "sqlExpression": metric.sql_expression,
            "label": metric.label or metric.column,
        }
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": metric.column},
        "aggregate": metric.aggregate,
        "label": metric.label or metric.column,
    }


def chart_params(viz: VizSpec, datasource_id: int, row_cap: int) -> dict[str, object]:
    """Build the params block the runbook's builders produce, per viz type."""
    metrics = [_metric_json(metric) for metric in viz.metrics]
    row_limit = viz.bounded_row_limit(row_cap)
    params: dict[str, object] = {
        "datasource": f"{datasource_id}__table",
        "viz_type": viz.viz_type,
        "time_range": "No filter",
        "row_limit": row_limit,
    }
    if viz.viz_type == "big_number_total":
        params.update(
            {
                "metrics": metrics,
                "groupby": [],
                "y_axis_format": "SMART_NUMBER",
                "subheader": "promoted GenBI answer",
            }
        )
    elif viz.viz_type in {"echarts_timeseries_bar", "echarts_timeseries_line"}:
        params.update(
            {
                "x_axis": viz.x_axis,
                "metrics": metrics,
                "groupby": viz.groupby,
                "y_axis_format": "SMART_NUMBER",
                "order_desc": True,
            }
        )
        if viz.viz_type == "echarts_timeseries_line":
            params.update(
                {
                    "granularity_sqla": viz.x_axis,
                    "time_grain_sqla": "P1M",
                }
            )
    elif viz.viz_type == "pie":
        params.update({"metrics": metrics, "groupby": [viz.x_axis] if viz.x_axis else []})
    else:  # table
        params.update(
            {
                "query_mode": "raw",
                "all_columns": [
                    viz.x_axis,
                    *(m.column for m in viz.metrics if not m.sql_expression),
                ],
            }
        )
    return params
