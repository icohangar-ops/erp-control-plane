"""The promotion loop end to end, against an in-memory fake Superset.

The fake speaks exactly the REST surface ``SupersetClient`` uses (login, CSRF,
dataset/chart/dashboard REST with ``q`` filters and ``json_metadata`` positions)
and — like the real Superset — happily allows a second chart POST with the same
``slice_name``: the dedup contract belongs to the caller, which is exactly what
the concurrency test pins.
"""

from __future__ import annotations

import datetime as dt
import json
import threading
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from api.genbi.audit import AuditTrail
from api.genbi.config import GenbiSettings, NotConfigured
from api.genbi.coverage import CoverageQueue
from api.genbi.guardrails import (
    GuardrailError,
    NotReadOnlyUri,
    execute_readonly,
)
from api.genbi.layout import (
    HEADER_ROW_ID,
    append_chart_row,
    ensure_ask_save_header,
    fresh_layout,
)
from api.genbi.promote import PromotionService
from api.genbi.slugs import dataset_table_name, genbi_row_id, slug_for_question
from api.genbi.superset import SupersetClient, SupersetError
from api.genbi.viz import MetricSpec, VizSpec, chart_params

ANSWER_DATE = dt.date(2026, 9, 19)
QUESTION = "What is revenue by branch?"
SQL = "select branch_code, sum(revenue) as revenue from dealer_revenue group by 1 order by 2 desc"
VIZ = VizSpec(
    viz_type="echarts_timeseries_bar",
    x_axis="branch_code",
    metrics=[MetricSpec(column="revenue", aggregate="SUM", label="Revenue")],
)


class FakeSuperset:
    """In-memory Superset implementing the exact surface the client drives."""

    READONLY_URI = "duckdb:///analytics.duckdb?access_mode=READ_ONLY&read_only=1"
    RW_URI = "duckdb:///analytics.duckdb"

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.databases: dict[int, dict[str, Any]] = {
            1: {
                "id": 1,
                "database_name": "GenBI analytics (READ ONLY)",
                "sqlalchemy_uri": self.READONLY_URI,
            },
            2: {
                "id": 2,
                "database_name": "GenBI analytics (read-write!)",
                "sqlalchemy_uri": self.RW_URI,
            },
        }
        self.datasets: dict[str, dict[str, Any]] = {}  # by table_name
        self.charts: dict[str, dict[str, Any]] = {}  # by slice_name
        self.dashboards: dict[str, dict[str, Any]] = {}  # by slug
        self.counts: Counter[str] = Counter()
        self.next_id = 100
        self.chart_find_delay = 0.0  # widen the find->create window for races
        self.fail_chart_post = False

    # ------------------------------------------------------------ transport
    def handler(self, request: httpx.Request) -> httpx.Response:
        method, path = request.method, request.url.path
        if method == "POST" and path == "/api/v1/security/login":
            self.counts["login"] += 1
            return httpx.Response(200, json={"access_token": "test-token"})
        if method == "GET" and path == "/api/v1/security/csrf_token/":
            return httpx.Response(200, json={"result": "test-csrf"})
        if method == "GET" and path.startswith("/api/v1/database/"):
            database_id = int(path.rsplit("/", 1)[1])
            database = self.databases.get(database_id)
            if database is None:
                return httpx.Response(404, json={"message": "not found"})
            return httpx.Response(200, json={"result": database})
        if path == "/api/v1/dataset/":
            return self._dataset_route(request, method)
        if path == "/api/v1/chart/" or path.startswith("/api/v1/chart/"):
            return self._chart_route(request, method, path)
        if path == "/api/v1/dashboard/" or path.startswith("/api/v1/dashboard/"):
            return self._dashboard_route(request, method, path)
        return httpx.Response(404, json={"message": f"fake superset: unhandled {method} {path}"})

    def _filter_value(self, request: httpx.Request) -> Any:
        # httpx returns the raw query as bytes on some versions; url.params
        # decodes it properly on all of them.
        q = request.url.params.get("q", "{}")
        filters = json.loads(q).get("filters", [])
        return filters[0]["value"] if filters else None

    def _dataset_route(self, request: httpx.Request, method: str) -> httpx.Response:
        if method == "GET":
            table_name = self._filter_value(request)
            with self.lock:
                matches = [d for d in self.datasets.values() if d["table_name"] == table_name]
            return httpx.Response(
                200,
                json={"result": [{"id": m["id"], "table_name": m["table_name"]} for m in matches]},
            )
        payload = json.loads(request.content)
        with self.lock:
            self.counts["dataset_post"] += 1
            if payload["database"] != 1:
                return httpx.Response(400, json={"message": "fake: dataset must use database 1"})
            dataset_id = self.next_id
            self.next_id += 1
            self.datasets[payload["table_name"]] = {
                "id": dataset_id,
                "database": payload["database"],
                "table_name": payload["table_name"],
                "schema": payload.get("schema"),
                "sql": payload.get("sql"),
            }
        return httpx.Response(200, json={"id": dataset_id})

    def _chart_route(self, request: httpx.Request, method: str, path: str) -> httpx.Response:
        if method == "GET" and path == "/api/v1/chart/":
            if self.chart_find_delay:
                time.sleep(self.chart_find_delay)
            slice_name = self._filter_value(request)
            with self.lock:
                matches = [c for c in self.charts.values() if c["slice_name"] == slice_name]
            return httpx.Response(
                200,
                json={
                    "result": [
                        {"id": m["id"], "uuid": m["uuid"], "slice_name": m["slice_name"]}
                        for m in matches
                    ]
                },
            )
        if method == "POST" and path == "/api/v1/chart/":
            with self.lock:
                self.counts["chart_post"] += 1  # duplicates allowed, like real Superset
                if self.fail_chart_post:
                    return httpx.Response(500, json={"message": "boom"})
                payload = json.loads(request.content)
                chart_id = self.next_id
                self.next_id += 1
                uuid_value = f"uuid-{chart_id}"
                self.charts[payload["slice_name"]] = {
                    "id": chart_id,
                    "uuid": uuid_value,
                    "slice_name": payload["slice_name"],
                    "params": payload["params"],
                    "viz_type": payload["viz_type"],
                    "datasource_id": payload["datasource_id"],
                }
            return httpx.Response(200, json={"id": chart_id, "uuid": uuid_value})
        chart_id = int(path.rsplit("/", 1)[1])
        if method == "PUT":
            with self.lock:
                self.counts["chart_put"] += 1
                for chart in self.charts.values():
                    if chart["id"] == chart_id:
                        chart["params"] = json.loads(json.loads(request.content)["params"])
            return httpx.Response(200, json={"id": chart_id})
        if method == "GET":
            with self.lock:
                for chart in self.charts.values():
                    if chart["id"] == chart_id:
                        return httpx.Response(200, json={"result": chart})
        return httpx.Response(404, json={"message": "chart not found"})

    def _dashboard_route(self, request: httpx.Request, method: str, path: str) -> httpx.Response:
        if method == "GET" and path == "/api/v1/dashboard/":
            slug = self._filter_value(request)
            with self.lock:
                matches = [d for d in self.dashboards.values() if d["slug"] == slug]
            return httpx.Response(
                200, json={"result": [{"id": m["id"], "slug": m["slug"]} for m in matches]}
            )
        if method == "POST" and path == "/api/v1/dashboard/":
            payload = json.loads(request.content)
            with self.lock:
                self.counts["dashboard_post"] += 1
                dashboard_id = self.next_id
                self.next_id += 1
                self.dashboards[payload["slug"]] = {
                    "id": dashboard_id,
                    "slug": payload["slug"],
                    "dashboard_title": payload["dashboard_title"],
                    "position_json": "",
                    "json_metadata": "",
                }
            return httpx.Response(201, json={"id": dashboard_id})
        dashboard_id = int(path.rsplit("/", 1)[1])
        if method == "GET":
            with self.lock:
                for dashboard in self.dashboards.values():
                    if dashboard["id"] == dashboard_id:
                        return httpx.Response(200, json={"result": dashboard})
        if method == "PUT":
            metadata = json.loads(json.loads(request.content)["json_metadata"])
            with self.lock:
                self.counts["dashboard_put"] += 1
                for dashboard in self.dashboards.values():
                    if dashboard["id"] == dashboard_id:
                        dashboard["position_json"] = json.dumps(metadata["positions"])
            return httpx.Response(200, json={"id": dashboard_id})
        return httpx.Response(404, json={"message": "dashboard not found"})

    # ------------------------------------------------------------ helpers
    def layout(self, slug: str) -> dict[str, Any]:
        raw = self.dashboards[slug]["position_json"]
        return json.loads(raw) if raw else {}


@pytest.fixture()
def fake() -> FakeSuperset:
    return FakeSuperset()


@pytest.fixture()
def env(tmp_path: Path, analytics_file: Path) -> dict[str, str]:
    return {
        "GENBI_SUPERSET_URL": "http://superset.test",
        "GENBI_SUPERSET_USER": "admin",
        "GENBI_SUPERSET_PASSWORD": "pw",
        "GENBI_SUPERSET_READONLY_DATABASE_ID": "1",
        "GENBI_ANALYTICS_DUCKDB_PATH": str(analytics_file),
        "GENBI_ROW_CAP": "500",
        "GENBI_AUDIT_PATH": str(tmp_path / "audit.jsonl"),
        "GENBI_COVERAGE_PATH": str(tmp_path / "coverage.jsonl"),
    }


def make_service(
    env: dict[str, str], fake: FakeSuperset, executor: Callable[..., Any] = execute_readonly
) -> PromotionService:
    settings = GenbiSettings.from_env(env)
    transport = httpx.MockTransport(fake.handler)
    client = SupersetClient(
        settings.superset_url,
        settings.superset_user,
        settings.superset_password,
        client=httpx.Client(transport=transport),
    )
    return PromotionService(
        settings=settings,
        superset=client,
        audit=AuditTrail(settings.audit_path),
        coverage=CoverageQueue(settings.coverage_path),
        executor=executor,
    )


def grid_children(layout: dict[str, Any]) -> list[str]:
    return layout["GRID_ID"]["children"]


def assert_runbook_invariants(layout: dict[str, Any]) -> None:
    """The three Superset-4.1 constraints the v0.1 runbook established."""
    assert layout["DASHBOARD_VERSION_KEY"] == "v2"
    for node_id, node in layout.items():
        if not isinstance(node, dict):  # e.g. DASHBOARD_VERSION_KEY
            continue
        if node.get("type") in {"ROW", "COLUMN"}:
            assert node["meta"]["background"], f"{node_id} missing meta.background (SPA crash)"
        if node.get("type") == "CHART":
            assert node["meta"]["width"] > 0, f"{node_id} zero-width orphan"
            assert node["meta"]["height"] > 0, f"{node_id} zero-height orphan"


# ---------------------------------------------------------------- the loop
def test_promotion_creates_governed_artifacts(env: dict[str, str], fake: FakeSuperset) -> None:
    service = make_service(env, fake)
    result = service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)

    assert result["chart_created"] is True
    assert result["slug"] == slug_for_question(QUESTION, ANSWER_DATE)
    assert result["rows_returned"] == 3

    # Virtual dataset: on the READ_ONLY database (id 1), defined by the answer SQL.
    table_name = dataset_table_name(QUESTION)
    assert table_name in fake.datasets
    dataset = fake.datasets[table_name]
    assert dataset["database"] == 1
    assert dataset["sql"] == SQL
    assert dataset["schema"] is None

    # Chart: deterministic slug, datasource points at the dataset, runbook params.
    chart = fake.charts[result["slug"]]
    assert chart["datasource_id"] == dataset["id"]
    params = json.loads(chart["params"])  # the runbook sends params as a JSON string
    assert params["datasource"] == f"{dataset['id']}__table"
    assert params["viz_type"] == "echarts_timeseries_bar"
    assert params["metrics"][0]["expressionType"] == "SIMPLE"

    # Layout: header row separating GenBI answers, then the question's row.
    layout = fake.layout("genbi-ask-save")
    assert_runbook_invariants(layout)
    assert HEADER_ROW_ID in layout
    assert grid_children(layout) == [HEADER_ROW_ID, genbi_row_id(QUESTION)]
    header_node = layout[HEADER_ROW_ID]
    assert header_node["children"][0].startswith("MARKDOWN-")

    # Audit: executed (query) then promoted (persistence), newest first.
    records = AuditTrail(GenbiSettings.from_env(env).audit_path).list()
    assert [r["outcome"] for r in records] == ["promoted", "executed"]
    assert records[0]["artifacts"]["chart_id"] == result["chart_id"]
    assert records[1]["sql"] == SQL
    assert records[1]["rows_returned"] == 3


def test_repromotion_creates_nothing_new(env: dict[str, str], fake: FakeSuperset) -> None:
    service = make_service(env, fake)
    first = service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)
    second = service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)

    assert second["chart_created"] is False
    assert second["chart_id"] == first["chart_id"]
    assert second["dataset_id"] == first["dataset_id"]
    assert fake.counts["chart_post"] == 1, "re-run must update, not create"
    assert fake.counts["chart_put"] == 1
    assert fake.counts["dataset_post"] == 1
    assert len(grid_children(fake.layout("genbi-ask-save"))) == 2, "no duplicate grid rows"

    records = AuditTrail(GenbiSettings.from_env(env).audit_path).list()
    assert [r["outcome"] for r in records] == ["updated", "executed", "promoted", "executed"]


def test_distinct_questions_get_distinct_rows(env: dict[str, str], fake: FakeSuperset) -> None:
    service = make_service(env, fake)
    service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)
    other = "Show open purchase orders by vendor"
    service.promote(other, SQL, VIZ, answer_date=ANSWER_DATE)

    layout = fake.layout("genbi-ask-save")
    assert grid_children(layout) == [HEADER_ROW_ID, genbi_row_id(QUESTION), genbi_row_id(other)]
    assert len(fake.charts) == 2
    assert_runbook_invariants(layout)


def test_curated_layout_is_preserved(env: dict[str, str], fake: FakeSuperset) -> None:
    service = make_service(env, fake)
    dashboard_id, _ = service.superset.ensure_dashboard("genbi-ask-save", "GenBI — Ask → Save")
    curated = fresh_layout()
    curated["CHART-99"] = {
        "type": "CHART",
        "id": "CHART-99",
        "children": [],
        "parents": ["ROOT_ID", "ROW-curated"],
        "meta": {
            "chartId": 99,
            "uuid": "uuid-99",
            "width": 4,
            "height": 50,
            "background": "BACKGROUND_TRANSPARENT",
        },
    }
    curated["ROW-curated"] = {
        "type": "ROW",
        "id": "ROW-curated",
        "parents": ["ROOT_ID", "GRID_ID"],
        "children": ["CHART-99"],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    curated["GRID_ID"]["children"].append("ROW-curated")
    service.superset.put_positions(dashboard_id, curated)

    service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)
    layout = fake.layout("genbi-ask-save")
    assert grid_children(layout)[0] == "ROW-curated", "curated charts stay above"
    assert HEADER_ROW_ID in grid_children(layout)
    assert "CHART-99" in layout
    assert_runbook_invariants(layout)


# ---------------------------------------------------------------- guardrails
def test_guardrail_rejection_persists_nothing_and_is_audited(
    env: dict[str, str], fake: FakeSuperset
) -> None:
    service = make_service(env, fake)
    with pytest.raises(GuardrailError):
        service.promote(QUESTION, "delete from dealer_revenue", VIZ, answer_date=ANSWER_DATE)

    assert not fake.datasets and not fake.charts, "rejected answers must not persist"
    records = AuditTrail(GenbiSettings.from_env(env).audit_path).list()
    assert len(records) == 1
    assert records[0]["outcome"] == "guardrail_rejected"
    assert records[0]["detail"], "the audit record must carry the guardrail reason"


def test_read_write_database_refused(env: dict[str, str], fake: FakeSuperset) -> None:
    env = {**env, "GENBI_SUPERSET_READONLY_DATABASE_ID": "2"}
    service = make_service(env, fake)
    with pytest.raises(NotReadOnlyUri):
        service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)
    assert not fake.datasets and not fake.charts, "loop must fail closed on a RW database"


def test_unconfigured_database_id_fails_before_execution(
    env: dict[str, str], fake: FakeSuperset
) -> None:
    env = {k: v for k, v in env.items() if k != "GENBI_SUPERSET_READONLY_DATABASE_ID"}
    service = make_service(env, fake, executor=lambda *a, **k: pytest.fail("must not execute"))
    with pytest.raises(NotConfigured):
        service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)


def test_superset_failure_is_audited_and_maps_to_superset_error(
    env: dict[str, str], fake: FakeSuperset
) -> None:
    fake.fail_chart_post = True
    service = make_service(env, fake)
    with pytest.raises(SupersetError):
        service.promote(QUESTION, SQL, VIZ, answer_date=ANSWER_DATE)
    records = AuditTrail(GenbiSettings.from_env(env).audit_path).list()
    assert [r["outcome"] for r in records] == ["superset_error", "executed"]


# ------------------------------------------------------- spec risk #10: races
def test_concurrent_saves_do_not_double_create(env: dict[str, str], fake: FakeSuperset) -> None:
    """Two simultaneous saves of the same question must converge on one chart."""
    fake.chart_find_delay = 0.1  # widen the find->create window
    service = make_service(env, fake)
    results: dict[int, dict[str, Any]] = {}
    barrier = threading.Barrier(2)

    def worker() -> None:
        barrier.wait()  # maximize overlap
        results[threading.get_ident()] = service.promote(
            QUESTION, SQL, VIZ, answer_date=ANSWER_DATE
        )

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert fake.counts["chart_post"] == 1, "spec risk #10: concurrent saves must not double-create"
    assert fake.counts["dataset_post"] == 1
    assert len({r["chart_id"] for r in results.values()}) == 1
    assert len(grid_children(fake.layout("genbi-ask-save"))) == 2


# ---------------------------------------------------------------- viz params
def test_row_limit_is_bounded_by_cap() -> None:
    viz = VizSpec(viz_type="table", x_axis="branch_code", row_limit=9999)
    params = chart_params(viz, datasource_id=5, row_cap=500)
    assert params["row_limit"] == 500


def test_line_params_match_runbook_shape() -> None:
    viz = VizSpec(
        viz_type="echarts_timeseries_line",
        x_axis="invoice_month",
        metrics=[MetricSpec(column="revenue", aggregate="SUM")],
    )
    params = chart_params(viz, datasource_id=5, row_cap=100)
    assert params["granularity_sqla"] == "invoice_month"
    assert params["time_grain_sqla"] == "P1M"
    assert params["groupby"] == []
    assert params["metrics"][0]["column"] == {"column_name": "revenue"}


def test_sql_expression_metric_shape() -> None:
    viz = VizSpec(
        viz_type="big_number_total",
        metrics=[MetricSpec(column="margin", sql_expression="sum(gm)/sum(rev)", label="Margin %")],
    )
    params = chart_params(viz, datasource_id=5, row_cap=100)
    metric = params["metrics"][0]
    assert metric["expressionType"] == "SQL"
    assert metric["sqlExpression"] == "sum(gm)/sum(rev)"


def test_pie_and_table_params() -> None:
    pie = chart_params(
        VizSpec(viz_type="pie", x_axis="branch_code", metrics=[MetricSpec(column="revenue")]),
        datasource_id=5,
        row_cap=100,
    )
    assert pie["groupby"] == ["branch_code"]
    table = chart_params(
        VizSpec(viz_type="table", x_axis="branch_code", metrics=[MetricSpec(column="revenue")]),
        datasource_id=5,
        row_cap=100,
    )
    assert table["query_mode"] == "raw"
    assert table["all_columns"] == ["branch_code", "revenue"]


def test_unknown_viz_type_rejected() -> None:
    with pytest.raises(ValidationError):
        VizSpec(viz_type="dashboard", metrics=[MetricSpec(column="revenue")])


# ---------------------------------------------------------------- layout
def test_header_is_idempotent() -> None:
    layout = fresh_layout()
    ensure_ask_save_header(layout)
    ensure_ask_save_header(layout)
    assert grid_children(layout) == [HEADER_ROW_ID]


def test_chart_row_is_idempotent() -> None:
    layout = fresh_layout()
    append_chart_row(layout, question=QUESTION, chart_id=1, chart_uuid="u")
    append_chart_row(layout, question=QUESTION, chart_id=1, chart_uuid="u")
    assert grid_children(layout) == [HEADER_ROW_ID, genbi_row_id(QUESTION)]


def test_empty_dashboard_layout_backfills_skeleton() -> None:
    layout: dict[str, Any] = {}
    append_chart_row(layout, question=QUESTION, chart_id=2, chart_uuid="u")
    assert_runbook_invariants(layout)
