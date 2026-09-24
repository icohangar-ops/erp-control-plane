#!/usr/bin/env python3
"""Evidence check: 21-metric headline mart, golden set, and API pins agree.

Backs the ``evidence/matrix.yaml`` rows claiming the dbt ``kpi_headline`` mart
publishes 21 KPIs, the golden Q→A set covers one case per metric with expected
values, and ``tests/test_kpi_api.py`` pins the same values to the mart.
Cross-checks three independently committed surfaces (mart SQL, golden YAML, API
test pins) against each other, offline; exit 0 = verified, exit 1 = refused.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

EXPECTED_KPI_COUNT = 21
WINDOW_COLUMNS = {"window_start", "window_end", "window_days"}

# Headline values reproduced from the dbt-built mart (make demo / CI dbt job);
# the same numbers the golden set and the API test pin.
PINNED_EXPECTED = {
    "gmroi": 1.73,
    "inventory_turns": 5.34,
    "dio_days": 68.3,
    "gross_margin_pct": 0.2443,
    "line_fill_rate": 0.9113,
    "order_fill_rate": 0.6809,
    "otif_pct": 0.5444,
    "dso_days": 50.7,
    "dpo_days": 29.3,
    "ccc_days": 89.7,
}

MART_COLUMN_RE = re.compile(r"^\s*[a-z_]+\.([a-z_]+),?\s*$")
CASE_ID_RE = re.compile(r"^- id:\s*(\S+)\s*$")
METRIC_RE = re.compile(r"^\s+metric:\s*([a-z_]+)\s*$")
EXPECTED_RE = re.compile(r"^\s+expected:\s*([0-9][0-9.]*)\s*$")
API_PIN_RE = re.compile(r'^\s*"([a-z_]+)":\s*([0-9][0-9.]*),\s*$')


def parse_mart_kpi_columns(sql: str) -> set[str]:
    """Column aliases selected by kpi_headline.sql, excluding window columns."""
    columns: set[str] = set()
    in_select = False
    for line in sql.splitlines():
        if line.strip() == "select":
            in_select = True
            continue
        if in_select and line.strip().startswith("from"):
            break
        if in_select:
            column_match = MART_COLUMN_RE.match(line)
            if column_match:
                columns.add(column_match.group(1))
    return columns - WINDOW_COLUMNS


def parse_golden_cases(text: str) -> dict[str, float]:
    """Golden Q→A cases as metric → expected value, in declaration order."""
    cases: dict[str, float] = {}
    current: str | None = None
    for line in text.splitlines():
        case_match = CASE_ID_RE.match(line)
        if case_match:
            current = case_match.group(1)
            continue
        metric_match = METRIC_RE.match(line)
        if metric_match and current is not None:
            current = metric_match.group(1)
            continue
        expected_match = EXPECTED_RE.match(line)
        if expected_match and current is not None:
            cases[current] = float(expected_match.group(1))
    return cases


def parse_api_pins(text: str) -> dict[str, float]:
    """The EXPECTED_KPIS literal in tests/test_kpi_api.py."""
    pins: dict[str, float] = {}
    inside = False
    for line in text.splitlines():
        if line.startswith("EXPECTED_KPIS = {"):
            inside = True
            continue
        if inside:
            if line.startswith("}"):
                break
            pin_match = API_PIN_RE.match(line)
            if pin_match:
                pins[pin_match.group(1)] = float(pin_match.group(2))
    return pins


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    mart_sql = root / "dbt" / "models" / "marts" / "kpi_headline.sql"
    golden_yaml = root / "analytics" / "evals" / "golden_qa.yaml"
    api_test = root / "tests" / "test_kpi_api.py"
    for path in (mart_sql, golden_yaml, api_test):
        if not path.is_file():
            print(f"FAIL: expected surface missing: {path}")
            return 1

    kpi_columns = parse_mart_kpi_columns(mart_sql.read_text(encoding="utf-8"))
    if len(kpi_columns) != EXPECTED_KPI_COUNT:
        print(
            f"FAIL: kpi_headline selects {len(kpi_columns)} KPI columns, expected {EXPECTED_KPI_COUNT}"
        )
        return 1

    golden = parse_golden_cases(golden_yaml.read_text(encoding="utf-8"))
    if len(golden) != EXPECTED_KPI_COUNT:
        print(f"FAIL: golden set has {len(golden)} cases, expected {EXPECTED_KPI_COUNT}")
        return 1
    if set(golden) != kpi_columns:
        print(f"FAIL: golden metrics {sorted(golden)} != mart columns {sorted(kpi_columns)}")
        return 1

    api_pins = parse_api_pins(api_test.read_text(encoding="utf-8"))
    if api_pins != golden:
        print("FAIL: tests/test_kpi_api.py EXPECTED_KPIS drifted from the golden set")
        return 1

    drifted = {
        name: (expected, golden[name])
        for name, expected in PINNED_EXPECTED.items()
        if golden[name] != expected
    }
    if drifted:
        print(f"FAIL: pinned headline values drifted: {drifted}")
        return 1

    print(
        f"OK: {len(kpi_columns)} mart KPIs == {len(golden)} golden cases == API pins; headline values match"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
