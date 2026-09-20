"""GenBI evaluation runner for the WrenAI + Ragas layer.

Three modes:

  --check-parity            CI gate: every golden expected value must match the
                            dbt-built main_marts.kpi_headline mart (keyless).
  --live --wren-url URL     End-to-end: pose each golden question to the
                            wren-ai-service, execute the generated SQL against
                            the DuckDB mart, score deterministic correctness,
                            optionally add Ragas LLM-judged metrics.
  --replay FILE             Score recorded ask outcomes (offline / CI smoke).

Ragas is optional: the judge path activates only when the ragas package is
importable AND OPENAI_API_KEY is set (see analytics/evals/README.md — ragas
must be installed in an isolated venv, not the app environment).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import yaml

try:
    from analytics.evals.wren_client import WrenAIClient, WrenAIError
except ModuleNotFoundError:  # direct execution: python3 analytics/evals/run_evals.py
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from analytics.evals.wren_client import WrenAIClient, WrenAIError

DEFAULT_GOLDEN = Path(__file__).with_name("golden_qa.yaml")
DEFAULT_DUCKDB = os.environ.get("ANALYTICS_DUCKDB_PATH", "./data/analytics/analytics.duckdb")
REPORT_DIR = Path(__file__).with_name("reports")
VALID_UNITS = {"ratio", "percent", "days", "usd"}

_NUMBER_RE = re.compile(r"-?\d[\d,]*(?:\.\d+)?")


# ---------------------------------------------------------------------------
# Golden set


def load_golden(path: Path) -> dict:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1:
        raise SystemExit(f"unsupported golden schema_version: {document.get('schema_version')!r}")
    cases = document.get("cases") or []
    if not cases:
        raise SystemExit(f"no golden cases found in {path}")
    ids = [case.get("id") for case in cases]
    if len(set(ids)) != len(ids):
        raise SystemExit("golden case ids are not unique")
    for case in cases:
        for field_name in ("question", "metric", "unit", "expected", "tolerance"):
            if case.get(field_name) is None:
                raise SystemExit(f"golden case {case.get('id')!r} is missing {field_name!r}")
        if case["unit"] not in VALID_UNITS:
            raise SystemExit(f"golden case {case['id']!r} has unknown unit {case['unit']!r}")
        if float(case["tolerance"]) <= 0:
            raise SystemExit(f"golden case {case['id']!r} tolerance must be positive")
    return document


# ---------------------------------------------------------------------------
# Scoring primitives (deterministic, keyless)


def extract_first_number(text: str) -> float | None:
    match = _NUMBER_RE.search(text.replace("%", ""))
    if not match:
        return None
    return float(match.group(0).replace(",", ""))


def is_within_tolerance(unit: str, expected: float, tolerance: float, actual: float) -> bool:
    """Compare an extracted value against the golden expectation.

    percent-unit metrics are stored as ratios (0.2443 == 24.43%). Generated
    answers may use either convention, so both interpretations are accepted.
    """
    candidates = {actual, actual / 100.0} if unit == "percent" else {actual}
    return any(not math.isnan(c) and abs(c - expected) <= tolerance for c in candidates)


def execute_sql_value(duckdb_path: str, sql: str) -> float:
    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        cur = con.execute(sql)
        row = cur.fetchone()
        if row is None or row[0] is None:
            raise ValueError("query returned no rows")
        return float(row[0])
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Modes


def check_parity(golden: dict, duckdb_path: str) -> list[str]:
    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        cur = con.execute("select * from main_marts.kpi_headline")
        columns = [d[0] for d in cur.description]
        row = cur.fetchone()
        if row is None:
            return ["main_marts.kpi_headline is empty"]
        values = dict(
            zip(columns, row, strict=False)
        )  # window columns hold dates; cast per-metric below
    finally:
        con.close()

    mismatches = []
    for case in golden["cases"]:
        metric = case["metric"]
        if metric not in values:
            mismatches.append(f"{case['id']}: metric column {metric!r} missing from the mart")
            continue
        actual = float(values[metric])
        if not is_within_tolerance(case["unit"], case["expected"], case["tolerance"], actual):
            mismatches.append(
                f"{case['id']}: mart {metric}={actual!r} does not match golden "
                f"expected={case['expected']!r} (tolerance {case['tolerance']})"
            )
    return mismatches


def run_live(golden: dict, wren_url: str, mdl_hash: str | None, duckdb_path: str) -> list[dict]:
    client = WrenAIClient(wren_url, mdl_hash=mdl_hash)
    results = []
    for case in golden["cases"]:
        entry = {
            "id": case["id"],
            "question": case["question"],
            "unit": case["unit"],
            "expected": case["expected"],
        }
        try:
            outcome = client.ask(case["question"])
        except WrenAIError as exc:
            entry.update(correct=False, reason=f"transport error: {exc}")
            results.append(entry)
            print(f"  {case['id']:<26} TRANSPORT ERROR: {str(exc)[:120]}")
            continue

        entry.update(
            status=outcome.status,
            sql=outcome.sql,
            retrieved_tables=outcome.retrieved_tables,
            reasoning=outcome.reasoning,
            error_code=outcome.error_code,
            latency_s=outcome.latency_s,
        )
        if outcome.status != "finished" or not outcome.sql:
            detail = outcome.error_message or f"status={outcome.status}"
            entry.update(correct=False, reason=detail)
            print(f"  {case['id']:<26} NOT FINISHED: {detail[:120]}")
            results.append(entry)
            continue

        try:
            value = execute_sql_value(duckdb_path, outcome.sql)
            entry["value"] = value
            entry["correct"] = is_within_tolerance(
                case["unit"], case["expected"], case["tolerance"], value
            )
            if not entry["correct"]:
                entry["reason"] = f"generated SQL returned {value!r}, expected {case['expected']!r}"
        except Exception as exc:
            entry.update(correct=False, reason=f"SQL execution failed: {exc}")
        results.append(entry)
        mark = "OK " if entry["correct"] else "BAD"
        got = entry.get("value", "n/a")
        print(
            f"  {case['id']:<26} {mark} expected={case['expected']!r:>12} "
            f"got={got!r:>12} ({outcome.latency_s}s)"
        )
    return results


def run_replay(golden: dict, replay_path: Path, duckdb_path: str) -> list[dict]:
    replay = json.loads(replay_path.read_text(encoding="utf-8"))
    by_id = {case["id"]: case for case in replay.get("cases", [])}
    results = []
    for case in golden["cases"]:
        recorded = by_id.get(case["id"])
        if recorded is None:
            results.append({"id": case["id"], "correct": False, "reason": "no replay record"})
            continue
        entry = {
            "id": case["id"],
            "status": recorded.get("status"),
            "sql": recorded.get("sql"),
            "latency_s": recorded.get("latency_s", 0.0),
        }
        answer_text = recorded.get("answer_text")
        if recorded.get("sql"):
            try:
                value = execute_sql_value(duckdb_path, recorded["sql"])
                entry.update(value=value, correct=True)
            except Exception as exc:
                entry.update(correct=False, reason=f"SQL execution failed: {exc}")
        elif answer_text:
            value = extract_first_number(answer_text)
            if value is None:
                entry.update(correct=False, reason=f"no number in answer: {answer_text[:120]!r}")
            else:
                entry.update(
                    value=value,
                    correct=is_within_tolerance(
                        case["unit"], case["expected"], case["tolerance"], value
                    ),
                )
        else:
            entry.update(correct=False, reason="replay record has neither sql nor answer_text")
        results.append(entry)
        mark = "OK " if entry["correct"] else "BAD"
        got = entry.get("value", "n/a")
        print(f"  {case['id']:<26} {mark} got={got!r}")
    return results


# ---------------------------------------------------------------------------
# Optional Ragas judge


def ragas_judge(golden: dict, results: list[dict]) -> tuple[dict | None, str]:
    try:
        from ragas import EvaluationDataset
        from ragas import evaluate as ragas_evaluate
        from ragas.metrics import answer_relevancy, context_precision, faithfulness
    except Exception as exc:  # pragma: no cover - environment dependent
        return None, f"ragas not importable ({str(exc)[:160]})"

    rows = []
    for case, result in zip(golden["cases"], results, strict=False):
        value = result.get("value")
        response = "no answer produced"
        if value is not None:
            if case["unit"] == "percent":
                response = f"{value * 100:.2f}%"
            else:
                response = f"{value:.4f}".rstrip("0").rstrip(".")
        contexts = [piece for piece in (result.get("reasoning"), result.get("sql")) if piece]
        rows.append(
            {
                "user_input": case["question"],
                "response": response,
                "retrieved_contexts": contexts or ["no context retrieved"],
                "reference": f"dbt golden truth for {case['metric']}: {case['expected']!r}",
            }
        )

    try:
        result = ragas_evaluate(
            EvaluationDataset.from_list(rows),
            metrics=[faithfulness, answer_relevancy, context_precision],
        )
        frame = result.to_pandas()
        scores = {
            metric: round(float(frame[metric].mean()), 4)
            for metric in ("faithfulness", "answer_relevancy", "context_precision")
            if metric in frame.columns
        }
        return scores, "ragas judge completed"
    except Exception as exc:  # pragma: no cover - depends on judge LLM availability
        return None, f"ragas judge failed ({str(exc)[:200]})"


# ---------------------------------------------------------------------------
# Entry point


def main() -> int:
    parser = argparse.ArgumentParser(description="GenBI evaluation runner (golden set + Ragas).")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument(
        "--check-parity", action="store_true", help="CI gate: golden set vs dbt mart."
    )
    modes.add_argument(
        "--live", action="store_true", help="Run end-to-end against a live WrenAI service."
    )
    modes.add_argument("--replay", type=Path, metavar="FILE", help="Score recorded ask outcomes.")
    parser.add_argument("--wren-url", default=os.environ.get("WREN_AI_SERVICE_URL", ""))
    parser.add_argument("--mdl-hash", default=os.environ.get("WREN_MDL_HASH"))
    parser.add_argument("--golden", type=Path, default=DEFAULT_GOLDEN)
    parser.add_argument("--duckdb", default=DEFAULT_DUCKDB)
    parser.add_argument("--report", type=Path, help="Where to write the JSON report (live/replay).")
    parser.add_argument("--judge", choices=("auto", "on", "off"), default="auto")
    args = parser.parse_args()

    golden = load_golden(args.golden)

    if args.check_parity:
        if not Path(args.duckdb).exists():
            print(f"mart not found at {args.duckdb} — run `make dbt-build` first", file=sys.stderr)
            return 2
        mismatches = check_parity(golden, args.duckdb)
        if mismatches:
            print("GenBI golden-set parity FAILED:")
            for mismatch in mismatches:
                print(f"  - {mismatch}")
            return 1
        print(f"GenBI golden-set parity OK: {len(golden['cases'])} cases match the dbt mart.")
        return 0

    if args.live and not args.wren_url:
        print("--live requires --wren-url (or WREN_AI_SERVICE_URL)", file=sys.stderr)
        return 2

    started = time.monotonic()
    if args.live:
        print(f"== GenBI evals: live against {args.wren_url} ==")
        results = run_live(golden, args.wren_url, args.mdl_hash, args.duckdb)
    else:
        print(f"== GenBI evals: replay from {args.replay} ==")
        results = run_replay(golden, args.replay, args.duckdb)

    if args.judge == "off":
        scores, note = None, "judge disabled"
    elif args.judge == "on" or os.environ.get("OPENAI_API_KEY"):
        scores, note = ragas_judge(golden, results)
    else:
        scores, note = None, "judge skipped (OPENAI_API_KEY not set; deterministic scoring only)"

    total = len(results)
    correct = sum(1 for r in results if r.get("correct"))
    elapsed = round(time.monotonic() - started, 1)
    print(f"summary: {correct}/{total} correct in {elapsed}s · judge: {note}")
    if scores:
        print(f"ragas: {json.dumps(scores)}")

    report_path = (
        args.report or REPORT_DIR / f"eval-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "mode": "live" if args.live else "replay",
                "golden": str(args.golden),
                "summary": {
                    "total": total,
                    "correct": correct,
                    "elapsed_s": elapsed,
                    "judge": note,
                    "ragas_scores": scores,
                },
                "cases": results,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"report: {report_path}")

    return 0 if correct == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
