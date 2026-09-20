# GenBI Evaluation Gate — WrenAI + Ragas

Evaluates the governed GenBI layer (WrenAI NL→SQL over the 21-model MDL, Qdrant
retrieval) against a versioned golden Q→A set pinned to dbt `kpi_headline`
truth. The warehouse is the reference: if the GenBI layer drifts from dbt,
evals fail like any other test.

## Components

| File | Purpose |
| --- | --- |
| `golden_qa.yaml` | 21 golden questions, one per `kpi_headline` metric, with expected values, units, and tolerances. Committed and versioned. |
| `generate_golden.py` | Regenerates `golden_qa.yaml` from the dbt-built mart. Fails if a mart metric has no question template, so coverage cannot silently lag the warehouse. |
| `wren_client.py` | Synchronous client for the wren-ai-service ask flow. Contract verified against wren-ai-service 0.29.0 source: `POST /v1/asks` → poll `GET /v1/asks/{id}/result` until `finished`/`failed`/`stopped`. |
| `run_evals.py` | Runner: parity check, live e2e, replay scoring; deterministic scorer always, Ragas judge optionally. |
| `reports/` | JSON reports from live/replay runs (gitignored). |

## Modes

```bash
# CI gate (keyless): golden expectations must equal the dbt-built mart
make evals-parity          # == python analytics/evals/run_evals.py --check-parity

# Live end-to-end against a running WrenAI stack (wherever Docker Compose runs it)
WREN_URL=http://localhost:5555 make evals

# Score recorded outcomes without a live service
python analytics/evals/run_evals.py --replay path/to/replay.json
```

Live runs pose each golden question to the wren-ai-service, execute the
generated SQL against the DuckDB mart (`data/analytics/analytics.duckdb`), and
compare the returned value to the golden expectation (percent metrics accept
either `0.2443` or `24.43`). Transport errors, non-terminal asks, SQL that
fails to execute, and wrong values all count as failures; the runner exits 1.

## Ragas judge (optional, isolated venv)

Deterministic scoring needs nothing beyond the app environment. The Ragas
LLM-judged metrics (faithfulness, answer relevancy, context precision) need
the `ragas` package, which **must live in an isolated venv**: installing it
into the app environment upgrades `sqlglot` past the `dagster-dbt` pin
(`sqlglot[rs]<28.1.0`) and breaks the app suite (observed 2026-09-20,
Python 3.13).

```bash
python -m venv .venv-evals
.venv-evals/bin/pip install -r requirements-evals.txt
OPENAI_API_KEY=sk-... .venv-evals/bin/python analytics/evals/run_evals.py \
  --live --wren-url http://localhost:5555 --judge on
```

The judge composes, per question: `user_input` (golden question), `response`
(the value the system produced), `retrieved_contexts` (WrenAI's
`sql_generation_reasoning` and generated SQL), and `reference` (the dbt golden
truth). Judge scores are reported in the console and the JSON report; the
runner never fabricates scores — if ragas or the judge LLM is unavailable, the
note says so and the deterministic result stands.

## CI behavior

The `dbt` CI job builds the mart and then runs `--check-parity`, so a dbt
change that moves a headline KPI without regenerating the golden set fails
CI. The judge does not run in CI (no secrets, no WrenAI stack on runners);
live e2e and judge runs are local or scheduled against a deployed WrenAI.

## Regenerating the golden set

When a mart change is intentional:

```bash
make dbt-build
python analytics/evals/generate_golden.py
git add analytics/evals/golden_qa.yaml && git commit -m "chore(evals): refresh golden set to new mart truth"
```
