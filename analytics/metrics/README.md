# Semantic Metric Registry

A versioned registry of qualified metric definitions over the canonical dbt
warehouse — the contract that turns "open POs" from a guess into a name. The
registry extends the golden-master crosswalk from entity identity to metric
definition: every definition is qualified by source system (`open_po@dynamics`,
`open_po@csv_sftp`), because the same business question measures different
populations per acquired ERP.

**dbt stays the single source of truth.** The registry references dbt models
and columns; it never restates mart math. A mart-bound metric is a bare dbt
column reference; anything more is refused.

## Components

| File | Purpose |
| --- | --- |
| `registry.yaml` | 22 measured definitions: `open_po@csv_sftp` bound to the canonical fact, plus all 21 `kpi_headline` metrics qualified for `csv_sftp`. Each entry carries a population `count_sql` and the `measured` size it must keep matching. |
| `registry.py` | Schema validation, immutable definitions, resolution with refusal semantics, dbt-manifest cross-check, and population re-measurement. |
| `check.py` | CI gate: load + validate, cross-check bindings against the dbt manifest, re-measure every population against the built warehouse. |

## Naming and resolution

Definitions are named `metric@source_system`. Resolution refuses rather than
guesses:

- **Unknown name** → error listing the valid names (closed vocabulary).
- **Ambiguous bare name** (a metric registered for two sources) → error listing
  every candidate definition with its measured size. Qualify the name to pick
  one exactly.
- **Redefinition** → schema error: a definition carries exactly one name.

## CI gate

```bash
make metric-registry   # == python -m analytics.metrics.check --dbt-dir dbt --duckdb data/analytics/analytics.duckdb
```

The gate fails (exit 1) when the registry references a model or column the
dbt manifest does not define, binds to staging/crosswalk plumbing, restates
mart math as an expression, or when a recorded population size no longer
matches the built warehouse. It refuses a missing dbt manifest or warehouse
with exit 2 — the registry is only checked against built artifacts, never
against assumptions.

The `metric-registry` CI job builds the mart first, so a dbt change that
alters grain or populations fails until the registry is re-measured in the
same PR.

## GenBI parity integration

`analytics/evals/run_evals.py` resolves every golden `metric` through this
registry. The NL surface (WrenAI/Superset) may only pick registry-qualified
names: the parity gate refuses unknown, ambiguous, or wrongly bound metrics
with the registry's own error before comparing any values, and it scores the
registry-resolved column instead of trusting a raw string. See
`analytics/evals/README.md`.

## When a population or mart change is intentional

```bash
make dbt-build
# re-measure: python -m analytics.metrics.check reports the new sizes
# update registry.yaml measured values, commit together with the dbt change
git add analytics/metrics/registry.yaml && git commit -m "chore(metrics): re-measure populations after mart change"
```
