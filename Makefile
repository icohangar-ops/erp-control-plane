# =============================================================================
# construction-supplies-erp-control-plane — developer & deployment entry points
# =============================================================================
.PHONY: help demo up up-bi down dbt-build dbt-test test lint typecheck ci clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

demo: ## Run the seeded CSV dealer end-to-end: extract -> Parquet -> dbt build -> KPI report
	python demo/run_demo.py

up: ## Start postgres + dagster (docker compose)
	docker compose up -d
	@echo "Dagster UI: http://localhost:3000"

up-bi: ## Start postgres + dagster + superset (profile: bi)
	docker compose --profile bi up -d
	@echo "Dagster UI: http://localhost:3000 · Superset: http://localhost:8088"

down: ## Stop the stack
	docker compose --profile bi down

dbt-build: ## Run dbt build (seeds + models + tests) against the demo DuckDB target
	dbt build --project-dir dbt --profiles-dir dbt --target demo

dbt-test: ## Run dbt tests only
	dbt test --project-dir dbt --profiles-dir dbt --target demo

test: ## Run the Python test suite (connector contracts, csv_sftp e2e)
	pytest

lint: ## Ruff lint + format check
	ruff check .
	ruff format --check .

typecheck: ## Byte-compile every module (basic import sanity)
	python -m compileall -q connectors control_plane orchestration demo scripts

ci: lint typecheck test ## Everything CI runs locally

clean: ## Remove regenerable pipeline data
	rm -rf data/
