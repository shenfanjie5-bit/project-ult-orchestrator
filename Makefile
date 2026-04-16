.PHONY: install test integration-test lint boundary-check dagster-dev dbt-compile

install:
	python3 -m pip install -e ".[dev]"

test:
	python3 -m pytest -q

integration-test:
	python3 -m pytest tests/integration -v --tb=short

lint:
	python3 -m mypy src tests

boundary-check:
	python3 scripts/check_boundaries.py

dagster-dev:
	DAGSTER_HOME=./dagster_home dagster dev -m orchestrator.definitions

dbt-compile:  ## requires the dev extra: dbt-core + dbt-duckdb
	mkdir -p dbt_stub/dagster_home
	cd dbt_stub && dbt compile --profiles-dir . --project-dir .
