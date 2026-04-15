.PHONY: install test lint dagster-dev dbt-compile

install:
	python3 -m pip install -e ".[dev]"

test:
	python3 -m pytest -q

lint:
	python3 -m mypy src tests

dagster-dev:
	DAGSTER_HOME=./dagster_home dagster dev -m orchestrator.definitions

dbt-compile:
	cd dbt_stub && dbt compile --profiles-dir . --project-dir .
