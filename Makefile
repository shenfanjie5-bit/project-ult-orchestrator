PYTHON ?= python3
PYTHONPATH ?= src
export PYTHONPATH

.PHONY: install install-shared install-contracts-schemas install-all \
        test test-fast smoke contract regression \
        integration-test lint boundary-check dagster-dev dbt-compile

# Pure-pip dev install — offline-first per SUBPROJECT_TESTING_STANDARD.md §2.2.
install:
	$(PYTHON) -m pip install -e ".[dev]"

# install-shared adds the shared-fixtures git extra needed by tests/regression.
install-shared:
	$(PYTHON) -m pip install -e ".[dev,shared-fixtures]"

# install-contracts-schemas adds the contracts git extra (cross-repo align).
install-contracts-schemas:
	$(PYTHON) -m pip install -e ".[dev,contracts-schemas]"

# install-all gives the full offline+online dev environment.
install-all:
	$(PYTHON) -m pip install -e ".[dev,contracts-schemas,shared-fixtures]"

# Full suite — legacy tests/* + new canonical tier dirs.
test:
	$(PYTHON) -m pytest -q

# Fast lane for PR CI and local pre-commit. unit + boundary only.
test-fast:
	$(PYTHON) -m pytest tests/unit tests/boundary -q

# Minimal smoke — exercises public entrypoints. Infra-free.
smoke:
	$(PYTHON) -m pytest tests/smoke -q

# Contract tier — runs both self-check and (when contracts-schemas
# extra is installed) cross-repo alignment.
contract:
	$(PYTHON) -m pytest tests/contract -q

# Regression tier — explicit entry. Hard-fails when audit_eval_fixtures
# is not installed (no silent skip per iron rule #1).
regression:
	$(PYTHON) -m pytest tests/regression -q

# Existing repo-specific entries kept.
integration-test:
	$(PYTHON) -m pytest tests/integration -v --tb=short

lint:
	$(PYTHON) -m mypy src tests

boundary-check:
	$(PYTHON) scripts/check_boundaries.py

dagster-dev:
	DAGSTER_HOME=./dagster_home dagster dev -m orchestrator.definitions

dbt-compile:  ## requires the dev extra: dbt-core + dbt-duckdb
	mkdir -p dbt_stub/dagster_home
	cd dbt_stub && dbt compile --profiles-dir . --project-dir .
