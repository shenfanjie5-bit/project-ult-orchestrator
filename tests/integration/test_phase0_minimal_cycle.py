from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from tests.integration.conftest import (
    asset_check_evaluations,
    asset_materialization_keys,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FORBIDDEN_ROOT_MODULES = (
    "graph" + "_engine",
    "main" + "_core",
    "data" + "_platform",
    "audit" + "_eval",
)
_FORBIDDEN_BUSINESS_IMPORT = re.compile(
    r"^(" + "|".join(re.escape(module) for module in _FORBIDDEN_ROOT_MODULES) + r")\.",
)


def test_materialize_phase0_readiness_ping(
    dagster_module: object,
    dagster_dbt_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.phase0 import phase0_readiness_ping

    result = dagster.materialize(
        [phase0_readiness_ping],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
        instance=dagster_instance,
    )

    assert result.success is True


def test_phase0_ping_check_emits_asset_check_evaluation(
    dagster_module: object,
    dagster_dbt_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module
    from orchestrator.checks.asset_checks import phase0_ping_check
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.phase0 import phase0_readiness_ping

    job = dagster.define_asset_job(
        "phase0_ping_check_job",
        selection=dagster.AssetSelection.assets("phase0_readiness_ping"),
    )
    defs = dagster.Definitions(
        assets=[phase0_readiness_ping],
        asset_checks=[phase0_ping_check],
        jobs=[job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )

    result = defs.get_job_def("phase0_ping_check_job").execute_in_process(
        instance=dagster_instance,
    )
    evaluations = asset_check_evaluations(result)

    assert result.success is True
    assert len(evaluations) >= 1
    assert any(
        _check_name(evaluation) == "phase0_ping_check"
        and getattr(evaluation, "passed", None) is True
        for evaluation in evaluations
    )


def test_definitions_loads_without_errors(
    dagster_module: object,
    dagster_dbt_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    defs = build_definitions(policy_path=stub_policy_path)

    assert isinstance(defs, dagster.Definitions)


def test_daily_cycle_job_executes_in_process(
    dagster_module: object,
    dagster_dbt_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module
    dagster_dbt = dagster_dbt_module

    from orchestrator.checks.asset_checks import phase0_ping_check
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.jobs.phase0 import (
        DBT_PROFILES_DIR,
        DBT_PROJECT_DIR,
        dbt_phase0_assets,
        phase0_readiness_ping,
    )

    defs = dagster.Definitions(
        assets=[phase0_readiness_ping, dbt_phase0_assets],
        asset_checks=[phase0_ping_check],
        jobs=[daily_cycle_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
            "dbt": dagster_dbt.DbtCliResource(
                project_dir=str(DBT_PROJECT_DIR),
                profiles_dir=str(DBT_PROFILES_DIR),
            ),
        },
    )
    result = defs.get_job_def("daily_cycle_job").execute_in_process(
        instance=dagster_instance,
    )
    materialized_keys = asset_materialization_keys(result)
    evaluations = asset_check_evaluations(result)
    heartbeat_key = _heartbeat_asset_key(dbt_phase0_assets)

    assert result.success is True
    assert dagster.AssetKey(["phase0_readiness_ping"]) in materialized_keys
    assert heartbeat_key in materialized_keys
    assert len(evaluations) >= 1
    assert "phase0_ping_check" in {
        _check_name(evaluation) for evaluation in evaluations
    }


def test_no_business_imports() -> None:
    violations: list[str] = []

    for path in sorted((_REPO_ROOT / "src" / "orchestrator").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for module_name, lineno in _absolute_imports(tree):
            if _FORBIDDEN_BUSINESS_IMPORT.match(module_name):
                relative_path = path.relative_to(_REPO_ROOT)
                violations.append(f"{relative_path}:{lineno}: {module_name}")

    assert violations == []


def _absolute_imports(tree: ast.AST) -> list[tuple[str, int]]:
    imports: list[tuple[str, int]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend((alias.name, node.lineno) for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imports.append((node.module, node.lineno))

    return imports


def _heartbeat_asset_key(dbt_phase0_assets: object) -> object:
    for asset_key in getattr(dbt_phase0_assets, "keys", ()):
        path = tuple(getattr(asset_key, "path", ()))
        if path and path[-1] == "heartbeat":
            return asset_key
    pytest.fail("dbt heartbeat model asset key was not registered")


def _check_name(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str):
        return check_name
    check_key = getattr(evaluation, "check_key", None)
    name = getattr(check_key, "name", None)
    return name if isinstance(name, str) else None
