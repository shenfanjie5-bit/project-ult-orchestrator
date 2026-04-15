from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FORBIDDEN_BUSINESS_IMPORT = re.compile(
    r"^(graph_engine|main_core|data_platform|audit_eval)\.",
)


def test_materialize_phase0_readiness_ping(
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")

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


def test_asset_check_passes(stub_policy_path: str) -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.checks.asset_checks import phase0_ping_check
    from orchestrator.checks.resources import GatePolicyResource

    result = execute_asset_check(
        phase0_ping_check,
        gate_policy=GatePolicyResource(policy_path=stub_policy_path),
    )

    assert result.passed is True


def test_definitions_loads_without_errors(
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")

    from orchestrator.definitions import build_definitions

    defs = build_definitions(policy_path=stub_policy_path)

    assert isinstance(defs, dagster.Definitions)


def test_daily_cycle_job_executes_in_process(
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    dagster_dbt = pytest.importorskip(
        "dagster_dbt",
        reason="dagster-dbt is not installed",
    )

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
    materializations = [
        event
        for event in result.all_events
        if getattr(event, "is_step_materialization", False)
        or getattr(event, "event_type_value", None) == "ASSET_MATERIALIZATION"
    ]

    assert result.success is True
    assert len(materializations) >= 1


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


def execute_asset_check(check_def: Any, **resource_kwargs: object) -> Any:
    if not callable(check_def):
        pytest.fail("asset check definition is not directly executable")

    return check_def(**resource_kwargs)
