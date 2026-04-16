from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, cast

import pytest

from tests.integration.conftest import (
    asset_check_evaluations,
    asset_materialization_keys,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FORBIDDEN_RUNTIME_MODULE_ROOTS = ("main_core", "kafka", "flink")


def test_phase2_provider_contributes_daily_cycle_assets_and_checks(
    dagster_module: object,
    dagster_dbt_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME, PHASE2_STAGE_KEYS

    phase2_calls: list[str] = []
    phase0_provider = _fake_phase0_surface_provider(dagster)
    phase1_provider = _fake_phase1_provider(dagster)
    phase2_provider, _phase2_assets, _phase2_check = _fake_phase2_provider(
        dagster,
        phase2_calls,
    )
    defs = build_definitions(
        module_factories=[phase0_provider, phase1_provider, phase2_provider],
        policy_path=stub_policy_path,
    )

    dagster.Definitions.validate_loadable(defs)

    phase2_asset_keys = {dagster.AssetKey([stage]) for stage in PHASE2_STAGE_KEYS}
    selected_keys = daily_cycle_job.selection.resolve(defs.assets or ())

    assert phase2_asset_keys <= selected_keys
    assert _asset_keys_for_group(defs, PHASE2_GROUP_NAME) == phase2_asset_keys
    assert "fake_phase2_l7_pure_check" in _check_names(defs)

    result = defs.get_job_def("daily_cycle_job").execute_in_process(
        instance=dagster_instance,
        tags={"cycle_id": "cycle-20260416"},
    )
    materialized_keys = asset_materialization_keys(result)
    evaluations = asset_check_evaluations(result)

    assert result.success is True
    assert phase2_asset_keys <= materialized_keys
    assert phase2_calls == list(PHASE2_STAGE_KEYS)
    assert any(
        _check_name(evaluation) == "fake_phase2_l7_pure_check"
        and getattr(evaluation, "passed", None) is True
        for evaluation in evaluations
    )


def test_milestone2_requires_phase2_provider_assets(
    dagster_module: object,
    dagster_dbt_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    monkeypatch.setenv("ORCHESTRATOR_DEFINITIONS_PROFILE", "milestone-2")

    with pytest.raises(ValueError, match="phase2.*group_name='phase2'"):
        build_definitions(
            module_factories=[
                _fake_phase0_surface_provider(dagster),
                _fake_phase1_provider(dagster),
            ],
            policy_path=stub_policy_path,
        )


def test_orchestrator_has_no_forbidden_phase2_runtime_imports() -> None:
    violations: list[str] = []

    for path in sorted((_REPO_ROOT / "src" / "orchestrator").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for module_name, lineno in _absolute_imports(tree):
            if _is_forbidden_runtime_import(module_name):
                relative_path = path.relative_to(_REPO_ROOT)
                violations.append(f"{relative_path}:{lineno}: {module_name}")

    assert violations == []


def _fake_phase0_surface_provider(dagster: Any) -> object:
    from orchestrator.checks import DataReadinessSignal
    from orchestrator.jobs.phase0_constants import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GROUP_NAME,
    )
    from orchestrator.sensors.data_readiness import DATA_READINESS_RESOURCE_KEY

    class FakeDataReadinessProvider:
        def get_data_readiness_signal(self) -> DataReadinessSignal:
            return DataReadinessSignal(ready=True, cycle_id="cycle-20260416")

    class FakeDataReadinessResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> FakeDataReadinessProvider:
            return FakeDataReadinessProvider()

    class FakeLLMHealthResult:
        healthy = True
        summary = "provider ready"
        provider = "fake-llm"

    class FakeLLMHealthProbe:
        def check_health(self) -> FakeLLMHealthResult:
            return FakeLLMHealthResult()

    class FakeLLMHealthProbeResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> FakeLLMHealthProbe:
            return FakeLLMHealthProbe()

    @dagster.asset(
        name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        group_name=PHASE0_GROUP_NAME,
    )
    def candidate_freeze() -> str:
        return "frozen"

    class FakePhase0SurfaceProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (candidate_freeze,)

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {
                DATA_READINESS_RESOURCE_KEY: cast(
                    object,
                    FakeDataReadinessResource(),
                ),
                "llm_health_probe": cast(object, FakeLLMHealthProbeResource()),
            }

    return FakePhase0SurfaceProvider()


def _fake_phase1_provider(dagster: Any) -> object:
    from orchestrator.jobs.phase0_constants import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_READINESS_ASSET_KEY,
    )
    from orchestrator.jobs.phase1 import (
        PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        PHASE1_GROUP_NAME,
    )

    @dagster.asset(
        name=PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
        deps=[
            dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
            dagster.AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
        ],
    )
    def graph_promotion() -> str:
        return "promoted"

    @dagster.asset(
        name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
    )
    def graph_snapshot(graph_promotion: str) -> str:
        return f"snapshot:{graph_promotion}"

    class FakePhase1Provider:
        def get_assets(self) -> tuple[object, ...]:
            return (graph_promotion, graph_snapshot)

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {}

    return FakePhase1Provider()


def _fake_phase2_provider(
    dagster: Any,
    calls: list[str],
) -> tuple[object, tuple[object, ...], object]:
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME, PHASE2_STAGE_KEYS

    @dagster.asset(name=PHASE2_STAGE_KEYS[0], group_name=PHASE2_GROUP_NAME)
    def phase2_l1(graph_snapshot: str) -> str:
        calls.append(PHASE2_STAGE_KEYS[0])
        return f"{graph_snapshot}:l1"

    @dagster.asset(name=PHASE2_STAGE_KEYS[1], group_name=PHASE2_GROUP_NAME)
    def phase2_l2(l1: str) -> str:
        calls.append(PHASE2_STAGE_KEYS[1])
        return f"{l1}:l2"

    @dagster.asset(name=PHASE2_STAGE_KEYS[2], group_name=PHASE2_GROUP_NAME)
    def phase2_l3(l2: str) -> str:
        calls.append(PHASE2_STAGE_KEYS[2])
        return f"{l2}:l3"

    @dagster.asset(name=PHASE2_STAGE_KEYS[3], group_name=PHASE2_GROUP_NAME)
    def phase2_l4(l3: str) -> str:
        calls.append(PHASE2_STAGE_KEYS[3])
        return f"{l3}:l4"

    @dagster.asset(name=PHASE2_STAGE_KEYS[4], group_name=PHASE2_GROUP_NAME)
    def phase2_l5(l4: str) -> str:
        calls.append(PHASE2_STAGE_KEYS[4])
        return f"{l4}:l5"

    @dagster.asset(name=PHASE2_STAGE_KEYS[5], group_name=PHASE2_GROUP_NAME)
    def phase2_l6(l5: str) -> str:
        calls.append(PHASE2_STAGE_KEYS[5])
        return f"{l5}:l6"

    @dagster.asset(name=PHASE2_STAGE_KEYS[6], group_name=PHASE2_GROUP_NAME)
    def phase2_l7(l6: str) -> str:
        calls.append(PHASE2_STAGE_KEYS[6])
        return f"{l6}:l7"

    @dagster.asset_check(
        asset=phase2_l7,
        name="fake_phase2_l7_pure_check",
    )
    def fake_phase2_l7_pure_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    phase2_assets = (
        phase2_l1,
        phase2_l2,
        phase2_l3,
        phase2_l4,
        phase2_l5,
        phase2_l6,
        phase2_l7,
    )

    class FakePhase2Provider:
        def get_assets(self) -> tuple[object, ...]:
            return phase2_assets

        def get_checks(self) -> tuple[object, ...]:
            return (fake_phase2_l7_pure_check,)

        def get_resources(self) -> dict[str, object]:
            return {}

    return FakePhase2Provider(), phase2_assets, fake_phase2_l7_pure_check


def _asset_keys_for_group(defs: Any, group_name: str) -> set[object]:
    return {
        asset_key
        for asset_def in defs.assets or ()
        for asset_key in getattr(asset_def, "keys", ())
        if getattr(asset_def, "group_names_by_key", {}).get(asset_key) == group_name
    }


def _check_names(defs: Any) -> set[str]:
    names: set[str] = set()
    for check_def in defs.asset_checks or ():
        names.update(
            check_key.name
            for check_key in getattr(check_def, "check_keys", ())
        )
        names.update(spec.name for spec in getattr(check_def, "specs", ()))
        if name := getattr(check_def, "name", None):
            names.add(name)
    return names


def _check_name(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str):
        return check_name
    check_key = getattr(evaluation, "check_key", None)
    name = getattr(check_key, "name", None)
    return name if isinstance(name, str) else None


def _absolute_imports(tree: ast.AST) -> list[tuple[str, int]]:
    imports: list[tuple[str, int]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend((alias.name, node.lineno) for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imports.append((node.module, node.lineno))

    return imports


def _is_forbidden_runtime_import(module_name: str) -> bool:
    return any(
        module_name == root or module_name.startswith(f"{root}.")
        for root in _FORBIDDEN_RUNTIME_MODULE_ROOTS
    )
