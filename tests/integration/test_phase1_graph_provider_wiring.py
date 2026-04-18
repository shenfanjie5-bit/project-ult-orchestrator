from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest


def test_phase1_graph_provider_contributes_daily_cycle_selection(
    dagster_module: object,
    dagster_dbt_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.jobs.phase0 import (
        PHASE0_GRAPH_STATUS_ASSET_KEY,
        dbt_phase0_assets,
        phase0_readiness_ping,
    )
    from orchestrator.jobs.phase1 import (
        PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        PHASE1_GROUP_NAME,
    )

    phase0_provider = _fake_phase0_surface_provider(dagster)
    provider, graph_promotion, graph_snapshot, _graph_check = (
        _fake_graph_provider(dagster)
    )
    defs = build_definitions(
        module_factories=[phase0_provider, provider],
        policy_path=stub_policy_path,
    )

    dagster.Definitions.validate_loadable(defs)

    promotion_key = dagster.AssetKey([PHASE1_GRAPH_PROMOTION_ASSET_KEY])
    snapshot_key = dagster.AssetKey([PHASE1_GRAPH_SNAPSHOT_ASSET_KEY])
    graph_status_key = dagster.AssetKey([PHASE0_GRAPH_STATUS_ASSET_KEY])
    selected_keys = daily_cycle_job.selection.resolve(
        [
            phase0_readiness_ping,
            dbt_phase0_assets,
            *phase0_provider.get_assets(),
            graph_promotion,
            graph_snapshot,
        ],
    )

    assert dagster.AssetKey(["phase0_readiness_ping"]) in selected_keys
    assert _heartbeat_asset_key(dbt_phase0_assets) in selected_keys
    assert graph_status_key in selected_keys
    assert promotion_key in selected_keys
    assert snapshot_key in selected_keys
    assert _phase1_asset_keys(defs, PHASE1_GROUP_NAME) == {
        promotion_key,
        snapshot_key,
    }
    assert "fake_graph_snapshot_check" in _check_names(defs)

    result = dagster.materialize(
        [
            phase0_readiness_ping,
            *phase0_provider.get_assets(),
            graph_promotion,
            graph_snapshot,
        ],
        instance=dagster_instance,
    )

    assert result.success is True
    assert graph_status_key in _dependency_keys(graph_promotion, promotion_key)


def test_milestone2_missing_phase1_contract_is_rejected(
    dagster_module: object,
    dagster_dbt_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    monkeypatch.setenv("ORCHESTRATOR_DEFINITIONS_PROFILE", "milestone-2")

    with pytest.raises(ValueError, match="phase1.*contract"):
        build_definitions(
            module_factories=[_fake_phase0_surface_provider(dagster)],
            policy_path=stub_policy_path,
        )


def test_phase1_graph_promotion_without_phase0_dependency_is_rejected(
    dagster_module: object,
    dagster_dbt_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    provider, _graph_promotion, _graph_snapshot, _graph_check = _fake_graph_provider(
        dagster,
        include_phase0_dependency=False,
    )

    with pytest.raises(ValueError, match="phase1 graph_promotion.*Phase 0"):
        build_definitions(
            module_factories=[provider],
            policy_path=stub_policy_path,
        )


def test_phase1_graph_promotion_without_graph_ready_gate_is_rejected(
    dagster_module: object,
    dagster_dbt_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    provider, _graph_promotion, _graph_snapshot, _graph_check = _fake_graph_provider(
        dagster,
        include_graph_status_dependency=False,
    )

    with pytest.raises(ValueError, match="graph_status"):
        build_definitions(
            module_factories=[provider],
            policy_path=stub_policy_path,
        )


def test_phase1_graph_snapshot_without_ancestry_is_rejected(
    dagster_module: object,
    dagster_dbt_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    provider, _graph_promotion, _graph_snapshot, _graph_check = _fake_graph_provider(
        dagster,
        snapshot_depends_on_promotion=False,
    )

    with pytest.raises(ValueError, match="phase1 graph_snapshot.*graph_promotion"):
        build_definitions(
            module_factories=[provider],
            policy_path=stub_policy_path,
        )


def test_phase0_graph_consistency_failure_blocks_phase1(
    dagster_module: object,
    dagster_instance: object,
) -> None:
    dagster = dagster_module
    defs, calls = _graph_gate_execution_defs(
        dagster,
        graph_status_value="stale",
        check_passes=False,
    )

    result = defs.get_job_def("graph_gate_job").execute_in_process(
        instance=dagster_instance,
        raise_on_error=False,
    )

    materialized_keys = _materialized_keys(result)

    assert result.success is False
    assert calls == ["graph_status"]
    assert dagster.AssetKey(["graph_status"]) in materialized_keys
    assert dagster.AssetKey(["graph_promotion"]) not in materialized_keys
    assert dagster.AssetKey(["graph_snapshot"]) not in materialized_keys


def test_phase0_graph_reload_path_allows_phase1_after_ready_status(
    dagster_module: object,
    dagster_instance: object,
) -> None:
    dagster = dagster_module
    defs, calls = _graph_gate_execution_defs(
        dagster,
        graph_status_value="ready",
        check_passes=True,
        reload_before_ready=True,
    )

    result = defs.get_job_def("graph_gate_job").execute_in_process(
        instance=dagster_instance,
    )

    materialized_keys = _materialized_keys(result)

    assert result.success is True
    assert calls == ["cold_reload", "graph_status", "graph_promotion"]
    assert dagster.AssetKey(["graph_status"]) in materialized_keys
    assert dagster.AssetKey(["graph_promotion"]) in materialized_keys
    assert dagster.AssetKey(["graph_snapshot"]) in materialized_keys


def _fake_graph_provider(
    dagster: Any,
    *,
    include_phase0_dependency: bool = True,
    include_graph_status_dependency: bool = True,
    snapshot_depends_on_promotion: bool = True,
) -> tuple[object, object, object, object]:
    from orchestrator.jobs.phase0 import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GRAPH_STATUS_ASSET_KEY,
        PHASE0_READINESS_ASSET_KEY,
    )
    from orchestrator.jobs.phase1 import (
        PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        PHASE1_GROUP_NAME,
    )
    promotion_asset_kwargs: dict[str, object] = {
        "name": PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        "group_name": PHASE1_GROUP_NAME,
    }
    if include_phase0_dependency:
        promotion_asset_kwargs["deps"] = [
            dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
            dagster.AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
            *(
                [dagster.AssetKey([PHASE0_GRAPH_STATUS_ASSET_KEY])]
                if include_graph_status_dependency
                else []
            ),
        ]

    @dagster.asset(**promotion_asset_kwargs)
    def graph_promotion() -> str:
        return "promoted"

    if snapshot_depends_on_promotion:

        @dagster.asset(
            name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
            group_name=PHASE1_GROUP_NAME,
        )
        def graph_snapshot(graph_promotion: str) -> str:
            return f"snapshot:{graph_promotion}"

    else:

        @dagster.asset(
            name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
            group_name=PHASE1_GROUP_NAME,
        )
        def graph_snapshot() -> str:
            return "snapshot:independent"

    @dagster.asset_check(
        asset=graph_snapshot,
        name="fake_graph_snapshot_check",
    )
    def fake_graph_snapshot_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    class FakeGraphProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (graph_promotion, graph_snapshot)

        def get_checks(self) -> tuple[object, ...]:
            return (fake_graph_snapshot_check,)

        def get_resources(self) -> dict[str, object]:
            return {}

    return (
        FakeGraphProvider(),
        graph_promotion,
        graph_snapshot,
        fake_graph_snapshot_check,
    )


def _fake_phase0_surface_provider(dagster: Any) -> object:
    from orchestrator.checks import DataReadinessSignal
    from orchestrator.jobs.phase0 import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
        PHASE0_GRAPH_STATUS_ASSET_KEY,
        PHASE0_GROUP_NAME,
    )
    from orchestrator.sensors.data_readiness import DATA_READINESS_RESOURCE_KEY

    class FakeDataReadinessProvider:
        def get_data_readiness_signal(self) -> DataReadinessSignal:
            return DataReadinessSignal(ready=True, cycle_id="cycle-20260416")

    class FakeDataReadinessResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> FakeDataReadinessProvider:
            return FakeDataReadinessProvider()

    class FakeProviderHealthStatus:
        provider = "fake-llm"
        model = "critical-model"
        reachable = True
        latency_ms = 12.0
        quota_status = "available"
        error = None

    class FakeLLMHealthReport:
        provider_statuses = (FakeProviderHealthStatus(),)
        all_critical_targets_available = True
        summary = "provider ready"

    class FakeLLMHealthProbe:
        def check_health(self) -> FakeLLMHealthReport:
            return FakeLLMHealthReport()

    class FakeLLMHealthProbeResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> FakeLLMHealthProbe:
            return FakeLLMHealthProbe()

    @dagster.asset(name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def candidate_freeze() -> str:
        return "ok"

    @dagster.asset(name=PHASE0_GRAPH_STATUS_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def graph_status(candidate_freeze: str) -> str:
        return f"{candidate_freeze}:ready"

    @dagster.asset_check(
        asset=graph_status,
        name=PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
        blocking=True,
    )
    def neo4j_graph_consistency_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    class FakePhase0SurfaceProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (candidate_freeze, graph_status)

        def get_checks(self) -> tuple[object, ...]:
            return (neo4j_graph_consistency_check,)

        def get_resources(self) -> dict[str, object]:
            return {
                DATA_READINESS_RESOURCE_KEY: cast(
                    object,
                    FakeDataReadinessResource(),
                ),
                "llm_health_probe": cast(object, FakeLLMHealthProbeResource()),
            }

    return FakePhase0SurfaceProvider()


def _graph_gate_execution_defs(
    dagster: Any,
    *,
    graph_status_value: str,
    check_passes: bool,
    reload_before_ready: bool = False,
) -> tuple[object, list[str]]:
    from orchestrator.jobs.phase0 import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
        PHASE0_GRAPH_STATUS_ASSET_KEY,
        PHASE0_GROUP_NAME,
        PHASE0_READINESS_ASSET_KEY,
    )
    from orchestrator.jobs.phase1 import (
        PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        PHASE1_GROUP_NAME,
    )

    calls: list[str] = []

    @dagster.asset(name=PHASE0_READINESS_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def phase0_readiness_ping() -> str:
        return "ready"

    @dagster.asset(
        name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        group_name=PHASE0_GROUP_NAME,
    )
    def candidate_freeze() -> str:
        return "frozen"

    @dagster.asset(name=PHASE0_GRAPH_STATUS_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def graph_status(candidate_freeze: str) -> str:
        del candidate_freeze
        if reload_before_ready:
            calls.append("cold_reload")
        calls.append("graph_status")
        return graph_status_value

    @dagster.asset_check(
        asset=graph_status,
        name=PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
        blocking=True,
    )
    def neo4j_graph_consistency_check() -> object:
        return dagster.AssetCheckResult(
            passed=check_passes,
            metadata={"graph_status": graph_status_value},
        )

    @dagster.asset(
        name=PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
        deps=[
            dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
            dagster.AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
            dagster.AssetKey([PHASE0_GRAPH_STATUS_ASSET_KEY]),
        ],
    )
    def graph_promotion() -> str:
        calls.append("graph_promotion")
        return "promoted"

    @dagster.asset(
        name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
    )
    def graph_snapshot(graph_promotion: str) -> str:
        return f"snapshot:{graph_promotion}"

    job = dagster.define_asset_job(
        "graph_gate_job",
        selection=dagster.AssetSelection.assets(
            PHASE0_READINESS_ASSET_KEY,
            PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
            PHASE0_GRAPH_STATUS_ASSET_KEY,
            PHASE1_GRAPH_PROMOTION_ASSET_KEY,
            PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        ),
    )
    return (
        dagster.Definitions(
            assets=[
                phase0_readiness_ping,
                candidate_freeze,
                graph_status,
                graph_promotion,
                graph_snapshot,
            ],
            asset_checks=[neo4j_graph_consistency_check],
            jobs=[job],
        ),
        calls,
    )


def _heartbeat_asset_key(dbt_phase0_assets: object) -> object:
    for asset_key in getattr(dbt_phase0_assets, "keys", ()):
        path = tuple(getattr(asset_key, "path", ()))
        if path and path[-1] == "heartbeat":
            return asset_key
    pytest.fail("dbt heartbeat model asset key was not registered")


def _phase1_asset_keys(defs: Any, phase1_group_name: str) -> set[object]:
    return {
        asset_key
        for asset_def in defs.assets or ()
        for asset_key in getattr(asset_def, "keys", ())
        if getattr(asset_def, "group_names_by_key", {}).get(asset_key)
        == phase1_group_name
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


def _dependency_keys(asset_def: object, asset_key: object) -> set[object]:
    dependency_keys: set[object] = set()
    for attribute_name in (
        "asset_deps",
        "dependency_keys_by_key",
        "deps_by_key",
    ):
        dependency_mapping = getattr(asset_def, attribute_name, None)
        if isinstance(dependency_mapping, Mapping):
            dependency_keys.update(dependency_mapping.get(asset_key, ()))
    dependency_keys.update(getattr(asset_def, "dependency_keys", ()) or ())
    return dependency_keys


def _materialized_keys(result: object) -> set[object]:
    keys: set[object] = set()
    for event in getattr(result, "all_events", ()):
        if not (
            getattr(event, "is_step_materialization", False)
            or getattr(event, "event_type_value", None) == "ASSET_MATERIALIZATION"
        ):
            continue
        asset_key = getattr(event, "asset_key", None)
        if asset_key is not None:
            keys.add(asset_key)
    return keys
