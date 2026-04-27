from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pytest

from tests.integration.conftest import asset_materialization_keys


def test_phase3_publish_assets_materialize_after_formal_commit(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_GROUP_NAME,
        PHASE3_MANIFEST_ASSET_KEY,
    )

    phase3_calls: list[str] = []
    phase3_provider = _fake_phase3_provider(dagster, phase3_calls)
    defs = build_definitions(
        module_factories=[
            _fake_phase0_surface_provider(dagster),
            _fake_phase1_provider(dagster),
            _fake_phase2_provider(dagster),
            phase3_provider,
        ],
        policy_path=stub_policy_path,
    )

    dagster.Definitions.validate_loadable(defs)

    formal_commit_key = dagster.AssetKey([PHASE3_FORMAL_COMMIT_ASSET_KEY])
    manifest_key = dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY])
    selected_keys = daily_cycle_job.selection.resolve(defs.assets or ())
    manifest_def = _asset_def_for_key(defs.assets or (), manifest_key)

    assert _asset_keys_for_group(defs, PHASE3_GROUP_NAME) == {
        formal_commit_key,
        manifest_key,
    }
    assert formal_commit_key in selected_keys
    assert manifest_key in selected_keys
    assert formal_commit_key in _asset_dependency_keys(manifest_def, manifest_key)

    result = defs.get_job_def("daily_cycle_job").execute_in_process(
        instance=dagster_instance,
        tags={"cycle_id": "cycle-20260416"},
    )
    materialized_keys = asset_materialization_keys(result)

    assert result.success is True
    assert formal_commit_key in materialized_keys
    assert manifest_key in materialized_keys
    assert phase3_calls == [
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
    ]


def test_phase3_commit_failure_fails_run_without_manifest_materialization(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
    )

    phase3_calls: list[str] = []
    defs = build_definitions(
        module_factories=[
            _fake_phase0_surface_provider(dagster),
            _fake_phase1_provider(dagster),
            _fake_phase2_provider(dagster),
            _fake_phase3_provider(dagster, phase3_calls, fail_commit=True),
        ],
        policy_path=stub_policy_path,
    )
    dagster.Definitions.validate_loadable(defs)

    result = defs.get_job_def("daily_cycle_job").execute_in_process(
        instance=dagster_instance,
        raise_on_error=False,
        tags={"cycle_id": "cycle-20260416"},
    )
    materialized_keys = asset_materialization_keys(result)

    assert result.success is False
    assert dagster.AssetKey([PHASE3_FORMAL_COMMIT_ASSET_KEY]) not in materialized_keys
    assert dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY]) not in materialized_keys
    assert phase3_calls == [PHASE3_FORMAL_COMMIT_ASSET_KEY]


def test_phase3_manifest_without_formal_commit_dependency_is_rejected(
    dagster_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    with pytest.raises(
        ValueError,
        match="cycle_publish_manifest.*formal_objects_commit",
    ):
        build_definitions(
            module_factories=[
                _fake_phase3_provider(
                    dagster,
                    phase3_calls=[],
                    manifest_depends_on_commit=False,
                ),
            ],
            policy_path=stub_policy_path,
        )


def test_phase3_formal_commit_without_phase2_dependency_is_rejected(
    dagster_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    with pytest.raises(
        ValueError,
        match="formal_objects_commit.*Phase 2.*l8",
    ):
        build_definitions(
            module_factories=[
                _fake_phase0_surface_provider(dagster),
                _fake_phase1_provider(dagster),
                _fake_phase2_provider(dagster),
                _fake_phase3_provider(
                    dagster,
                    phase3_calls=[],
                    commit_depends_on_phase2=False,
                ),
            ],
            policy_path=stub_policy_path,
        )


def test_phase3_provider_requires_publish_manifest_asset(
    dagster_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    with pytest.raises(ValueError, match="cycle_publish_manifest"):
        build_definitions(
            module_factories=[
                _fake_phase3_provider(
                    dagster,
                    phase3_calls=[],
                    include_manifest=False,
                ),
            ],
            policy_path=stub_policy_path,
        )


@pytest.mark.parametrize("profile", ["milestone-2", "p2"])
def test_milestone2_profiles_require_phase3_publish_surface(
    dagster_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    monkeypatch: pytest.MonkeyPatch,
    profile: str,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    monkeypatch.setenv("ORCHESTRATOR_DEFINITIONS_PROFILE", profile)

    with pytest.raises(
        ValueError,
        match="phase3.*formal_objects_commit.*cycle_publish_manifest",
    ):
        build_definitions(
            module_factories=[
                _fake_phase0_surface_provider(dagster),
                _fake_phase1_provider(dagster),
                _fake_phase2_provider(dagster),
            ],
            policy_path=stub_policy_path,
        )


def _fake_phase0_surface_provider(dagster: Any) -> object:
    from orchestrator.checks import DataReadinessSignal
    from orchestrator.jobs.phase0_constants import (
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

    @dagster.asset(
        name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        group_name=PHASE0_GROUP_NAME,
    )
    def candidate_freeze() -> str:
        return "frozen"

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
                DATA_READINESS_RESOURCE_KEY: FakeDataReadinessResource(),
                "llm_health_probe": FakeLLMHealthProbeResource(),
            }

    return FakePhase0SurfaceProvider()


def _fake_phase1_provider(dagster: Any) -> object:
    from orchestrator.jobs.phase0_constants import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GRAPH_STATUS_ASSET_KEY,
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
            dagster.AssetKey([PHASE0_GRAPH_STATUS_ASSET_KEY]),
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


def _fake_phase2_provider(dagster: Any) -> object:
    from orchestrator.checks import (
        PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
        Phase2PoolFailureRateEvent,
    )
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME, PHASE2_STAGE_KEYS

    @dagster.asset(name=PHASE2_STAGE_KEYS[-1], group_name=PHASE2_GROUP_NAME)
    def phase2_l8(graph_snapshot: str) -> str:
        return f"{graph_snapshot}:l8"

    class FakePhase2PoolFailureRateResource(dagster.ConfigurableResource):
        def get_phase2_pool_failure_rate_event(
            self,
        ) -> Phase2PoolFailureRateEvent:
            return Phase2PoolFailureRateEvent(
                failed_count=0,
                total_count=10,
                failed_nodes=(),
            )

    class FakePhase2Provider:
        def get_assets(self) -> tuple[object, ...]:
            return (phase2_l8,)

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {
                PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: (
                    FakePhase2PoolFailureRateResource()
                ),
            }

    return FakePhase2Provider()


def _fake_phase3_provider(
    dagster: Any,
    phase3_calls: list[str],
    *,
    fail_commit: bool = False,
    include_manifest: bool = True,
    commit_depends_on_phase2: bool = True,
    manifest_depends_on_commit: bool = True,
) -> object:
    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_GROUP_NAME,
        PHASE3_MANIFEST_ASSET_KEY,
    )

    if commit_depends_on_phase2:

        @dagster.asset(
            name=PHASE3_FORMAL_COMMIT_ASSET_KEY,
            group_name=PHASE3_GROUP_NAME,
        )
        def formal_objects_commit(l8: str) -> str:
            assert l8
            phase3_calls.append(PHASE3_FORMAL_COMMIT_ASSET_KEY)
            if fail_commit:
                raise RuntimeError("fake formal commit failed")
            return "formal-commit-ok"

    else:

        @dagster.asset(
            name=PHASE3_FORMAL_COMMIT_ASSET_KEY,
            group_name=PHASE3_GROUP_NAME,
        )
        def formal_objects_commit() -> str:
            phase3_calls.append(PHASE3_FORMAL_COMMIT_ASSET_KEY)
            if fail_commit:
                raise RuntimeError("fake formal commit failed")
            return "formal-commit-ok"

    if include_manifest and manifest_depends_on_commit:

        @dagster.asset(
            name=PHASE3_MANIFEST_ASSET_KEY,
            group_name=PHASE3_GROUP_NAME,
        )
        def cycle_publish_manifest(formal_objects_commit: str) -> str:
            assert formal_objects_commit
            phase3_calls.append(PHASE3_MANIFEST_ASSET_KEY)
            return "manifest-ok"

    elif include_manifest:

        @dagster.asset(
            name=PHASE3_MANIFEST_ASSET_KEY,
            group_name=PHASE3_GROUP_NAME,
        )
        def cycle_publish_manifest() -> str:
            phase3_calls.append(PHASE3_MANIFEST_ASSET_KEY)
            return "manifest-ok"

    phase3_assets = [formal_objects_commit]
    if include_manifest:
        phase3_assets.append(cycle_publish_manifest)

    class FakePhase3Provider:
        def get_assets(self) -> tuple[object, ...]:
            return tuple(phase3_assets)

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {}

    return FakePhase3Provider()


def _asset_keys_for_group(defs: Any, group_name: str) -> set[object]:
    return {
        asset_key
        for asset_def in defs.assets or ()
        for asset_key in getattr(asset_def, "keys", ())
        if getattr(asset_def, "group_names_by_key", {}).get(asset_key) == group_name
    }


def _asset_def_for_key(asset_defs: Iterable[object], asset_key: object) -> object:
    for asset_def in asset_defs:
        if asset_key in getattr(asset_def, "keys", ()):
            return asset_def
    pytest.fail(f"missing asset definition for {asset_key}")


def _asset_dependency_keys(asset_def: object, asset_key: object) -> set[object]:
    dependency_keys: set[object] = set()
    for attribute_name in (
        "asset_deps",
        "dependency_keys_by_key",
        "deps_by_key",
    ):
        dependency_mapping = getattr(asset_def, attribute_name, None)
        if isinstance(dependency_mapping, Mapping):
            dependency_keys.update(dependency_mapping.get(asset_key, ()))

    raw_dependency_keys = getattr(asset_def, "dependency_keys", ())
    if raw_dependency_keys:
        dependency_keys.update(raw_dependency_keys)

    return dependency_keys
