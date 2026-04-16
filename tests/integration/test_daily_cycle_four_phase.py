from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from zoneinfo import ZoneInfo

import pytest

from tests.integration.conftest import (
    asset_check_evaluations,
    asset_materialization_keys,
    materialization_order,
)

if TYPE_CHECKING:
    from orchestrator.resources import AssetFactoryProvider


def test_daily_cycle_schedule_triggers_four_phase_minimal_closure(
    dagster_module: object,
    dagster_dbt_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module
    assert dagster_dbt_module is not None

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.audit import RETROSPECTIVE_HOOK_ASSET_KEY
    from orchestrator.jobs.phase0 import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_READINESS_ASSET_KEY,
        dbt_phase0_assets,
    )
    from orchestrator.jobs.phase1 import (
        PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
    )
    from orchestrator.jobs.phase2 import PHASE2_STAGE_KEYS
    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
    )
    from orchestrator.schedules import daily_cycle_schedule

    defs = build_definitions(
        module_factories=[
            fake_data_platform_reasoner_provider(dagster),
            fake_graph_engine_provider(dagster),
            fake_main_core_provider(dagster),
            fake_audit_eval_provider(dagster),
        ],
        policy_path=stub_policy_path,
    )
    dagster.Definitions.validate_loadable(defs)

    assert daily_cycle_schedule.cron_schedule == "0 17 * * 1-5"
    assert daily_cycle_schedule.execution_timezone == "Asia/Shanghai"

    schedule_tick = _evaluate_daily_cycle_schedule(
        dagster,
        daily_cycle_schedule,
        defs,
    )
    run_requests = list(getattr(schedule_tick, "run_requests", ()) or ())

    assert len(run_requests) == 1
    assert _run_request_job_name(run_requests[0]) in {None, "daily_cycle_job"}

    result = defs.get_job_def("daily_cycle_job").execute_in_process(
        instance=dagster_instance,
        run_config=getattr(run_requests[0], "run_config", {}) or {},
        tags=getattr(run_requests[0], "tags", {}) or {},
    )

    heartbeat_key = _heartbeat_asset_key(dbt_phase0_assets)
    formal_commit_key = dagster.AssetKey([PHASE3_FORMAL_COMMIT_ASSET_KEY])
    manifest_key = dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY])
    audit_hook_key = dagster.AssetKey([RETROSPECTIVE_HOOK_ASSET_KEY])
    expected_materialized_keys = {
        dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
        heartbeat_key,
        dagster.AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
        dagster.AssetKey([PHASE1_GRAPH_PROMOTION_ASSET_KEY]),
        dagster.AssetKey([PHASE1_GRAPH_SNAPSHOT_ASSET_KEY]),
        *(dagster.AssetKey([stage]) for stage in PHASE2_STAGE_KEYS),
        formal_commit_key,
        manifest_key,
        audit_hook_key,
    }

    materialized_keys = asset_materialization_keys(result)
    materialized_order = materialization_order(result)
    evaluation_names = _check_names(asset_check_evaluations(result))

    assert result.success is True
    assert materialized_keys == expected_materialized_keys
    assert materialized_order.index(formal_commit_key) < materialized_order.index(
        manifest_key,
    )
    assert materialized_order.index(manifest_key) < materialized_order.index(
        audit_hook_key,
    )
    assert {
        "phase0_ping_check",
        "llm_health_check",
        "fake_phase1_graph_snapshot_pure_check",
        "fake_phase2_l7_pure_check",
        "fake_phase3_manifest_pure_check",
    } <= evaluation_names


def fake_data_platform_reasoner_provider(dagster: Any) -> AssetFactoryProvider:
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

    class FakeDataPlatformReasonerProvider:
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

    return FakeDataPlatformReasonerProvider()


def fake_graph_engine_provider(dagster: Any) -> AssetFactoryProvider:
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

    @dagster.asset_check(
        asset=graph_snapshot,
        name="fake_phase1_graph_snapshot_pure_check",
    )
    def fake_phase1_graph_snapshot_pure_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    class FakeGraphEngineProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (graph_promotion, graph_snapshot)

        def get_checks(self) -> tuple[object, ...]:
            return (fake_phase1_graph_snapshot_pure_check,)

        def get_resources(self) -> dict[str, object]:
            return {}

    return FakeGraphEngineProvider()


def fake_main_core_provider(dagster: Any) -> AssetFactoryProvider:
    from orchestrator.checks import (
        PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
        Phase2PoolFailureRateEvent,
    )
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME, PHASE2_STAGE_KEYS
    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_GROUP_NAME,
        PHASE3_MANIFEST_ASSET_KEY,
    )

    @dagster.asset(name=PHASE2_STAGE_KEYS[0], group_name=PHASE2_GROUP_NAME)
    def phase2_l1(graph_snapshot: str) -> str:
        return f"{graph_snapshot}:l1"

    @dagster.asset(name=PHASE2_STAGE_KEYS[1], group_name=PHASE2_GROUP_NAME)
    def phase2_l2(l1: str) -> str:
        return f"{l1}:l2"

    @dagster.asset(name=PHASE2_STAGE_KEYS[2], group_name=PHASE2_GROUP_NAME)
    def phase2_l3(l2: str) -> str:
        return f"{l2}:l3"

    @dagster.asset(name=PHASE2_STAGE_KEYS[3], group_name=PHASE2_GROUP_NAME)
    def phase2_l4(l3: str) -> str:
        return f"{l3}:l4"

    @dagster.asset(name=PHASE2_STAGE_KEYS[4], group_name=PHASE2_GROUP_NAME)
    def phase2_l5(l4: str) -> str:
        return f"{l4}:l5"

    @dagster.asset(name=PHASE2_STAGE_KEYS[5], group_name=PHASE2_GROUP_NAME)
    def phase2_l6(l5: str) -> str:
        return f"{l5}:l6"

    @dagster.asset(name=PHASE2_STAGE_KEYS[6], group_name=PHASE2_GROUP_NAME)
    def phase2_l7(l6: str) -> str:
        return f"{l6}:l7"

    @dagster.asset_check(asset=phase2_l7, name="fake_phase2_l7_pure_check")
    def fake_phase2_l7_pure_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    @dagster.asset(
        name=PHASE3_FORMAL_COMMIT_ASSET_KEY,
        group_name=PHASE3_GROUP_NAME,
    )
    def formal_objects_commit(l7: str) -> str:
        return f"{l7}:formal"

    @dagster.asset(
        name=PHASE3_MANIFEST_ASSET_KEY,
        group_name=PHASE3_GROUP_NAME,
    )
    def cycle_publish_manifest(formal_objects_commit: str) -> str:
        return f"{formal_objects_commit}:manifest"

    @dagster.asset_check(
        asset=cycle_publish_manifest,
        name="fake_phase3_manifest_pure_check",
    )
    def fake_phase3_manifest_pure_check() -> object:
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

    class FakePhase2PoolFailureRateResource(dagster.ConfigurableResource):
        def get_phase2_pool_failure_rate_event(
            self,
        ) -> Phase2PoolFailureRateEvent:
            return Phase2PoolFailureRateEvent(
                failed_count=0,
                total_count=10,
                failed_nodes=(),
            )

    class FakeMainCoreProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (
                *phase2_assets,
                formal_objects_commit,
                cycle_publish_manifest,
            )

        def get_checks(self) -> tuple[object, ...]:
            return (
                fake_phase2_l7_pure_check,
                fake_phase3_manifest_pure_check,
            )

        def get_resources(self) -> dict[str, object]:
            return {
                PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: (
                    FakePhase2PoolFailureRateResource()
                ),
            }

    return FakeMainCoreProvider()


def fake_audit_eval_provider(dagster: Any) -> AssetFactoryProvider:
    from orchestrator.jobs.audit import (
        AUDIT_EVAL_GROUP_NAME,
        RETROSPECTIVE_HOOK_ASSET_KEY,
    )

    @dagster.asset(
        name=RETROSPECTIVE_HOOK_ASSET_KEY,
        group_name=AUDIT_EVAL_GROUP_NAME,
    )
    def retrospective_hook(cycle_publish_manifest: str) -> str:
        return f"{cycle_publish_manifest}:retrospective"

    @dagster.asset_check(asset=retrospective_hook, name="fake_audit_eval_check")
    def fake_audit_eval_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    class FakeAuditEvalProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (retrospective_hook,)

        def get_checks(self) -> tuple[object, ...]:
            return (fake_audit_eval_check,)

        def get_resources(self) -> dict[str, object]:
            return {}

    return FakeAuditEvalProvider()


def _evaluate_daily_cycle_schedule(
    dagster: Any,
    daily_cycle_schedule: object,
    defs: object,
) -> object:
    scheduled_time = datetime(
        2026,
        4,
        16,
        17,
        0,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    context = _build_schedule_context(dagster, defs, scheduled_time)

    if callable(getattr(context, "__enter__", None)):
        with context as active_context:
            return daily_cycle_schedule.evaluate_tick(active_context)

    return daily_cycle_schedule.evaluate_tick(context)


def _build_schedule_context(
    dagster: Any,
    defs: object,
    scheduled_time: datetime,
) -> object:
    build_schedule_context = getattr(dagster, "build_schedule_context", None)
    if build_schedule_context is None:
        pytest.fail("dagster.build_schedule_context is required for schedule E2E")

    last_error: TypeError | None = None
    for kwargs in (
        {
            "definitions": defs,
            "scheduled_execution_time": scheduled_time,
        },
        {"scheduled_execution_time": scheduled_time},
        {},
    ):
        try:
            return build_schedule_context(**kwargs)
        except TypeError as exc:
            last_error = exc

    pytest.fail(f"could not build Dagster schedule context: {last_error}")


def _heartbeat_asset_key(dbt_phase0_assets: object) -> object:
    for asset_key in getattr(dbt_phase0_assets, "keys", ()):
        path = tuple(getattr(asset_key, "path", ()))
        if path and path[-1] == "heartbeat":
            return asset_key
    pytest.fail("dbt heartbeat model asset key was not registered")


def _check_names(evaluations: list[object]) -> set[str]:
    return {
        check_name
        for evaluation in evaluations
        if (check_name := _check_name(evaluation)) is not None
    }


def _check_name(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str):
        return check_name
    check_key = getattr(evaluation, "check_key", None)
    name = getattr(check_key, "name", None)
    return name if isinstance(name, str) else None


def _run_request_job_name(run_request: object) -> str | None:
    job_name = getattr(run_request, "job_name", None)
    return job_name if isinstance(job_name, str) else None
