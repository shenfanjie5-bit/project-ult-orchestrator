from __future__ import annotations

import json
import logging
import shutil
from importlib.util import module_from_spec, spec_from_file_location
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from types import ModuleType
from pathlib import Path
from typing import Any

import pytest

from orchestrator.policy import (
    FailureClass,
    GateAction,
    PhaseEnum,
    load_gate_policy,
)
from tests.integration.conftest import (
    _clear_phase0_imports,
    asset_check_evaluations,
    asset_materialization_keys,
    metadata_value,
)


def _load_job_constants_module(module_name: str, relative_path: str) -> ModuleType:
    spec = spec_from_file_location(
        f"_orchestrator_test_constants_{module_name}",
        _REPO_ROOT / relative_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load job constants module {relative_path}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_REPO_ROOT = Path(__file__).resolve().parents[2]
_PHASE0_CONSTANTS = _load_job_constants_module(
    "phase0_constants",
    "src/orchestrator/jobs/phase0_constants.py",
)
_PHASE1_CONSTANTS = _load_job_constants_module(
    "phase1",
    "src/orchestrator/jobs/phase1.py",
)
_PHASE2_CONSTANTS = _load_job_constants_module(
    "phase2",
    "src/orchestrator/jobs/phase2.py",
)
_PHASE3_CONSTANTS = _load_job_constants_module(
    "phase3",
    "src/orchestrator/jobs/phase3.py",
)

PHASE0_CANDIDATE_FREEZE_ASSET_KEY = str(
    _PHASE0_CONSTANTS.PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
)
PHASE0_GRAPH_CONSISTENCY_CHECK_NAME = str(
    _PHASE0_CONSTANTS.PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
)
PHASE0_GRAPH_STATUS_ASSET_KEY = str(_PHASE0_CONSTANTS.PHASE0_GRAPH_STATUS_ASSET_KEY)
PHASE0_GROUP_NAME = str(_PHASE0_CONSTANTS.PHASE0_GROUP_NAME)
PHASE0_READINESS_ASSET_KEY = str(_PHASE0_CONSTANTS.PHASE0_READINESS_ASSET_KEY)
PHASE1_GRAPH_PROMOTION_ASSET_KEY = str(
    _PHASE1_CONSTANTS.PHASE1_GRAPH_PROMOTION_ASSET_KEY,
)
PHASE1_GRAPH_SNAPSHOT_ASSET_KEY = str(
    _PHASE1_CONSTANTS.PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
)
PHASE1_GROUP_NAME = str(_PHASE1_CONSTANTS.PHASE1_GROUP_NAME)
PHASE2_GROUP_NAME = str(_PHASE2_CONSTANTS.PHASE2_GROUP_NAME)
PHASE2_STAGE_KEYS = tuple(str(key) for key in _PHASE2_CONSTANTS.PHASE2_STAGE_KEYS)
PHASE3_FORMAL_COMMIT_ASSET_KEY = str(
    _PHASE3_CONSTANTS.PHASE3_FORMAL_COMMIT_ASSET_KEY,
)
PHASE3_GROUP_NAME = str(_PHASE3_CONSTANTS.PHASE3_GROUP_NAME)
PHASE3_MANIFEST_ASSET_KEY = str(_PHASE3_CONSTANTS.PHASE3_MANIFEST_ASSET_KEY)

BuildFailureInjectionCase = Callable[..., "FailureInjectionOutcome"]

PHASE2_DOWNSTREAM_PUBLISH_ASSET_KEY = "phase2_downstream_publish"
PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY = "phase2_stock_AAPL"
PHASE2_SINGLE_STOCK_MSFT_ASSET_KEY = "phase2_stock_MSFT"
READY_GRAPH_MARKER_ASSET_KEY = "ready_graph_marker"
REPAIR_MANIFEST_ASSET_KEY = "repair_cycle_publish_manifest"


@dataclass(frozen=True, slots=True)
class FailureInjectionCase:
    scenario_id: str
    phase: str
    expected_action: str
    expected_failure_class: str
    expected_success: bool
    expected_failed_node: str | None
    build: BuildFailureInjectionCase


@dataclass(frozen=True, slots=True)
class FailureInjectionOutcome:
    action: str
    failure_class: str
    success: bool
    failed_node: str | None
    result: object | None = None
    sensor_result: object | None = None
    alert_payloads: tuple[dict[str, object], ...] = ()
    materialized_keys: frozenset[object] = frozenset()
    evaluations: tuple[object, ...] = ()
    observations: tuple[object, ...] = ()
    rerun_requests: tuple[dict[str, object], ...] = ()
    run_request: object | None = None
    extras: Mapping[str, object] = field(default_factory=dict)


class _GatePolicyResource:
    def __init__(self, policy: object) -> None:
        self.policy = policy


class _ReadinessResource:
    def __init__(self, signal: object) -> None:
        self._signal = signal

    def get_data_readiness_signal(self) -> object:
        return self._signal


class _UnhealthyProviderHealthStatus:
    provider = "fake-llm"
    model = "critical-model"
    reachable = False
    latency_ms = None
    quota_status = "unavailable"
    error = "provider unavailable"


class _UnhealthyLLMHealthReport:
    provider_statuses = (_UnhealthyProviderHealthStatus(),)
    all_critical_targets_available = False
    summary = "provider unavailable"


class _UnhealthyLLMHealthProbe:
    def check_health(self) -> _UnhealthyLLMHealthReport:
        return _UnhealthyLLMHealthReport()


def gate_matrix_failure_cases(
    dagster: Any,
    *,
    stub_policy_path: str,
    tmp_path: Path,
    tmp_dbt_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[FailureInjectionCase, ...]:
    return (
        FailureInjectionCase(
            scenario_id="phase0_data_readiness_delayed",
            phase="phase0",
            expected_action="fail_run",
            expected_failure_class="data_quality",
            expected_success=False,
            expected_failed_node=PHASE0_READINESS_ASSET_KEY,
            build=lambda **kwargs: _run_phase0_readiness_failure(
                dagster,
                stub_policy_path=stub_policy_path,
                **kwargs,
            ),
        ),
        FailureInjectionCase(
            scenario_id="phase0_llm_health_check_failed",
            phase="phase0",
            expected_action="fail_run",
            expected_failure_class="infra",
            expected_success=False,
            expected_failed_node="llm_health_check",
            build=lambda **kwargs: _run_phase0_llm_health_failure(
                dagster,
                stub_policy_path=stub_policy_path,
                **kwargs,
            ),
        ),
        FailureInjectionCase(
            scenario_id="phase0_dbt_test_failed",
            phase="phase0",
            expected_action="partial_rerun",
            expected_failure_class="task_level",
            expected_success=False,
            expected_failed_node=None,
            build=lambda **kwargs: _run_phase0_dbt_failure(
                dagster,
                stub_policy_path=stub_policy_path,
                tmp_path=tmp_path,
                tmp_dbt_project=tmp_dbt_project,
                monkeypatch=monkeypatch,
                **kwargs,
            ),
        ),
        FailureInjectionCase(
            scenario_id="phase1_graph_promotion_snapshot_failed",
            phase="phase1",
            expected_action="fail_run",
            expected_failure_class="publish",
            expected_success=False,
            expected_failed_node=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
            build=lambda **kwargs: _run_phase1_graph_snapshot_failure(
                dagster,
                stub_policy_path=stub_policy_path,
                **kwargs,
            ),
        ),
        FailureInjectionCase(
            scenario_id="phase2_single_stock_task_failed",
            phase="phase2",
            expected_action="mark_inconclusive",
            expected_failure_class="task_level",
            expected_success=True,
            expected_failed_node=PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY,
            build=lambda **kwargs: _run_phase2_single_stock_failure(
                dagster,
                stub_policy_path=stub_policy_path,
                **kwargs,
            ),
        ),
        FailureInjectionCase(
            scenario_id="phase2_pool_failure_rate_exceeded",
            phase="phase2",
            expected_action="fail_run",
            expected_failure_class="data_quality",
            expected_success=False,
            expected_failed_node=(
                f"{PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY}, "
                f"{PHASE2_SINGLE_STOCK_MSFT_ASSET_KEY}"
            ),
            build=lambda **kwargs: _run_phase2_pool_failure_rate_exceeded(
                dagster,
                stub_policy_path=stub_policy_path,
                **kwargs,
            ),
        ),
        FailureInjectionCase(
            scenario_id="phase3_formal_commit_failed",
            phase="phase3",
            expected_action="fail_run",
            expected_failure_class="publish",
            expected_success=False,
            expected_failed_node=PHASE3_FORMAL_COMMIT_ASSET_KEY,
            build=lambda **kwargs: _run_phase3_formal_commit_failure(
                dagster,
                stub_policy_path=stub_policy_path,
                **kwargs,
            ),
        ),
        FailureInjectionCase(
            scenario_id="phase3_manifest_write_failed",
            phase="phase3",
            expected_action="repair_manifest",
            expected_failure_class="infra",
            expected_success=False,
            expected_failed_node=PHASE3_MANIFEST_ASSET_KEY,
            build=lambda **kwargs: _run_phase3_manifest_repair_failure(
                dagster,
                stub_policy_path=stub_policy_path,
                tmp_path=tmp_path,
                monkeypatch=monkeypatch,
                **kwargs,
            ),
        ),
        FailureInjectionCase(
            scenario_id="infra_unavailable_hard_stop",
            phase="phase2",
            expected_action="fail_run",
            expected_failure_class="infra",
            expected_success=False,
            expected_failed_node="phase2_pool_failure_rate",
            build=lambda **kwargs: _run_phase2_infra_unavailable_failure(
                dagster,
                stub_policy_path=stub_policy_path,
                monkeypatch=monkeypatch,
                **kwargs,
            ),
        ),
    )


def fake_phase0_surface_provider(dagster: Any) -> object:
    from orchestrator.checks import DataReadinessSignal
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
        deps=[dagster.AssetKey([PHASE0_READINESS_ASSET_KEY])],
    )
    def candidate_freeze() -> str:
        return "frozen"

    @dagster.asset(
        name=PHASE0_GRAPH_STATUS_ASSET_KEY,
        group_name=PHASE0_GROUP_NAME,
    )
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


def fake_phase1_provider(dagster: Any) -> object:
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


def fake_phase2_provider(
    dagster: Any,
    *,
    failed_count: int = 0,
    total_count: int = 10,
    failed_nodes: tuple[str, ...] = (),
    failing_resource: bool = False,
) -> object:
    from orchestrator.checks import (
        PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
        Phase2PoolFailureRateEvent,
    )

    phase2_assets = _phase2_stage_assets(dagster)

    class RaisingPoolFailureRateResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> object:
            raise RuntimeError(
                f"fake {PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY} unavailable",
            )

    class FakePhase2PoolFailureRateResource(dagster.ConfigurableResource):
        def get_phase2_pool_failure_rate_event(
            self,
        ) -> Phase2PoolFailureRateEvent:
            return Phase2PoolFailureRateEvent(
                failed_count=failed_count,
                total_count=total_count,
                failed_nodes=failed_nodes,
                reason=(
                    "fake pool failures"
                    if failed_count
                    else "healthy fake pool"
                ),
            )

    class FakePhase2Provider:
        def get_assets(self) -> tuple[object, ...]:
            return phase2_assets

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            if failing_resource:
                return {
                    PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: (
                        RaisingPoolFailureRateResource()
                    ),
                }
            return {
                PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: (
                    FakePhase2PoolFailureRateResource()
                ),
            }

    return FakePhase2Provider()


def _run_phase0_readiness_failure(
    dagster: Any,
    *,
    stub_policy_path: str,
    caplog: pytest.LogCaptureFixture,
    **_: object,
) -> FailureInjectionOutcome:
    from orchestrator.checks import DataReadinessSignal
    from orchestrator.checks.classifier import classify_gate_result
    from orchestrator.sensors.data_readiness import data_readiness_sensor

    policy = load_gate_policy(stub_policy_path)
    signal = DataReadinessSignal(
        ready=False,
        cycle_id="cycle-20260416",
        reason="market data delayed",
        failed_node=PHASE0_READINESS_ASSET_KEY,
    )
    decision = classify_gate_result(
        PhaseEnum.PHASE0,
        {"failure_class": FailureClass.DATA_QUALITY},
        policy,
    )
    context = dagster.build_sensor_context(
        resources={
            "data_readiness": _ReadinessResource(signal),
            "gate_policy": _GatePolicyResource(policy),
        },
    )

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        sensor_result = data_readiness_sensor.evaluation_fn(context)

    return FailureInjectionOutcome(
        action=decision.action.value,
        failure_class=decision.failure_class.value,
        success=False,
        failed_node=signal.failed_node,
        sensor_result=sensor_result,
        alert_payloads=tuple(alert_payloads(caplog.records)),
    )


def _run_phase0_llm_health_failure(
    dagster: Any,
    *,
    stub_policy_path: str,
    dagster_instance: object,
    caplog: pytest.LogCaptureFixture,
    **_: object,
) -> FailureInjectionOutcome:
    from orchestrator.checks.asset_checks import llm_health_check
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.jobs.phase0 import phase0_readiness_ping

    @dagster.asset(name="phase1_trigger_marker", group_name=PHASE1_GROUP_NAME)
    def phase1_trigger_marker(phase0_readiness_ping: str) -> str:
        return f"triggered after {phase0_readiness_ping}"

    defs = dagster.Definitions(
        assets=[phase0_readiness_ping, phase1_trigger_marker],
        asset_checks=[llm_health_check],
        jobs=[daily_cycle_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
            "llm_health_probe": _UnhealthyLLMHealthProbe(),
        },
    )

    result = _execute_daily_cycle(defs, dagster_instance, caplog)
    evaluation = single_evaluation(
        asset_check_evaluations(result),
        "llm_health_check",
    )

    return FailureInjectionOutcome(
        action=str(metadata_value(evaluation, "action")),
        failure_class=str(metadata_value(evaluation, "failure_class")),
        success=bool(getattr(result, "success", False)),
        failed_node="llm_health_check",
        result=result,
        alert_payloads=tuple(alert_payloads(caplog.records)),
        materialized_keys=frozenset(asset_materialization_keys(result)),
        evaluations=(evaluation,),
        extras={"blocked_asset_keys": ("phase1_trigger_marker",)},
    )


def _run_phase0_dbt_failure(
    dagster: Any,
    *,
    stub_policy_path: str,
    tmp_path: Path,
    tmp_dbt_project: Path,
    monkeypatch: pytest.MonkeyPatch,
    dagster_instance: object,
    caplog: pytest.LogCaptureFixture,
    **_: object,
) -> FailureInjectionOutcome:
    from orchestrator.checks.dbt_events import plan_dbt_test_partial_rerun
    from orchestrator.rerun_request import request_from_plan

    failing_project = tmp_path / "phase0_dbt_test_failed_project"
    request_dir = tmp_path / "phase0_dbt_test_failed_requests"
    shutil.copytree(tmp_dbt_project, failing_project)
    (failing_project / "models" / "phase0" / "heartbeat.sql").write_text(
        "select null as heartbeat\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ORCHESTRATOR_DBT_PROJECT_DIR", str(failing_project))
    monkeypatch.setenv("ORCHESTRATOR_RERUN_REQUEST_DIR", str(request_dir))
    _clear_phase0_imports()

    try:
        from dagster_dbt import DbtCliResource
        from orchestrator.checks.resources import GatePolicyResource
        from orchestrator.jobs.cycle import daily_cycle_job
        from orchestrator.jobs.phase0 import (
            DBT_PROFILES_DIR,
            DBT_PROJECT_DIR,
            dbt_phase0_assets,
        )

        failed_asset_key = heartbeat_asset_key(dbt_phase0_assets)
        defs = dagster.Definitions(
            assets=[dbt_phase0_assets],
            jobs=[daily_cycle_job],
            resources={
                "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
                "dbt": DbtCliResource(
                    project_dir=str(DBT_PROJECT_DIR),
                    profiles_dir=str(DBT_PROFILES_DIR),
                ),
            },
        )
        result = _execute_daily_cycle(defs, dagster_instance, caplog)
    finally:
        _clear_phase0_imports()

    failed_node = failed_asset_key.to_user_string()
    observations = tuple(asset_observations(result))
    gate_observation = gate_observation_for_asset(observations, failed_node)
    request_payloads = tuple(rerun_request_payloads(request_dir))
    event = {
        "asset_key": failed_asset_key,
        "metadata": {"node_info": {"node_name": "not_null_heartbeat_heartbeat"}},
    }
    plan = plan_dbt_test_partial_rerun(
        str(getattr(result, "run_id")),
        failed_asset_key,
        event,
        load_gate_policy(stub_policy_path),
    )

    return FailureInjectionOutcome(
        action=str(metadata_value(gate_observation, "action")),
        failure_class=str(metadata_value(gate_observation, "failure_class")),
        success=bool(getattr(result, "success", False)),
        failed_node=failed_node,
        result=result,
        alert_payloads=tuple(alert_payloads(caplog.records)),
        materialized_keys=frozenset(asset_materialization_keys(result)),
        observations=observations,
        rerun_requests=request_payloads,
        extras={
            "gate_observation": gate_observation,
            "expected_request": expected_request_from_plan(
                plan,
                request_payloads,
                request_from_plan=request_from_plan,
            ),
        },
    )


def _run_phase1_graph_snapshot_failure(
    dagster: Any,
    *,
    stub_policy_path: str,
    dagster_instance: object,
    caplog: pytest.LogCaptureFixture,
    **_: object,
) -> FailureInjectionOutcome:
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.cycle import daily_cycle_job

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
        return f"{candidate_freeze}:ready"

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
        raise RuntimeError(f"snapshot writer failed after {graph_promotion}")

    @dagster.asset(name=READY_GRAPH_MARKER_ASSET_KEY, group_name=PHASE1_GROUP_NAME)
    def ready_graph_marker(graph_snapshot: str) -> str:
        return graph_snapshot

    defs = dagster.Definitions(
        assets=[
            phase0_readiness_ping,
            candidate_freeze,
            graph_status,
            graph_promotion,
            graph_snapshot,
            ready_graph_marker,
        ],
        jobs=[daily_cycle_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )

    result = _execute_daily_cycle(
        defs,
        dagster_instance,
        caplog,
        tags={"cycle_id": "cycle-20260416", "snapshot_id": "snapshot-20260416"},
    )
    alert = last_alert_payload(caplog.records)

    return FailureInjectionOutcome(
        action=str(alert["action"]),
        failure_class=str(alert["failure_class"]),
        success=bool(getattr(result, "success", False)),
        failed_node=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        result=result,
        alert_payloads=tuple(alert_payloads(caplog.records)),
        materialized_keys=frozenset(asset_materialization_keys(result)),
        extras={
            "required_materialized_asset_keys": (
                PHASE1_GRAPH_PROMOTION_ASSET_KEY,
            ),
            "blocked_asset_keys": (
                PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
                READY_GRAPH_MARKER_ASSET_KEY,
            ),
        },
    )


def _run_phase2_single_stock_failure(
    dagster: Any,
    *,
    stub_policy_path: str,
    dagster_instance: object,
    caplog: pytest.LogCaptureFixture,
    **_: object,
) -> FailureInjectionOutcome:
    from orchestrator.checks import (
        Phase2SingleStockFailureEvent,
        classify_phase2_single_stock_failure,
        dispatch_gate_decision_alert,
        inconclusive_metadata,
    )
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.cycle import daily_cycle_job

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
        return f"{candidate_freeze}:ready"

    @dagster.asset(
        name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
        deps=[
            dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
            dagster.AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
            dagster.AssetKey([PHASE0_GRAPH_STATUS_ASSET_KEY]),
        ],
    )
    def graph_snapshot() -> str:
        return "snapshot"

    @dagster.asset(
        name=PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY,
        group_name=PHASE2_GROUP_NAME,
        deps=[dagster.AssetKey([PHASE1_GRAPH_SNAPSHOT_ASSET_KEY])],
    )
    def phase2_stock_aapl() -> str:
        return "aapl placeholder"

    @dagster.asset(
        name=PHASE2_SINGLE_STOCK_MSFT_ASSET_KEY,
        group_name=PHASE2_GROUP_NAME,
        deps=[dagster.AssetKey([PHASE1_GRAPH_SNAPSHOT_ASSET_KEY])],
    )
    def phase2_stock_msft() -> str:
        return "msft placeholder"

    @dagster.asset_check(
        asset=phase2_stock_aapl,
        name="phase2_aapl_single_stock_gate",
        blocking=False,
    )
    def phase2_aapl_single_stock_gate(
        context: object,
        gate_policy: GatePolicyResource,
    ) -> object:
        event = Phase2SingleStockFailureEvent(
            stock_id="AAPL",
            failed_node=PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY,
            failed_count=1,
            total_count=10,
            reason="fake LLM task failed",
        )
        decision = classify_phase2_single_stock_failure(event, gate_policy.policy)
        dispatch_gate_decision_alert(
            decision,
            cycle_id=cycle_id_from_context(context),
            failed_node=event.failed_node,
            summary=event.reason or decision.reason or "single stock failed",
            channels=gate_policy.policy.alert_channels,
        )
        return dagster.AssetCheckResult(
            passed=decision.action is GateAction.CONTINUE,
            metadata=inconclusive_metadata(event, decision),
        )

    defs = dagster.Definitions(
        assets=[
            phase0_readiness_ping,
            candidate_freeze,
            graph_status,
            graph_snapshot,
            phase2_stock_aapl,
            phase2_stock_msft,
        ],
        asset_checks=[phase2_aapl_single_stock_gate],
        jobs=[daily_cycle_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )

    result = _execute_daily_cycle(defs, dagster_instance, caplog)
    evaluation = single_evaluation(
        asset_check_evaluations(result),
        "phase2_aapl_single_stock_gate",
    )

    return FailureInjectionOutcome(
        action=str(metadata_value(evaluation, "action")),
        failure_class=str(metadata_value(evaluation, "failure_class")),
        success=bool(getattr(result, "success", False)),
        failed_node=PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY,
        result=result,
        alert_payloads=tuple(alert_payloads(caplog.records)),
        materialized_keys=frozenset(asset_materialization_keys(result)),
        evaluations=(evaluation,),
        extras={
            "required_materialized_asset_keys": (
                PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY,
                PHASE2_SINGLE_STOCK_MSFT_ASSET_KEY,
            ),
        },
    )


def _run_phase2_pool_failure_rate_exceeded(
    dagster: Any,
    *,
    stub_policy_path: str,
    dagster_instance: object,
    caplog: pytest.LogCaptureFixture,
    **_: object,
) -> FailureInjectionOutcome:
    from orchestrator.checks import (
        PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
        build_phase2_pool_failure_rate_check,
    )
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.cycle import daily_cycle_job

    phase0_assets = _phase0_direct_assets(dagster)
    phase1_assets = _phase1_snapshot_direct_assets(dagster)
    phase2_assets = _phase2_stage_assets(dagster)
    downstream_publish = _phase2_downstream_publish_asset(dagster)
    l7_asset = phase2_assets[-1]

    class FakePhase2PoolFailureRateResource(dagster.ConfigurableResource):
        def get_phase2_pool_failure_rate_event(self) -> object:
            from orchestrator.checks import Phase2PoolFailureRateEvent

            return Phase2PoolFailureRateEvent(
                failed_count=4,
                total_count=10,
                failed_nodes=(
                    PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY,
                    PHASE2_SINGLE_STOCK_MSFT_ASSET_KEY,
                ),
                reason="fake pool failures",
            )

    defs = dagster.Definitions(
        assets=[*phase0_assets, *phase1_assets, *phase2_assets, downstream_publish],
        asset_checks=[build_phase2_pool_failure_rate_check(l7_asset)],
        jobs=[daily_cycle_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
            PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: (
                FakePhase2PoolFailureRateResource()
            ),
        },
    )

    result = _execute_daily_cycle(defs, dagster_instance, caplog)
    evaluation = single_evaluation(
        asset_check_evaluations(result),
        "phase2_pool_failure_rate_gate",
    )

    return FailureInjectionOutcome(
        action=str(metadata_value(evaluation, "action")),
        failure_class=str(metadata_value(evaluation, "failure_class")),
        success=bool(getattr(result, "success", False)),
        failed_node=(
            f"{PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY}, "
            f"{PHASE2_SINGLE_STOCK_MSFT_ASSET_KEY}"
        ),
        result=result,
        alert_payloads=tuple(alert_payloads(caplog.records)),
        materialized_keys=frozenset(asset_materialization_keys(result)),
        evaluations=(evaluation,),
        extras={
            "required_materialized_asset_keys": (PHASE2_STAGE_KEYS[-1],),
            "blocked_asset_keys": (PHASE2_DOWNSTREAM_PUBLISH_ASSET_KEY,),
        },
    )


def _run_phase3_formal_commit_failure(
    dagster: Any,
    *,
    stub_policy_path: str,
    dagster_instance: object,
    caplog: pytest.LogCaptureFixture,
    **_: object,
) -> FailureInjectionOutcome:
    from orchestrator.checks import (
        FormalCommitFailureEvent,
        classify_formal_commit_failure,
        dispatch_gate_decision_alert,
    )
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.cycle import daily_cycle_job

    phase0_assets = _phase0_direct_assets(dagster)
    phase1_assets = _phase1_snapshot_direct_assets(dagster)

    @dagster.asset(name=PHASE2_STAGE_KEYS[-1], group_name=PHASE2_GROUP_NAME)
    def phase2_l7(graph_snapshot: str) -> str:
        return f"{graph_snapshot}:l7"

    @dagster.asset(
        name=PHASE3_FORMAL_COMMIT_ASSET_KEY,
        group_name=PHASE3_GROUP_NAME,
    )
    def formal_objects_commit(l7: str) -> str:
        return f"{l7}:formal-commit"

    @dagster.asset_check(
        asset=formal_objects_commit,
        name="phase3_formal_commit_gate",
        blocking=True,
    )
    def phase3_formal_commit_gate(
        context: object,
        gate_policy: GatePolicyResource,
    ) -> object:
        event = FormalCommitFailureEvent(
            failed_node=PHASE3_FORMAL_COMMIT_ASSET_KEY,
            table_name="formal.daily_scores",
            reason="fake formal commit failed",
        )
        decision = classify_formal_commit_failure(event, gate_policy.policy)
        dispatch_gate_decision_alert(
            decision,
            cycle_id=cycle_id_from_context(context),
            failed_node=event.failed_node,
            summary=event.reason or decision.reason or "formal commit failed",
            channels=gate_policy.policy.alert_channels,
        )
        return dagster.AssetCheckResult(
            passed=decision.action is GateAction.CONTINUE,
            metadata={
                "phase": decision.phase.value,
                "failure_class": decision.failure_class.value,
                "action": decision.action.value,
                "scenario_id": decision.scenario_id or "",
                "failed_node": event.failed_node,
                "table_name": event.table_name or "",
            },
        )

    @dagster.asset(
        name=PHASE3_MANIFEST_ASSET_KEY,
        group_name=PHASE3_GROUP_NAME,
    )
    def cycle_publish_manifest(formal_objects_commit: str) -> str:
        return f"{formal_objects_commit}:manifest"

    defs = dagster.Definitions(
        assets=[
            *phase0_assets,
            *phase1_assets,
            phase2_l7,
            formal_objects_commit,
            cycle_publish_manifest,
        ],
        asset_checks=[phase3_formal_commit_gate],
        jobs=[daily_cycle_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )

    result = _execute_daily_cycle(defs, dagster_instance, caplog)
    evaluation = single_evaluation(
        asset_check_evaluations(result),
        "phase3_formal_commit_gate",
    )

    return FailureInjectionOutcome(
        action=str(metadata_value(evaluation, "action")),
        failure_class=str(metadata_value(evaluation, "failure_class")),
        success=bool(getattr(result, "success", False)),
        failed_node=PHASE3_FORMAL_COMMIT_ASSET_KEY,
        result=result,
        alert_payloads=tuple(alert_payloads(caplog.records)),
        materialized_keys=frozenset(asset_materialization_keys(result)),
        evaluations=(evaluation,),
        extras={
            "required_materialized_asset_keys": (
                PHASE3_FORMAL_COMMIT_ASSET_KEY,
            ),
            "blocked_asset_keys": (PHASE3_MANIFEST_ASSET_KEY,),
        },
    )


def _run_phase3_manifest_repair_failure(
    dagster: Any,
    *,
    stub_policy_path: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dagster_instance: object,
    caplog: pytest.LogCaptureFixture,
    **_: object,
) -> FailureInjectionOutcome:
    from orchestrator.checks import (
        ManifestWriteFailureEvent,
        plan_manifest_repair_rerun,
    )
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.rerun_request import request_from_plan
    from orchestrator.sensors.manual_rerun import evaluate_manual_rerun_requests

    request_dir = tmp_path / "phase3_manifest_repair_requests"
    monkeypatch.setenv("ORCHESTRATOR_RERUN_REQUEST_DIR", str(request_dir))
    monkeypatch.setenv(
        "ORCHESTRATOR_MANIFEST_REPAIR_ASSET_KEY",
        REPAIR_MANIFEST_ASSET_KEY,
    )
    phase0_assets = _phase0_direct_assets(dagster)
    phase1_assets = _phase1_snapshot_direct_assets(dagster)

    @dagster.asset(name=PHASE2_STAGE_KEYS[-1], group_name=PHASE2_GROUP_NAME)
    def phase2_l7(graph_snapshot: str) -> str:
        return f"{graph_snapshot}:l7"

    @dagster.asset(
        name=PHASE3_FORMAL_COMMIT_ASSET_KEY,
        group_name=PHASE3_GROUP_NAME,
    )
    def formal_objects_commit(l7: str) -> str:
        return f"{l7}:formal-commit"

    @dagster.asset(
        name=PHASE3_MANIFEST_ASSET_KEY,
        group_name=PHASE3_GROUP_NAME,
    )
    def cycle_publish_manifest(formal_objects_commit: str) -> str:
        raise RuntimeError(f"fake manifest write failed after {formal_objects_commit}")

    defs = dagster.Definitions(
        assets=[
            *phase0_assets,
            *phase1_assets,
            phase2_l7,
            formal_objects_commit,
            cycle_publish_manifest,
        ],
        jobs=[daily_cycle_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )

    result = _execute_daily_cycle(defs, dagster_instance, caplog)
    request_payloads = tuple(rerun_request_payloads(request_dir))
    run_request = evaluate_manual_rerun_requests(request_dir)
    plan = plan_manifest_repair_rerun(
        str(getattr(result, "run_id")),
        ManifestWriteFailureEvent(
            repair_node=REPAIR_MANIFEST_ASSET_KEY,
            reason="fake manifest write failed",
        ),
        load_gate_policy(stub_policy_path),
    )
    alert = last_alert_payload(caplog.records)

    return FailureInjectionOutcome(
        action=str(alert["action"]),
        failure_class=str(alert["failure_class"]),
        success=bool(getattr(result, "success", False)),
        failed_node=PHASE3_MANIFEST_ASSET_KEY,
        result=result,
        alert_payloads=tuple(alert_payloads(caplog.records)),
        materialized_keys=frozenset(asset_materialization_keys(result)),
        rerun_requests=request_payloads,
        run_request=run_request,
        extras={
            "expected_request": expected_request_from_plan(
                plan,
                request_payloads,
                request_from_plan=request_from_plan,
            ),
            "required_materialized_asset_keys": (
                PHASE3_FORMAL_COMMIT_ASSET_KEY,
            ),
            "blocked_asset_keys": (PHASE3_MANIFEST_ASSET_KEY,),
        },
    )


def _run_phase2_infra_unavailable_failure(
    dagster: Any,
    *,
    stub_policy_path: str,
    monkeypatch: pytest.MonkeyPatch,
    dagster_instance: object,
    caplog: pytest.LogCaptureFixture,
    **_: object,
) -> FailureInjectionOutcome:
    _clear_phase0_imports()
    from orchestrator.definitions import build_definitions

    monkeypatch.delenv("ORCHESTRATOR_DEFINITIONS_PROFILE", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_PROFILE", raising=False)
    defs = build_definitions(
        module_factories=[
            fake_phase0_surface_provider(dagster),
            fake_phase1_provider(dagster),
            fake_phase2_provider(dagster, failing_resource=True),
        ],
        policy_path=stub_policy_path,
    )
    result = _execute_daily_cycle(defs, dagster_instance, caplog)
    alert = single_alert_for_failed_node(caplog.records, "phase2_pool_failure_rate")

    return FailureInjectionOutcome(
        action=str(alert["action"]),
        failure_class=str(alert["failure_class"]),
        success=bool(getattr(result, "success", False)),
        failed_node="phase2_pool_failure_rate",
        result=result,
        alert_payloads=tuple(alert_payloads(caplog.records)),
        materialized_keys=frozenset(asset_materialization_keys(result)),
        extras={"blocked_asset_keys": (PHASE2_DOWNSTREAM_PUBLISH_ASSET_KEY,)},
    )


def _execute_daily_cycle(
    defs: object,
    dagster_instance: object,
    caplog: pytest.LogCaptureFixture,
    *,
    tags: Mapping[str, str] | None = None,
) -> object:
    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        return defs.get_job_def("daily_cycle_job").execute_in_process(
            instance=dagster_instance,
            raise_on_error=False,
            tags=dict(tags or {"cycle_id": "cycle-20260416"}),
        )


def _phase0_direct_assets(dagster: Any) -> tuple[object, object, object]:
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
        return f"{candidate_freeze}:ready"

    return phase0_readiness_ping, candidate_freeze, graph_status


def _phase1_snapshot_direct_assets(dagster: Any) -> tuple[object, object]:
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
        return f"{graph_promotion}:snapshot"

    return graph_promotion, graph_snapshot


def _phase2_stage_assets(dagster: Any) -> tuple[object, ...]:
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

    return (
        phase2_l1,
        phase2_l2,
        phase2_l3,
        phase2_l4,
        phase2_l5,
        phase2_l6,
        phase2_l7,
    )


def _phase2_downstream_publish_asset(dagster: Any) -> object:
    @dagster.asset(
        name=PHASE2_DOWNSTREAM_PUBLISH_ASSET_KEY,
        group_name=PHASE2_GROUP_NAME,
    )
    def phase2_downstream_publish(l7: str) -> str:
        return f"{l7}:published"

    return phase2_downstream_publish


def cycle_id_from_context(context: object) -> str:
    dagster_run = getattr(context, "dagster_run", None)
    if dagster_run is None:
        dagster_run = getattr(context, "run", None)
    tags = getattr(dagster_run, "tags", {}) or {}
    cycle_id = tags.get("cycle_id") if isinstance(tags, Mapping) else None
    if isinstance(cycle_id, str) and cycle_id:
        return cycle_id
    run_id = getattr(context, "run_id", None)
    return run_id if isinstance(run_id, str) and run_id else "unknown-cycle"


def alert_payloads(records: Iterable[logging.LogRecord]) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in records:
        if record.name != "orchestrator.alerting.dispatcher":
            continue
        try:
            payload = json.loads(record.message)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def last_alert_payload(records: Iterable[logging.LogRecord]) -> dict[str, object]:
    payloads = alert_payloads(records)
    assert payloads
    return payloads[-1]


def single_alert_for_failed_node(
    records: Iterable[logging.LogRecord],
    failed_node: str,
) -> dict[str, object]:
    payloads = [
        payload
        for payload in alert_payloads(records)
        if payload.get("failed_node") == failed_node
    ]
    assert len(payloads) == 1
    return payloads[0]


def single_evaluation(evaluations: Iterable[object], check_name: str) -> object:
    matches = [
        evaluation
        for evaluation in evaluations
        if check_name_from_evaluation(evaluation) == check_name
    ]
    assert len(matches) == 1
    return matches[0]


def check_name_from_evaluation(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str):
        return check_name
    check_key = getattr(evaluation, "check_key", None)
    name = getattr(check_key, "name", None)
    return name if isinstance(name, str) else None


def asset_selection_strings(asset_selection: object) -> list[str]:
    output: list[str] = []
    for asset_key in asset_selection or ():
        if isinstance(asset_key, str):
            output.append(asset_key)
            continue
        to_user_string = getattr(asset_key, "to_user_string", None)
        if callable(to_user_string):
            output.append(to_user_string())
            continue
        path = getattr(asset_key, "path", None)
        if isinstance(path, (list, tuple)):
            output.append("/".join(str(part) for part in path))
            continue
        output.append(str(asset_key))
    return output


def asset_observations(result: object) -> list[object]:
    observations: list[object] = []
    for event in getattr(result, "all_events", ()):
        if not (
            getattr(event, "is_asset_observation", False)
            or getattr(event, "event_type_value", None) == "ASSET_OBSERVATION"
        ):
            continue

        event_specific_data = getattr(event, "event_specific_data", None)
        observation = getattr(event_specific_data, "asset_observation", None)
        if observation is None:
            observation = getattr(event_specific_data, "observation", None)
        if observation is None:
            observation = getattr(event, "asset_observation", None)
        if observation is not None:
            observations.append(observation)
    return observations


def gate_observation_for_asset(
    observations: Iterable[object],
    asset_key: str,
) -> object:
    for observation in observations:
        if asset_key_string(getattr(observation, "asset_key", None)) != asset_key:
            continue
        metadata = getattr(observation, "metadata", {}) or {}
        if "action" in metadata and "failure_class" in metadata:
            return observation
    pytest.fail(f"missing dbt gate observation for {asset_key}")


def asset_key_string(asset_key: object) -> str:
    if isinstance(asset_key, str):
        return asset_key
    to_user_string = getattr(asset_key, "to_user_string", None)
    if callable(to_user_string):
        return to_user_string()
    path = getattr(asset_key, "path", None)
    if isinstance(path, (list, tuple)):
        return "/".join(str(part) for part in path)
    return str(asset_key)


def heartbeat_asset_key(dbt_phase0_assets: object) -> object:
    for asset_key in getattr(dbt_phase0_assets, "keys", ()):
        path = tuple(getattr(asset_key, "path", ()))
        if path and path[-1] == "heartbeat":
            return asset_key
    pytest.fail("dbt heartbeat model asset key was not registered")


def rerun_request_payloads(request_dir: Path) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for request_path in sorted(request_dir.glob("*.json")):
        payload = json.loads(request_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def expected_request_from_plan(
    plan: object,
    request_payloads: tuple[dict[str, object], ...],
    *,
    request_from_plan: Callable[[object], dict[str, object]],
) -> dict[str, object]:
    assert len(request_payloads) == 1
    generated_at = request_payloads[0]["generated_at"]
    assert isinstance(generated_at, str)
    return request_from_plan(
        replace(plan, generated_at=datetime.fromisoformat(generated_at)),
    )


__all__ = [
    "FailureInjectionCase",
    "FailureInjectionOutcome",
    "PHASE1_GRAPH_PROMOTION_ASSET_KEY",
    "PHASE2_DOWNSTREAM_PUBLISH_ASSET_KEY",
    "PHASE2_STAGE_KEYS",
    "PHASE3_FORMAL_COMMIT_ASSET_KEY",
    "PHASE3_MANIFEST_ASSET_KEY",
    "READY_GRAPH_MARKER_ASSET_KEY",
    "REPAIR_MANIFEST_ASSET_KEY",
    "alert_payloads",
    "asset_key_string",
    "asset_observations",
    "asset_selection_strings",
    "fake_phase0_surface_provider",
    "fake_phase1_provider",
    "fake_phase2_provider",
    "gate_matrix_failure_cases",
    "gate_observation_for_asset",
    "heartbeat_asset_key",
    "last_alert_payload",
    "rerun_request_payloads",
    "single_alert_for_failed_node",
    "single_evaluation",
]
