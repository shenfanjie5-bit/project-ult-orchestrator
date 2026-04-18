from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from tests.integration.conftest import (
    asset_check_evaluations,
    asset_materialization_keys,
    metadata_value,
)

_DOWNSTREAM_PUBLISH_ASSET_KEY = "phase2_downstream_publish"


def test_daily_cycle_phase2_pool_failure_rate_gate_fails_and_alerts(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dagster = dagster_module
    defs = _build_defs(
        dagster,
        stub_policy_path=stub_policy_path,
        failed_count=4,
        total_count=10,
        failed_nodes=("phase2_stock_AAPL", "phase2_stock_MSFT"),
    )
    dagster.Definitions.validate_loadable(defs)
    assert "phase2_pool_failure_rate_gate" in _definition_check_names(defs)

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        result = defs.get_job_def("daily_cycle_job").execute_in_process(
            instance=dagster_instance,
            raise_on_error=False,
            tags={"cycle_id": "cycle-20260416"},
        )

    materialized_keys = asset_materialization_keys(result)
    evaluation = _single_evaluation(
        asset_check_evaluations(result),
        "phase2_pool_failure_rate_gate",
    )
    payloads = _alert_payloads(caplog.records)

    assert result.success is False
    assert dagster.AssetKey(["l7"]) in materialized_keys
    assert dagster.AssetKey([_DOWNSTREAM_PUBLISH_ASSET_KEY]) not in materialized_keys
    assert getattr(evaluation, "passed", None) is False
    assert metadata_value(evaluation, "action") == "fail_run"
    assert metadata_value(evaluation, "failure_rate") == 0.4
    assert payloads == [
        {
            "cycle_id": "cycle-20260416",
            "phase": "phase2",
            "status": "failed",
            "failed_node": "phase2_stock_AAPL, phase2_stock_MSFT",
            "action": "fail_run",
            "summary": "Phase 2 pool failure rate 0.4 (4/10): fake pool failures",
            "failure_class": "data_quality",
            "scenario_id": "phase2_pool_failure_rate_exceeded",
            "runbook_url": "docs/RUNBOOK_P5.md#phase2-data_quality-fail_run",
        },
    ]


def test_daily_cycle_phase2_pool_failure_rate_gate_allows_without_alert(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dagster = dagster_module
    defs = _build_defs(
        dagster,
        stub_policy_path=stub_policy_path,
        failed_count=3,
        total_count=10,
        failed_nodes=("phase2_stock_AAPL",),
    )
    dagster.Definitions.validate_loadable(defs)
    assert "phase2_pool_failure_rate_gate" in _definition_check_names(defs)

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        result = defs.get_job_def("daily_cycle_job").execute_in_process(
            instance=dagster_instance,
            raise_on_error=False,
            tags={"cycle_id": "cycle-20260416"},
        )

    materialized_keys = asset_materialization_keys(result)
    evaluation = _single_evaluation(
        asset_check_evaluations(result),
        "phase2_pool_failure_rate_gate",
    )

    assert result.success is True
    assert dagster.AssetKey(["l7"]) in materialized_keys
    assert dagster.AssetKey([_DOWNSTREAM_PUBLISH_ASSET_KEY]) in materialized_keys
    assert getattr(evaluation, "passed", None) is True
    assert metadata_value(evaluation, "action") == "continue"
    assert _alert_payloads(caplog.records) == []


def _build_defs(
    dagster: Any,
    *,
    stub_policy_path: str,
    failed_count: int,
    total_count: int,
    failed_nodes: tuple[str, ...],
) -> object:
    from orchestrator.definitions import build_definitions

    return build_definitions(
        module_factories=[
            _fake_phase0_surface_provider(dagster),
            _fake_phase1_provider(dagster),
            _fake_phase2_provider(
                dagster,
                failed_count=failed_count,
                total_count=total_count,
                failed_nodes=failed_nodes,
            ),
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
            return (
                graph_promotion,
                graph_snapshot,
            )

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {}

    return FakePhase1Provider()


def _fake_phase2_provider(
    dagster: Any,
    *,
    failed_count: int,
    total_count: int,
    failed_nodes: tuple[str, ...],
) -> object:
    from orchestrator.checks import (
        PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
        Phase2PoolFailureRateEvent,
    )
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME, PHASE2_STAGE_KEYS

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

    @dagster.asset(name=_DOWNSTREAM_PUBLISH_ASSET_KEY, group_name=PHASE2_GROUP_NAME)
    def phase2_downstream_publish(l7: str) -> str:
        return f"{l7}:published"

    class FakePhase2PoolFailureRateResource(dagster.ConfigurableResource):
        def get_phase2_pool_failure_rate_event(
            self,
        ) -> Phase2PoolFailureRateEvent:
            return Phase2PoolFailureRateEvent(
                failed_count=failed_count,
                total_count=total_count,
                failed_nodes=failed_nodes,
                reason="fake pool failures",
            )

    phase2_assets = (
        phase2_l1,
        phase2_l2,
        phase2_l3,
        phase2_l4,
        phase2_l5,
        phase2_l6,
        phase2_l7,
        phase2_downstream_publish,
    )

    class FakePhase2Provider:
        def get_assets(self) -> tuple[object, ...]:
            return phase2_assets

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {
                PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: (
                    FakePhase2PoolFailureRateResource()
                ),
            }

    return FakePhase2Provider()


def _single_evaluation(evaluations: list[object], check_name: str) -> object:
    matches = [
        evaluation
        for evaluation in evaluations
        if _check_name(evaluation) == check_name
    ]
    assert len(matches) == 1
    return matches[0]


def _check_name(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str):
        return check_name
    check_key = getattr(evaluation, "check_key", None)
    name = getattr(check_key, "name", None)
    return name if isinstance(name, str) else None


def _definition_check_names(defs: object) -> set[str]:
    names: set[str] = set()
    for check_def in getattr(defs, "asset_checks", ()) or ():
        names.update(
            check_key.name
            for check_key in getattr(check_def, "check_keys", ())
        )
        names.update(spec.name for spec in getattr(check_def, "specs", ()))
        if name := getattr(check_def, "name", None):
            names.add(name)
    return names


def _alert_payloads(records: Iterable[logging.LogRecord]) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in records:
        try:
            payload = json.loads(record.message)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads
