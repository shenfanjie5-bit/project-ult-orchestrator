from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from orchestrator.checks import DataReadinessSignal
from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.dbt_events import (
    classify_dbt_test_failure,
    plan_dbt_test_partial_rerun,
)
from orchestrator.policy import FailureClass, GateAction, PhaseEnum, load_gate_policy
from tests.integration.conftest import (
    asset_check_evaluations,
    asset_materialization_keys,
    metadata_value,
)


class _ReadinessResource:
    def __init__(self, signal: DataReadinessSignal) -> None:
        self._signal = signal

    def get_data_readiness_signal(self) -> DataReadinessSignal:
        return self._signal


class _GatePolicyResource:
    def __init__(self, policy: object) -> None:
        self.policy = policy


class _LLMHealthResult:
    healthy = False
    summary = "provider unavailable"
    provider = "fake-llm"


class _LLMHealthProbe:
    def check_health(self) -> _LLMHealthResult:
        return _LLMHealthResult()


def test_readiness_not_ready_fails_phase0_alerts_and_skips(
    dagster_module: object,
    stub_policy_path: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dagster = dagster_module
    from orchestrator.sensors.data_readiness import data_readiness_sensor

    policy = load_gate_policy(stub_policy_path)
    signal = DataReadinessSignal(
        ready=False,
        cycle_id="cycle-20260416",
        reason="market data delayed",
        failed_node="phase0_readiness_ping",
    )
    context = dagster.build_sensor_context(
        resources={
            "data_readiness": _ReadinessResource(signal),
            "gate_policy": _GatePolicyResource(policy),
        },
    )

    decision = classify_gate_result(
        PhaseEnum.PHASE0,
        {"failure_class": FailureClass.DATA_QUALITY},
        policy,
    )
    with caplog.at_level(logging.WARNING):
        result = data_readiness_sensor.evaluation_fn(context)

    alert = _alert_payloads(caplog)[-1]

    assert isinstance(result, dagster.SkipReason)
    assert not isinstance(result, dagster.RunRequest)
    assert decision.phase is PhaseEnum.PHASE0
    assert decision.failure_class is FailureClass.DATA_QUALITY
    assert decision.action is GateAction.FAIL_RUN
    assert alert["cycle_id"] == "cycle-20260416"
    assert alert["phase"] == "phase0"
    assert alert["failed_node"] == "phase0_readiness_ping"
    assert alert["action"] == "fail_run"
    assert alert["failure_class"] == "data_quality"


def test_llm_health_failure_emits_failed_check_and_blocks_downstream_phase(
    dagster_module: object,
    dagster_dbt_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module
    from orchestrator.checks.asset_checks import llm_health_check
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.phase0 import phase0_readiness_ping

    @dagster.asset(name="phase1_trigger_marker", group_name="phase1")
    def phase1_trigger_marker(phase0_readiness_ping: str) -> str:
        return f"triggered after {phase0_readiness_ping}"

    job = dagster.define_asset_job(
        "llm_health_blocking_job",
        selection=dagster.AssetSelection.assets(
            "phase0_readiness_ping",
            "phase1_trigger_marker",
        ),
    )
    defs = dagster.Definitions(
        assets=[phase0_readiness_ping, phase1_trigger_marker],
        asset_checks=[llm_health_check],
        jobs=[job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
            "llm_health_probe": _LLMHealthProbe(),
        },
    )

    result = defs.get_job_def("llm_health_blocking_job").execute_in_process(
        instance=dagster_instance,
        raise_on_error=False,
    )
    llm_evaluation = _evaluation_by_name(
        asset_check_evaluations(result),
        "llm_health_check",
    )

    assert result.success is False
    assert getattr(llm_evaluation, "passed", None) is False
    assert metadata_value(llm_evaluation, "action") == "fail_run"
    assert metadata_value(llm_evaluation, "failure_class") == "infra"
    assert dagster.AssetKey(["phase1_trigger_marker"]) not in asset_materialization_keys(
        result,
    )


def test_dbt_test_failure_matrix_plans_only_failed_dbt_asset_group(
    dagster_module: object,
    stub_policy_path: str,
) -> None:
    dagster = dagster_module
    policy = load_gate_policy(stub_policy_path)
    failed_asset_key = dagster.AssetKey(["dbt_phase0_assets"])
    event = {
        "asset_key": failed_asset_key,
        "metadata": {
            "node_info": {
                "node_name": "not_null_heartbeat_heartbeat",
            },
        },
    }

    decision = classify_dbt_test_failure(event, policy)
    plan = plan_dbt_test_partial_rerun(
        "run-phase0-dbt",
        failed_asset_key,
        event,
        policy,
    )

    assert decision.phase is PhaseEnum.PHASE0
    assert decision.failure_class is FailureClass.TASK_LEVEL
    assert decision.action is GateAction.PARTIAL_RERUN
    assert plan.rerun_selection == ("dbt_phase0_assets",)
    assert plan.requires_manual_ack is False
    assert plan.rerun_mode == "asset_only"
    assert "phase0_readiness_ping" not in plan.rerun_selection
    assert "candidate_freeze" not in plan.rerun_selection


def test_manual_rerun_request_sensor_minimal_happy_path(
    dagster_module: object,
    tmp_path: Path,
) -> None:
    dagster = dagster_module
    from orchestrator.sensors.manual_rerun import evaluate_manual_rerun_requests

    request_path = tmp_path / "dbt-rerun.json"
    request_path.write_text(
        json.dumps(
            {
                "run_id": "run-phase0-dbt",
                "failed_node": "dbt_phase0_assets",
                "rerun_selection": ["dbt_phase0_assets"],
                "requires_manual_ack": False,
                "rerun_mode": "asset_only",
                "generated_at": "2026-04-16T00:00:00+00:00",
            },
        ),
        encoding="utf-8",
    )

    result = evaluate_manual_rerun_requests(tmp_path)

    assert isinstance(result, dagster.RunRequest)
    assert result.job_name == "daily_cycle_job"
    assert result.run_key == (
        "manual-rerun:run-phase0-dbt:dbt_phase0_assets:"
        "2026-04-16T00:00:00+00:00"
    )
    assert result.tags["rerun_of"] == "run-phase0-dbt"
    assert result.tags["failed_node"] == "dbt_phase0_assets"
    assert _asset_selection_strings(result.asset_selection) == ["dbt_phase0_assets"]


def _alert_payloads(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in caplog.records:
        if record.name != "orchestrator.alerting.dispatcher":
            continue
        payloads.append(json.loads(record.message))
    return payloads


def _evaluation_by_name(evaluations: list[object], check_name: str) -> object:
    for evaluation in evaluations:
        if _check_name(evaluation) == check_name:
            return evaluation
    pytest.fail(f"missing asset check evaluation for {check_name}")


def _check_name(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str):
        return check_name
    check_key = getattr(evaluation, "check_key", None)
    name = getattr(check_key, "name", None)
    return name if isinstance(name, str) else None


def _asset_selection_strings(asset_selection: object) -> list[str]:
    output: list[str] = []
    for asset_key in asset_selection or ():
        if isinstance(asset_key, str):
            output.append(asset_key)
            continue
        to_user_string = getattr(asset_key, "to_user_string", None)
        if callable(to_user_string):
            output.append(to_user_string())
            continue
        output.append(str(asset_key))
    return output
