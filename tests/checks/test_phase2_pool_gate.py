from __future__ import annotations

import json
import logging
from dataclasses import fields
from pathlib import Path
from typing import Any

import pytest
import yaml

from orchestrator.checks import (
    Phase2PoolFailureRateEvent,
    classify_phase2_pool_failure_rate,
    dispatch_phase2_pool_failure_alert,
)
from orchestrator.policy import FailureClass, GateAction, PhaseEnum, load_gate_policy


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


@pytest.fixture
def gate_policy() -> Any:
    return load_gate_policy(LITE_POLICY_PATH)


def test_phase2_pool_failure_rate_event_fields_match_contract() -> None:
    assert [field.name for field in fields(Phase2PoolFailureRateEvent)] == [
        "failure_class",
        "failed_count",
        "total_count",
        "failed_nodes",
        "reason",
        "cycle_id",
    ]


@pytest.mark.parametrize(
    ("failed_count", "total_count", "expected_action"),
    [
        (1, 10, GateAction.CONTINUE),
        (3, 10, GateAction.CONTINUE),
        (4, 10, GateAction.FAIL_RUN),
    ],
)
def test_classify_phase2_pool_failure_rate_threshold(
    gate_policy: Any,
    failed_count: int,
    total_count: int,
    expected_action: GateAction,
) -> None:
    event = Phase2PoolFailureRateEvent(
        failed_count=failed_count,
        total_count=total_count,
        failed_nodes=("phase2_stock_AAPL",),
        reason="fake pool failures",
    )

    decision = classify_phase2_pool_failure_rate(event, gate_policy)

    assert event.failure_class is FailureClass.DATA_QUALITY
    assert decision.phase is PhaseEnum.PHASE2
    assert decision.failure_class is FailureClass.DATA_QUALITY
    assert decision.action is expected_action
    if expected_action is GateAction.FAIL_RUN:
        assert decision.scenario_id == "phase2_pool_failure_rate_exceeded"
        assert (
            decision.reason
            == "Pool failure rate exceeded threshold; fail the run and alert."
        )


def test_classify_phase2_pool_failure_rate_reads_yaml_threshold(
    tmp_path: Path,
) -> None:
    policy_data = yaml.safe_load(LITE_POLICY_PATH.read_text(encoding="utf-8"))
    policy_data["thresholds"]["phase2_pool_failure_rate"] = 0.45
    policy_path = tmp_path / "gate_policy.yaml"
    policy_path.write_text(yaml.safe_dump(policy_data), encoding="utf-8")
    policy = load_gate_policy(policy_path)
    event = Phase2PoolFailureRateEvent(
        failed_count=2,
        total_count=5,
        failed_nodes=("phase2_stock_AAPL",),
    )

    decision = classify_phase2_pool_failure_rate(event, policy)

    assert decision.action is GateAction.CONTINUE


def test_classify_phase2_pool_failure_rate_requires_threshold(
    gate_policy: Any,
) -> None:
    policy = gate_policy.model_copy(update={"thresholds": {}})
    event = Phase2PoolFailureRateEvent(
        failed_count=1,
        total_count=2,
        failed_nodes=("phase2_stock_AAPL",),
    )

    with pytest.raises(
        ValueError,
        match="missing thresholds.phase2_pool_failure_rate",
    ):
        classify_phase2_pool_failure_rate(event, policy)


@pytest.mark.parametrize("threshold", [float("nan"), float("inf"), float("-inf")])
def test_classify_phase2_pool_failure_rate_rejects_non_finite_threshold(
    gate_policy: Any,
    threshold: float,
) -> None:
    policy = gate_policy.model_copy(
        update={
            "thresholds": {
                **gate_policy.thresholds,
                "phase2_pool_failure_rate": threshold,
            },
        },
    )
    event = Phase2PoolFailureRateEvent(
        failed_count=1,
        total_count=2,
        failed_nodes=("phase2_stock_AAPL",),
    )

    with pytest.raises(
        ValueError,
        match=(
            "thresholds.phase2_pool_failure_rate "
            "must be finite and between 0 and 1"
        ),
    ):
        classify_phase2_pool_failure_rate(event, policy)


def test_dispatch_phase2_pool_failure_alert_logs_shared_payload(
    gate_policy: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    event = Phase2PoolFailureRateEvent(
        failed_count=4,
        total_count=10,
        failed_nodes=("phase2_stock_AAPL", "phase2_stock_MSFT"),
        reason="fake pool failures",
    )
    decision = classify_phase2_pool_failure_rate(event, gate_policy)

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        dispatch_phase2_pool_failure_alert(
            decision,
            event,
            cycle_id="cycle-20260416",
            channels=("logging",),
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload["cycle_id"] == "cycle-20260416"
    assert payload["phase"] == "phase2"
    assert payload["failure_class"] == "data_quality"
    assert payload["action"] == "fail_run"
    assert payload["scenario_id"] == "phase2_pool_failure_rate_exceeded"
    assert payload["failed_node"] == "phase2_stock_AAPL, phase2_stock_MSFT"
    assert payload["summary"] == (
        "Phase 2 pool failure rate 0.4 (4/10): fake pool failures"
    )
    assert payload["runbook_url"] == (
        "docs/RUNBOOK_P5.md#phase2-data_quality-fail_run"
    )


def test_dispatch_phase2_pool_failure_alert_skips_continue(
    gate_policy: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    event = Phase2PoolFailureRateEvent(
        failed_count=1,
        total_count=10,
        failed_nodes=("phase2_stock_AAPL",),
    )
    decision = classify_phase2_pool_failure_rate(event, gate_policy)

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        dispatch_phase2_pool_failure_alert(
            decision,
            event,
            cycle_id="cycle-20260416",
            channels=("logging",),
        )

    assert caplog.records == []
