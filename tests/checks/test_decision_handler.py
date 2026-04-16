import json
import logging

import pytest

from orchestrator.checks import GateDecision, dispatch_gate_decision_alert
from orchestrator.policy import FailureClass, GateAction, PhaseEnum


def test_dispatch_gate_decision_alert_logs_fail_run_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    decision = GateDecision(
        phase=PhaseEnum.PHASE0,
        failure_class=FailureClass.DATA_QUALITY,
        action=GateAction.FAIL_RUN,
        reason="policy row",
    )

    with caplog.at_level(logging.WARNING):
        dispatch_gate_decision_alert(
            decision,
            cycle_id="cycle-1",
            failed_node="data_readiness",
            summary="not ready",
            channels=("logging",),
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload == {
        "cycle_id": "cycle-1",
        "phase": "phase0",
        "status": "failed",
        "failed_node": "data_readiness",
        "action": "fail_run",
        "summary": "not ready",
        "failure_class": "data_quality",
        "scenario_id": None,
        "runbook_url": "docs/RUNBOOK_P5.md#phase0-data_quality-fail_run",
    }


@pytest.mark.parametrize(
    ("phase", "failure_class", "action", "expected_runbook_url"),
    [
        (
            PhaseEnum.PHASE0,
            FailureClass.DATA_QUALITY,
            GateAction.FAIL_RUN,
            "docs/RUNBOOK_P5.md#phase0-data_quality-fail_run",
        ),
        (
            PhaseEnum.PHASE0,
            FailureClass.TASK_LEVEL,
            GateAction.PARTIAL_RERUN,
            "docs/RUNBOOK_P5.md#phase0-task_level-partial_rerun",
        ),
        (
            PhaseEnum.PHASE2,
            FailureClass.TASK_LEVEL,
            GateAction.MARK_INCONCLUSIVE,
            "docs/RUNBOOK_P5.md#phase2-task_level-mark_inconclusive",
        ),
        (
            PhaseEnum.PHASE3,
            FailureClass.INFRA,
            GateAction.REPAIR_MANIFEST,
            "docs/RUNBOOK_P5.md#phase3-infra-repair_manifest",
        ),
    ],
)
def test_dispatch_gate_decision_alert_fills_runbook_for_non_continue_actions(
    phase: PhaseEnum,
    failure_class: FailureClass,
    action: GateAction,
    expected_runbook_url: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    decision = GateDecision(
        phase=phase,
        failure_class=failure_class,
        action=action,
        reason="policy row",
    )

    with caplog.at_level(logging.WARNING):
        dispatch_gate_decision_alert(
            decision,
            cycle_id="cycle-1",
            failed_node="phase3_manifest",
            summary="gate failed",
            channels=("logging",),
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload["phase"] == phase.value
    assert payload["status"] == "failed"
    assert payload["action"] == action.value
    assert payload["failure_class"] == failure_class.value
    assert payload["failed_node"] == "phase3_manifest"
    assert payload["runbook_url"] == expected_runbook_url


def test_dispatch_gate_decision_alert_uses_scenario_runbook_mapping(
    caplog: pytest.LogCaptureFixture,
) -> None:
    decision = GateDecision(
        phase=PhaseEnum.PHASE1,
        failure_class=FailureClass.INFRA,
        action=GateAction.FAIL_RUN,
        reason="infra hard stop",
        scenario_id="infra_unavailable_hard_stop",
    )

    with caplog.at_level(logging.WARNING):
        dispatch_gate_decision_alert(
            decision,
            cycle_id="cycle-1",
            failed_node="graph_store",
            summary="graph store unavailable",
            channels=("logging",),
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload["scenario_id"] == "infra_unavailable_hard_stop"
    assert payload["runbook_url"] == "docs/RUNBOOK_P5.md#phase2-infra-fail_run"


def test_dispatch_gate_decision_alert_preserves_explicit_runbook_url(
    caplog: pytest.LogCaptureFixture,
) -> None:
    decision = GateDecision(
        phase=PhaseEnum.PHASE2,
        failure_class=FailureClass.DATA_QUALITY,
        action=GateAction.FAIL_RUN,
        reason="policy row",
    )

    with caplog.at_level(logging.WARNING):
        dispatch_gate_decision_alert(
            decision,
            cycle_id="cycle-1",
            failed_node="phase2_pool",
            summary="pool failed",
            channels=("logging",),
            runbook_url="docs/custom.md#pool",
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload["runbook_url"] == "docs/custom.md#pool"


def test_dispatch_gate_decision_alert_skips_continue(
    caplog: pytest.LogCaptureFixture,
) -> None:
    decision = GateDecision(
        phase=PhaseEnum.PHASE0,
        failure_class=None,
        action=GateAction.CONTINUE,
    )

    with caplog.at_level(logging.WARNING):
        dispatch_gate_decision_alert(
            decision,
            cycle_id="cycle-1",
            failed_node=None,
            summary="ready",
            channels=("logging",),
        )

    assert caplog.records == []
