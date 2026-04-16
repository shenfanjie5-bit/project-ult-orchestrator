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
        "runbook_url": None,
    }


def test_dispatch_gate_decision_alert_logs_repair_manifest_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    decision = GateDecision(
        phase=PhaseEnum.PHASE3,
        failure_class=FailureClass.INFRA,
        action=GateAction.REPAIR_MANIFEST,
        reason="policy row",
    )

    with caplog.at_level(logging.WARNING):
        dispatch_gate_decision_alert(
            decision,
            cycle_id="cycle-1",
            failed_node="phase3_manifest",
            summary="manifest write failed",
            channels=("logging",),
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload["phase"] == "phase3"
    assert payload["status"] == "failed"
    assert payload["action"] == "repair_manifest"
    assert payload["failure_class"] == "infra"
    assert payload["failed_node"] == "phase3_manifest"


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
