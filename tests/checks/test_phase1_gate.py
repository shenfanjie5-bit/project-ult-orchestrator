from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Any

import pytest

from orchestrator.checks import (
    GateDecision,
    GraphPromotionFailureEvent,
    classify_graph_promotion_failure,
    should_advance_ready_graph,
)
from orchestrator.policy import FailureClass, GateAction, PhaseEnum, load_gate_policy


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


@pytest.fixture
def gate_policy() -> Any:
    return load_gate_policy(LITE_POLICY_PATH)


def test_graph_promotion_failure_event_fields_match_contract() -> None:
    assert [field.name for field in fields(GraphPromotionFailureEvent)] == [
        "failure_class",
        "failed_node",
        "snapshot_id",
        "reason",
    ]


def test_classify_graph_promotion_failure_uses_phase1_publish_policy(
    gate_policy: Any,
) -> None:
    event = GraphPromotionFailureEvent(
        failed_node="graph_snapshot",
        snapshot_id="snapshot-20260416",
        reason="snapshot write failed",
    )

    decision = classify_graph_promotion_failure(event, gate_policy)

    assert event.failure_class is FailureClass.PUBLISH
    assert decision.phase is PhaseEnum.PHASE1
    assert decision.failure_class is FailureClass.PUBLISH
    assert decision.action is GateAction.FAIL_RUN
    assert decision.scenario_id == "phase1_graph_promotion_snapshot_failed"
    assert (
        decision.reason
        == "Graph promotion or snapshot failed; retain the previous ready graph."
    )


def test_classify_graph_promotion_failure_accepts_mapping_event(
    gate_policy: Any,
) -> None:
    decision = classify_graph_promotion_failure(
        {
            "failure_class": "publish",
            "failed_node": "graph_promotion",
            "reason": "promotion failed",
        },
        gate_policy,
    )

    assert decision.phase is PhaseEnum.PHASE1
    assert decision.failure_class is FailureClass.PUBLISH
    assert decision.action is GateAction.FAIL_RUN


def test_classify_graph_promotion_failure_rejects_unknown_event_shape(
    gate_policy: Any,
) -> None:
    with pytest.raises(TypeError, match="must include failure_class"):
        classify_graph_promotion_failure({"failed_node": "graph_snapshot"}, gate_policy)


def test_should_advance_ready_graph_only_allows_continue() -> None:
    fail_decision = GateDecision(
        phase=PhaseEnum.PHASE1,
        failure_class=FailureClass.PUBLISH,
        action=GateAction.FAIL_RUN,
    )
    continue_decision = GateDecision(
        phase=PhaseEnum.PHASE1,
        failure_class=None,
        action=GateAction.CONTINUE,
    )

    assert should_advance_ready_graph(fail_decision) is False
    assert should_advance_ready_graph(continue_decision) is True
