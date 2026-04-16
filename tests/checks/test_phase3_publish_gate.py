from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Any

import pytest

from orchestrator.checks import (
    FormalCommitFailureEvent,
    classify_formal_commit_failure,
)
from orchestrator.policy import FailureClass, GateAction, PhaseEnum, load_gate_policy


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


@pytest.fixture
def gate_policy() -> Any:
    return load_gate_policy(LITE_POLICY_PATH)


def test_formal_commit_failure_event_fields_match_contract() -> None:
    assert [field.name for field in fields(FormalCommitFailureEvent)] == [
        "failure_class",
        "failed_node",
        "table_name",
        "reason",
    ]


def test_classify_formal_commit_failure_uses_phase3_publish_policy(
    gate_policy: Any,
) -> None:
    event = FormalCommitFailureEvent(
        failed_node="formal_objects_commit",
        table_name="formal.daily_scores",
        reason="commit failed",
    )

    decision = classify_formal_commit_failure(event, gate_policy)

    assert event.failure_class is FailureClass.PUBLISH
    assert decision.phase is PhaseEnum.PHASE3
    assert decision.failure_class is FailureClass.PUBLISH
    assert decision.action is GateAction.FAIL_RUN
    assert decision.scenario_id == "phase3_formal_commit_failed"
    assert (
        decision.reason
        == "Formal table commit failed; fail Phase 3 before writing manifest."
    )
