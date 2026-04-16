"""Phase 3 formal commit and publish manifest gate adapters."""

from __future__ import annotations

from dataclasses import dataclass

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.models import GateDecision
from orchestrator.policy import FailureClass, GatePolicyProfile, PhaseEnum


@dataclass(frozen=True, slots=True, kw_only=True)
class FormalCommitFailureEvent:
    """Normalized Phase 3 formal object commit failure event."""

    failure_class: FailureClass = FailureClass.PUBLISH
    failed_node: str
    table_name: str | None = None
    reason: str | None = None


def classify_formal_commit_failure(
    event: FormalCommitFailureEvent,
    policy: GatePolicyProfile,
) -> GateDecision:
    """Classify a formal commit failure as a Phase 3 publish gate decision."""

    return classify_gate_result(PhaseEnum.PHASE3, event, policy)


__all__ = [
    "FormalCommitFailureEvent",
    "classify_formal_commit_failure",
]
