"""Pure gate failure classifier."""

from __future__ import annotations

from orchestrator.checks.models import GateDecision
from orchestrator.policy import FailureClass, GateAction, GatePolicyProfile, PhaseEnum


class UnknownGateFailure(Exception):
    """Raised when a policy has no row for a phase/failure-class pair."""


def classify_gate_result(
    phase: PhaseEnum,
    failure_class: FailureClass | None,
    policy: GatePolicyProfile,
) -> GateDecision:
    """Classify a gate event by looking up the configured policy matrix."""

    if failure_class is None:
        return GateDecision(
            phase=phase,
            failure_class=None,
            action=GateAction.CONTINUE,
        )

    for entry in policy.phase_matrix:
        if entry.phase is phase and entry.failure_class is failure_class:
            return GateDecision(
                phase=phase,
                failure_class=failure_class,
                action=entry.action,
                reason=entry.description,
            )

    msg = (
        "unknown gate failure policy entry for "
        f"phase={phase.value} failure_class={failure_class.value}"
    )
    raise UnknownGateFailure(msg)


__all__ = ["UnknownGateFailure", "classify_gate_result"]
