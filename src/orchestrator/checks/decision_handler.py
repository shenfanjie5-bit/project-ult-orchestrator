"""Gate decision side-effect handlers."""

from __future__ import annotations

from collections.abc import Iterable

from orchestrator.alerting import AlertPayload, dispatch_alert
from orchestrator.checks.models import GateDecision
from orchestrator.policy import GateAction


def dispatch_gate_decision_alert(
    decision: GateDecision,
    *,
    cycle_id: str,
    failed_node: str | None,
    summary: str,
    channels: Iterable[str],
) -> None:
    """Dispatch an alert for non-continue gate decisions."""

    if decision.action is GateAction.CONTINUE:
        return

    dispatch_alert(
        AlertPayload(
            cycle_id=cycle_id,
            phase=decision.phase.value,
            status="failed",
            failed_node=failed_node,
            action=decision.action.value,
            summary=summary,
            failure_class=(
                decision.failure_class.value if decision.failure_class else None
            ),
        ),
        channels=channels,
    )


__all__ = ["dispatch_gate_decision_alert"]
