"""Runtime gate decision models."""

from __future__ import annotations

from dataclasses import dataclass

from orchestrator.policy import FailureClass, GateAction, PhaseEnum


@dataclass(frozen=True, slots=True)
class GateDecision:
    """Decision produced by classifying a gate failure event."""

    phase: PhaseEnum
    failure_class: FailureClass | None
    action: GateAction
    reason: str | None = None


__all__ = ["GateDecision"]
