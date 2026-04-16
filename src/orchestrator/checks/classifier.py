"""Pure gate failure classifier."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from orchestrator.checks.models import GateDecision
from orchestrator.policy import FailureClass, GateAction, GatePolicyProfile, PhaseEnum


class UnknownGateFailure(Exception):
    """Raised when a policy has no row for a phase/failure-class pair."""


def classify_gate_result(
    phase: PhaseEnum,
    event: object | None,
    policy: GatePolicyProfile,
) -> GateDecision:
    """Classify a gate event by looking up the configured policy matrix."""

    failure_class = _failure_class_from_event(event)
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


def _failure_class_from_event(event: object | None) -> FailureClass | None:
    if event is None:
        return None
    if isinstance(event, Mapping):
        if "failure_class" not in event:
            raise TypeError("gate event mapping must include failure_class")
        return _coerce_failure_class(event["failure_class"])
    if hasattr(event, "failure_class"):
        return _coerce_failure_class(getattr(event, "failure_class"))
    raise TypeError("gate event must expose failure_class")


def _coerce_failure_class(value: Any) -> FailureClass | None:
    if value is None:
        return None
    if isinstance(value, FailureClass):
        return value
    if isinstance(value, str):
        try:
            return FailureClass(value)
        except ValueError as exc:
            raise ValueError(f"unknown failure_class: {value}") from exc
    raise TypeError("failure_class must be a FailureClass or string")


__all__ = ["UnknownGateFailure", "classify_gate_result"]
