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

    scenario_id = _scenario_id_from_event(event)
    if scenario_id is not None:
        return _classify_scenario(
            phase=phase,
            failure_class=failure_class,
            scenario_id=scenario_id,
            policy=policy,
        )

    for entry in policy.phase_matrix:
        if entry.phase is phase and entry.failure_class is failure_class:
            return GateDecision(
                phase=phase,
                failure_class=failure_class,
                action=entry.action,
                reason=entry.description,
                scenario_id=entry.scenario_id,
            )

    msg = (
        "unknown gate failure policy entry for "
        f"phase={phase.value} failure_class={failure_class.value}"
    )
    raise UnknownGateFailure(msg)


def _classify_scenario(
    *,
    phase: PhaseEnum,
    failure_class: FailureClass,
    scenario_id: str,
    policy: GatePolicyProfile,
) -> GateDecision:
    matching_entries = [
        entry for entry in policy.phase_matrix if entry.scenario_id == scenario_id
    ]
    if not matching_entries:
        msg = (
            "unknown gate failure policy entry for "
            f"scenario_id={scenario_id} phase={phase.value} "
            f"failure_class={failure_class.value}"
        )
        raise UnknownGateFailure(msg)
    if len(matching_entries) > 1:
        raise ValueError(f"ambiguous gate policy scenario_id={scenario_id}")

    entry = matching_entries[0]
    applies_to_phases = entry.applies_to_phases or (entry.phase,)
    if phase not in applies_to_phases or entry.failure_class is not failure_class:
        msg = (
            "unknown gate failure policy entry for "
            f"scenario_id={scenario_id} phase={phase.value} "
            f"failure_class={failure_class.value}"
        )
        raise UnknownGateFailure(msg)

    return GateDecision(
        phase=phase,
        failure_class=failure_class,
        action=entry.action,
        reason=entry.description,
        scenario_id=entry.scenario_id,
    )


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


def _scenario_id_from_event(event: object | None) -> str | None:
    if event is None:
        return None
    if isinstance(event, Mapping):
        if "scenario_id" not in event:
            return None
        return _coerce_scenario_id(event["scenario_id"])
    if hasattr(event, "scenario_id"):
        return _coerce_scenario_id(getattr(event, "scenario_id"))
    return None


def _coerce_failure_class(value: Any) -> FailureClass | None:
    if value is None:
        raise ValueError("gate event failure_class is required")
    if isinstance(value, FailureClass):
        return value
    if isinstance(value, str):
        try:
            return FailureClass(value)
        except ValueError as exc:
            raise ValueError(f"unknown failure_class: {value}") from exc
    raise TypeError("failure_class must be a FailureClass or string")


def _coerce_scenario_id(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("gate event scenario_id must be a non-empty string")
    return value.strip()


__all__ = ["UnknownGateFailure", "classify_gate_result"]
