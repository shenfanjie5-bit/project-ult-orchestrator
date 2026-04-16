"""Data readiness sensor."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from orchestrator.checks import (
    DataReadinessSignal,
    dispatch_gate_decision_alert,
)
from orchestrator.checks.classifier import classify_gate_result
from orchestrator.policy import (
    FailureClass,
    GatePolicyProfile,
    PhaseEnum,
    load_gate_policy,
)
from orchestrator.jobs.cycle import daily_cycle_job


_DEFAULT_POLICY_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "policy" / "gate_policy.lite.yaml"
)
_READINESS_RESOURCE_KEYS = ("data_readiness", "data_readiness_provider")
_READINESS_METHOD_NAMES = (
    "get_data_readiness_signal",
    "get_readiness_signal",
    "get_data_readiness",
)
_READINESS_ATTRIBUTE_NAMES = ("data_readiness_signal", "readiness_signal")


@dataclass(frozen=True, slots=True)
class _DataReadinessGateEvent:
    failure_class: FailureClass
    cycle_id: str
    failed_node: str
    reason: str | None = None


@sensor(job=daily_cycle_job, name="data_readiness_sensor")
def data_readiness_sensor(
    context: SensorEvaluationContext,
) -> RunRequest | SkipReason:
    signal = _read_data_readiness_signal(context)

    if signal.ready:
        return RunRequest(
            run_key=signal.cycle_id,
            run_config={},
            tags={"cycle_id": signal.cycle_id, "phase": PhaseEnum.PHASE0.value},
        )

    policy = _gate_policy_from_context(context)
    decision = classify_gate_result(
        PhaseEnum.PHASE0,
        _DataReadinessGateEvent(
            failure_class=FailureClass.DATA_QUALITY,
            cycle_id=signal.cycle_id,
            failed_node=signal.failed_node,
            reason=signal.reason,
        ),
        policy,
    )
    summary = decision.reason or signal.reason or "data readiness signal is not ready"
    dispatch_gate_decision_alert(
        decision,
        cycle_id=signal.cycle_id,
        failed_node=signal.failed_node,
        summary=summary,
        channels=policy.alert_channels,
    )

    if signal.reason:
        return SkipReason(f"data readiness not ready: {signal.reason}")
    return SkipReason("data readiness not ready")


def _read_data_readiness_signal(
    context: SensorEvaluationContext,
) -> DataReadinessSignal:
    resource = _first_available_resource(context, _READINESS_RESOURCE_KEYS)
    if resource is None:
        raise RuntimeError("data_readiness resource is required")

    signal = _signal_from_resource(resource)
    if signal is None:
        msg = (
            "data_readiness resource must expose one of "
            f"{', '.join(_READINESS_METHOD_NAMES + _READINESS_ATTRIBUTE_NAMES)}"
        )
        raise TypeError(msg)

    return signal


def _signal_from_resource(resource: object) -> DataReadinessSignal | None:
    if isinstance(resource, DataReadinessSignal):
        return resource
    if isinstance(resource, Mapping):
        return _coerce_signal(resource)

    for method_name in _READINESS_METHOD_NAMES:
        method = getattr(resource, method_name, None)
        if callable(method):
            return _coerce_signal(method())

    for attribute_name in _READINESS_ATTRIBUTE_NAMES:
        value = getattr(resource, attribute_name, None)
        if value is not None:
            return _coerce_signal(value)

    return None


def _coerce_signal(value: object) -> DataReadinessSignal:
    if isinstance(value, DataReadinessSignal):
        return value
    if isinstance(value, Mapping):
        return DataReadinessSignal(**value)
    raise TypeError("readiness provider returned an invalid DataReadinessSignal")


def _gate_policy_from_context(context: SensorEvaluationContext) -> GatePolicyProfile:
    resource = _first_available_resource(context, ("gate_policy",))
    if isinstance(resource, GatePolicyProfile):
        return resource

    policy = getattr(resource, "policy", None)
    if isinstance(policy, GatePolicyProfile):
        return policy

    return load_gate_policy(_DEFAULT_POLICY_PATH)


def _first_available_resource(
    context: SensorEvaluationContext,
    resource_keys: tuple[str, ...],
) -> Any:
    resources = getattr(context, "resources", None)
    if resources is None:
        return None

    if isinstance(resources, Mapping):
        for resource_key in resource_keys:
            if resource_key in resources:
                return resources[resource_key]
        return None

    for resource_key in resource_keys:
        try:
            return getattr(resources, resource_key)
        except AttributeError:
            continue
    return None


__all__ = ["data_readiness_sensor"]
