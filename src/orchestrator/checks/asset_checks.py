"""Dagster AssetCheck wiring for Phase 0."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from math import isfinite
from numbers import Real

from dagster import AssetCheckResult, ResourceParam, asset_check

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.decision_handler import dispatch_gate_decision_alert
from orchestrator.checks.models import GateDecision, LLMHealthProbe
from orchestrator.checks.resources import GatePolicyResource
from orchestrator.policy import FailureClass, GateAction, PhaseEnum

_LLM_HEALTH_CHECK_NAME = "llm_health_check"
_LLM_HEALTH_FAILED_SCENARIO_ID = "phase0_llm_health_check_failed"


@dataclass(frozen=True, slots=True)
class _LLMHealthGateEvent:
    failure_class: FailureClass
    summary: str
    provider_statuses: tuple["_NormalizedProviderHealthStatus", ...]
    failed_node: str = _LLM_HEALTH_CHECK_NAME
    scenario_id: str = _LLM_HEALTH_FAILED_SCENARIO_ID


@dataclass(frozen=True, slots=True)
class _NormalizedProviderHealthStatus:
    provider: str
    model: str
    reachable: bool
    latency_ms: float | None
    quota_status: str
    error: str | None = None


@dataclass(frozen=True, slots=True)
class _NormalizedLLMHealthReport:
    provider_statuses: tuple[_NormalizedProviderHealthStatus, ...]
    all_critical_targets_available: bool
    summary: str


@asset_check(asset="phase0_readiness_ping")
def phase0_ping_check(gate_policy: GatePolicyResource) -> AssetCheckResult:
    """Minimal policy-backed check node for the Phase 0 skeleton."""

    decision = classify_gate_result(PhaseEnum.PHASE0, None, gate_policy.policy)
    return AssetCheckResult(passed=decision.action is GateAction.CONTINUE)


@asset_check(
    asset="phase0_readiness_ping",
    name=_LLM_HEALTH_CHECK_NAME,
    blocking=True,
)
def llm_health_check(
    gate_policy: GatePolicyResource,
    llm_health_probe: ResourceParam[LLMHealthProbe],
) -> AssetCheckResult:
    """Block Phase 0 when the reasoner-runtime LLM probe is unhealthy."""

    report = _read_llm_health(llm_health_probe)
    if report.all_critical_targets_available:
        return AssetCheckResult(
            passed=True,
            metadata=_llm_health_metadata(report=report),
        )

    decision = classify_gate_result(
        PhaseEnum.PHASE0,
        _LLMHealthGateEvent(
            failure_class=FailureClass.INFRA,
            summary=report.summary,
            provider_statuses=report.provider_statuses,
        ),
        gate_policy.policy,
    )
    metadata = _llm_health_metadata(report=report, decision=decision)

    if decision.action is not GateAction.CONTINUE:
        dispatch_gate_decision_alert(
            decision,
            cycle_id=_LLM_HEALTH_CHECK_NAME,
            failed_node=_LLM_HEALTH_CHECK_NAME,
            summary=decision.reason or report.summary,
            channels=gate_policy.policy.alert_channels,
        )

    return AssetCheckResult(
        passed=decision.action is GateAction.CONTINUE,
        metadata=metadata,
    )


def _read_llm_health(
    llm_health_probe: LLMHealthProbe,
) -> _NormalizedLLMHealthReport:
    check_health = getattr(llm_health_probe, "check_health", None)
    if not callable(check_health):
        return _unhealthy_probe_result(
            llm_health_probe,
            TypeError("llm_health_probe resource must expose check_health()"),
        )

    try:
        raw_result = check_health()
    except Exception as exc:
        if _is_infrastructure_unavailable_error(exc):
            raise
        return _unhealthy_probe_result(llm_health_probe, exc)

    try:
        return _coerce_llm_health_report(raw_result)
    except Exception as exc:
        return _unhealthy_probe_result(llm_health_probe, exc, raw_result=raw_result)


def _coerce_llm_health_report(value: object) -> _NormalizedLLMHealthReport:
    if _is_provider_status_sequence(value):
        provider_statuses = _coerce_provider_statuses(value)
        return _NormalizedLLMHealthReport(
            provider_statuses=provider_statuses,
            all_critical_targets_available=all(
                _provider_status_available(status) for status in provider_statuses
            ),
            summary=_summary_from_statuses(provider_statuses),
        )

    provider_statuses = _coerce_provider_statuses(
        _required_field(value, "provider_statuses"),
    )
    all_critical_targets_available = _required_field(
        value,
        "all_critical_targets_available",
    )
    summary = _required_field(value, "summary")

    if not isinstance(all_critical_targets_available, bool):
        raise TypeError(
            "llm health report all_critical_targets_available must be bool",
        )
    if not isinstance(summary, str) or not summary:
        raise TypeError("llm health report summary must be a non-empty string")

    return _NormalizedLLMHealthReport(
        provider_statuses=provider_statuses,
        all_critical_targets_available=all_critical_targets_available,
        summary=summary,
    )


def _coerce_provider_statuses(
    value: object,
) -> tuple[_NormalizedProviderHealthStatus, ...]:
    if not _is_provider_status_sequence(value):
        raise TypeError("llm health report provider_statuses must be a sequence")

    statuses = tuple(_coerce_provider_status(status) for status in value)
    if not statuses:
        raise TypeError("llm health report provider_statuses must not be empty")
    return statuses


def _coerce_provider_status(value: object) -> _NormalizedProviderHealthStatus:
    provider = _required_field(value, "provider")
    model = _required_field(value, "model")
    reachable = _required_field(value, "reachable")
    latency_ms = _optional_field(value, "latency_ms")
    quota_status = _required_field(value, "quota_status")
    error = _optional_field(value, "error")

    if not isinstance(provider, str) or not provider:
        raise TypeError("provider health status provider must be a non-empty string")
    if not isinstance(model, str) or not model:
        raise TypeError("provider health status model must be a non-empty string")
    if not isinstance(reachable, bool):
        raise TypeError("provider health status reachable must be bool")
    if latency_ms is not None:
        if isinstance(latency_ms, bool) or not isinstance(latency_ms, Real):
            raise TypeError("provider health status latency_ms must be numeric or None")
        latency_ms = float(latency_ms)
        if not isfinite(latency_ms) or latency_ms < 0:
            raise ValueError("provider health status latency_ms must be non-negative")
    if not isinstance(quota_status, str) or not quota_status:
        raise TypeError(
            "provider health status quota_status must be a non-empty string",
        )
    if error is not None and not isinstance(error, str):
        raise TypeError("provider health status error must be a string or None")

    return _NormalizedProviderHealthStatus(
        provider=provider,
        model=model,
        reachable=reachable,
        latency_ms=latency_ms,
        quota_status=quota_status,
        error=error,
    )


def _required_field(value: object, name: str) -> object:
    field_value = _optional_field(value, name, missing=...)
    if field_value is ...:
        raise TypeError(f"llm health report must include {name}")
    return field_value


def _optional_field(
    value: object,
    name: str,
    *,
    missing: object | None = None,
) -> object | None:
    if isinstance(value, Mapping):
        return value.get(name, missing)
    return getattr(value, name, missing)


def _is_provider_status_sequence(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    )


def _unhealthy_probe_result(
    llm_health_probe: LLMHealthProbe,
    exc: Exception,
    *,
    raw_result: object | None = None,
) -> _NormalizedLLMHealthReport:
    provider_status = _NormalizedProviderHealthStatus(
        provider=_provider_from_probe_or_result(llm_health_probe, raw_result)
        or "unknown",
        model=_model_from_probe_or_result(llm_health_probe, raw_result) or "unknown",
        reachable=False,
        latency_ms=None,
        quota_status="unknown",
        error=str(exc),
    )
    return _NormalizedLLMHealthReport(
        provider_statuses=(provider_status,),
        all_critical_targets_available=False,
        summary=f"llm health probe failed: {exc}",
    )


def _provider_from_probe_or_result(
    llm_health_probe: LLMHealthProbe,
    raw_result: object | None,
) -> str | None:
    for value in (raw_result, llm_health_probe):
        provider = _optional_field(value, "provider") if value is not None else None
        if isinstance(provider, str) and provider:
            return provider
    return None


def _model_from_probe_or_result(
    llm_health_probe: LLMHealthProbe,
    raw_result: object | None,
) -> str | None:
    for value in (raw_result, llm_health_probe):
        model = _optional_field(value, "model") if value is not None else None
        if isinstance(model, str) and model:
            return model
    return None


def _is_infrastructure_unavailable_error(exc: Exception) -> bool:
    from orchestrator.resources.infra import InfrastructureUnavailableError

    return isinstance(exc, InfrastructureUnavailableError)


def _llm_health_metadata(
    *,
    report: _NormalizedLLMHealthReport,
    decision: GateDecision | None = None,
) -> dict[str, str]:
    provider_statuses = report.provider_statuses
    unavailable_statuses = tuple(
        status
        for status in provider_statuses
        if not _provider_status_available(status)
    )
    providers = sorted({status.provider for status in provider_statuses})
    metadata = {
        "provider": ", ".join(providers),
        "providers": ", ".join(providers),
        "provider_models": ", ".join(_provider_model_labels(provider_statuses)),
        "provider_statuses": _provider_statuses_json(provider_statuses),
        "summary": report.summary,
        "all_critical_targets_available": str(
            report.all_critical_targets_available,
        ).lower(),
        "target_count": str(len(provider_statuses)),
        "unavailable_target_count": str(len(unavailable_statuses)),
        "unavailable_provider_models": ", ".join(
            _provider_model_labels(unavailable_statuses),
        ),
    }
    if decision is not None:
        metadata["failure_class"] = (
            decision.failure_class.value if decision.failure_class else ""
        )
        metadata["action"] = decision.action.value
        metadata["scenario_id"] = decision.scenario_id or ""
    return metadata


def _provider_status_available(status: _NormalizedProviderHealthStatus) -> bool:
    return (
        status.reachable
        and not _quota_status_blocks_availability(status.quota_status)
        and status.error is None
    )


def _quota_status_blocks_availability(quota_status: str) -> bool:
    normalized = quota_status.strip().lower().replace("-", "_")
    return normalized in {
        "blocked",
        "denied",
        "depleted",
        "error",
        "exhausted",
        "failed",
        "over_quota",
        "quota_exceeded",
        "rate_limited",
        "unavailable",
    }


def _summary_from_statuses(
    statuses: tuple[_NormalizedProviderHealthStatus, ...],
) -> str:
    unavailable = tuple(
        status for status in statuses if not _provider_status_available(status)
    )
    if not unavailable:
        return f"{len(statuses)} provider/model target(s) available"
    return (
        f"{len(statuses)} provider/model target(s) checked; "
        f"{len(unavailable)} unavailable: "
        + ", ".join(_provider_model_labels(unavailable))
    )


def _provider_model_labels(
    statuses: Sequence[_NormalizedProviderHealthStatus],
) -> tuple[str, ...]:
    return tuple(f"{status.provider}/{status.model}" for status in statuses)


def _provider_statuses_json(
    statuses: tuple[_NormalizedProviderHealthStatus, ...],
) -> str:
    return json.dumps(
        [
            {
                "provider": status.provider,
                "model": status.model,
                "reachable": status.reachable,
                "latency_ms": status.latency_ms,
                "quota_status": status.quota_status,
                "error": status.error,
            }
            for status in statuses
        ],
        separators=(",", ":"),
        sort_keys=True,
    )


__all__ = ["llm_health_check", "phase0_ping_check"]
