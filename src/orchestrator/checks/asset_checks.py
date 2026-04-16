"""Dagster AssetCheck wiring for Phase 0."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dagster import AssetCheckResult, ResourceParam, asset_check

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.decision_handler import dispatch_gate_decision_alert
from orchestrator.checks.models import GateDecision, LLMHealthProbe
from orchestrator.checks.resources import GatePolicyResource
from orchestrator.policy import FailureClass, GateAction, PhaseEnum

_LLM_HEALTH_CHECK_NAME = "llm_health_check"


@dataclass(frozen=True, slots=True)
class _LLMHealthGateEvent:
    failure_class: FailureClass
    provider: str | None
    summary: str
    failed_node: str = _LLM_HEALTH_CHECK_NAME


@dataclass(frozen=True, slots=True)
class _NormalizedLLMHealthResult:
    healthy: bool
    summary: str
    provider: str | None = None


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

    health = _read_llm_health(llm_health_probe)
    if health.healthy:
        return AssetCheckResult(
            passed=True,
            metadata=_llm_health_metadata(health=health),
        )

    decision = classify_gate_result(
        PhaseEnum.PHASE0,
        _LLMHealthGateEvent(
            failure_class=FailureClass.INFRA,
            provider=health.provider,
            summary=health.summary,
        ),
        gate_policy.policy,
    )
    metadata = _llm_health_metadata(health=health, decision=decision)

    if decision.action is not GateAction.CONTINUE:
        dispatch_gate_decision_alert(
            decision,
            cycle_id=_LLM_HEALTH_CHECK_NAME,
            failed_node=_LLM_HEALTH_CHECK_NAME,
            summary=decision.reason or health.summary,
            channels=gate_policy.policy.alert_channels,
        )

    return AssetCheckResult(
        passed=decision.action is GateAction.CONTINUE,
        metadata=metadata,
    )


def _read_llm_health(
    llm_health_probe: LLMHealthProbe,
) -> _NormalizedLLMHealthResult:
    check_health = getattr(llm_health_probe, "check_health", None)
    if not callable(check_health):
        raise TypeError("llm_health_probe resource must expose check_health()")

    return _coerce_llm_health_result(check_health())


def _coerce_llm_health_result(value: object) -> _NormalizedLLMHealthResult:
    if isinstance(value, Mapping):
        healthy = value.get("healthy")
        summary = value.get("summary")
        provider = value.get("provider")
    else:
        healthy = getattr(value, "healthy", None)
        summary = getattr(value, "summary", None)
        provider = getattr(value, "provider", None)

    if not isinstance(healthy, bool):
        raise TypeError("llm health result healthy must be bool")
    if not isinstance(summary, str) or not summary:
        raise TypeError("llm health result summary must be a non-empty string")
    if provider is not None and not isinstance(provider, str):
        raise TypeError("llm health result provider must be a string or None")

    return _NormalizedLLMHealthResult(
        healthy=healthy,
        summary=summary,
        provider=provider,
    )


def _llm_health_metadata(
    *,
    health: _NormalizedLLMHealthResult,
    decision: GateDecision | None = None,
) -> dict[str, str]:
    metadata = {
        "provider": health.provider or "",
        "summary": health.summary,
    }
    if decision is not None:
        metadata["failure_class"] = (
            decision.failure_class.value if decision.failure_class else ""
        )
        metadata["action"] = decision.action.value
    return metadata


__all__ = ["llm_health_check", "phase0_ping_check"]
