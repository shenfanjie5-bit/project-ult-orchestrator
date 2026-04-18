"""Runtime gate decision models."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from orchestrator.policy import FailureClass, GateAction, PhaseEnum


@dataclass(frozen=True, slots=True)
class GateDecision:
    """Decision produced by classifying a gate failure event."""

    phase: PhaseEnum
    failure_class: FailureClass | None
    action: GateAction
    reason: str | None = None
    scenario_id: str | None = None


@dataclass(frozen=True, slots=True)
class DataReadinessSignal:
    """Readiness signal exposed by the upstream data provider."""

    ready: bool
    cycle_id: str
    reason: str | None = None
    failed_node: str = "data_readiness"


class ProviderHealthStatus(Protocol):
    """Provider/model health status exposed by reasoner-runtime providers."""

    provider: str
    model: str
    reachable: bool
    latency_ms: float | int | None
    quota_status: str
    error: str | None


class HealthCheckReport(Protocol):
    """Provider/model health report exposed by reasoner-runtime providers."""

    provider_statuses: Sequence[ProviderHealthStatus]
    all_critical_targets_available: bool
    summary: str


class LLMHealthProbe(Protocol):
    """Minimal reasoner-runtime health probe consumed by the orchestrator."""

    def check_health(self) -> HealthCheckReport:
        """Return the current provider/model health report."""
        ...


__all__ = [
    "DataReadinessSignal",
    "GateDecision",
    "HealthCheckReport",
    "LLMHealthProbe",
    "ProviderHealthStatus",
]
