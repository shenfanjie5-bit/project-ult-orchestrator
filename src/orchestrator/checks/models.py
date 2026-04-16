"""Runtime gate decision models."""

from __future__ import annotations

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


@dataclass(frozen=True, slots=True)
class DataReadinessSignal:
    """Readiness signal exposed by the upstream data provider."""

    ready: bool
    cycle_id: str
    reason: str | None = None
    failed_node: str = "data_readiness"


class LLMHealthResult(Protocol):
    """Health result shape exposed by reasoner-runtime providers."""

    healthy: bool
    summary: str
    provider: str | None


class LLMHealthProbe(Protocol):
    """Minimal reasoner-runtime health probe consumed by the orchestrator."""

    def check_health(self) -> LLMHealthResult:
        """Return the current LLM provider health."""
        ...


__all__ = [
    "DataReadinessSignal",
    "GateDecision",
    "LLMHealthProbe",
    "LLMHealthResult",
]
