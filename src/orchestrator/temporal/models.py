"""Pure Python models for the optional Phase 1-3 Temporal workflow."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, TypeAlias

from orchestrator.checks.models import GateDecision
from orchestrator.policy import PhaseEnum


TemporalWorkflowStatus: TypeAlias = Literal[
    "pending",
    "running",
    "succeeded",
    "failed",
    "skipped",
    "repair_required",
]

_EMPTY_MAPPING: Mapping[str, object] = MappingProxyType({})
_EMPTY_TAGS: Mapping[str, str] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class TemporalPhaseSpec:
    """A phase execution boundary passed to a Temporal phase executor."""

    phase: PhaseEnum
    asset_selection: tuple[str, ...]
    gate_checks: tuple[str, ...] = ()
    required_resources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "asset_selection", tuple(self.asset_selection))
        object.__setattr__(self, "gate_checks", tuple(self.gate_checks))
        object.__setattr__(self, "required_resources", tuple(self.required_resources))


@dataclass(frozen=True, slots=True)
class TemporalCycleRequest:
    """Request shape for the optional Phase 1-3 workflow."""

    cycle_id: str
    dagster_run_id: str
    phase0_run_id: str
    policy_version: str
    contract_version: str
    phase_specs: tuple[TemporalPhaseSpec, ...]
    tags: Mapping[str, str] = _EMPTY_TAGS

    def __post_init__(self) -> None:
        object.__setattr__(self, "phase_specs", tuple(self.phase_specs))
        object.__setattr__(
            self,
            "tags",
            MappingProxyType(dict(self.tags)),
        )


@dataclass(frozen=True, slots=True)
class TemporalPhaseResult:
    """Result returned by a phase executor for one phase."""

    phase: PhaseEnum
    status: TemporalWorkflowStatus
    gate_decision: GateDecision | None = None
    manifest_fields: Mapping[str, object] = _EMPTY_MAPPING

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "manifest_fields",
            MappingProxyType(dict(self.manifest_fields)),
        )


@dataclass(frozen=True, slots=True)
class TemporalCycleResult:
    """Final Phase 1-3 workflow result."""

    cycle_id: str
    status: TemporalWorkflowStatus
    phase_results: tuple[TemporalPhaseResult, ...]
    manifest_fields: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "phase_results", tuple(self.phase_results))
        object.__setattr__(
            self,
            "manifest_fields",
            MappingProxyType(dict(self.manifest_fields)),
        )


__all__ = [
    "TemporalCycleRequest",
    "TemporalCycleResult",
    "TemporalPhaseResult",
    "TemporalPhaseSpec",
    "TemporalWorkflowStatus",
]
