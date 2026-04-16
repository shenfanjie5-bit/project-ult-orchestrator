"""Pure snapshot model for backend parity assertions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from orchestrator.checks.models import GateDecision


@dataclass(frozen=True, slots=True)
class CycleParitySnapshot:
    """Normalized cycle outcome used to compare execution backends."""

    cycle_id: str
    phase_statuses: Mapping[str, str]
    gate_decisions: tuple[GateDecision, ...]
    manifest_fields: Mapping[str, object]
    alert_payloads: tuple[Mapping[str, object], ...]
    diagnostic_fields: Mapping[str, object]
    rerun_request_statuses: Mapping[str, str]


__all__ = ["CycleParitySnapshot"]
