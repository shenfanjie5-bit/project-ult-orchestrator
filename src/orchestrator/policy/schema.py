"""Pydantic schema for gate policy profiles."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator

from orchestrator.policy.contracts_adapter import FailureClass, GateAction, PhaseEnum


class PhaseMatrixEntry(BaseModel):
    """One phase/failure-class decision row from the gate policy matrix."""

    model_config = ConfigDict(extra="forbid")

    phase: PhaseEnum
    failure_class: FailureClass
    action: GateAction
    allow_partial_rerun: bool
    description: str


class GatePolicyProfile(BaseModel):
    """Versioned gate policy loaded from audited configuration."""

    model_config = ConfigDict(extra="forbid")

    policy_version: str
    contract_version: str
    execution_backend: Literal["dagster_only", "dagster_plus_temporal"]
    phase_matrix: list[PhaseMatrixEntry]
    thresholds: dict[str, float]
    alert_channels: list[str]
    updated_at: datetime

    @model_validator(mode="after")
    def reject_duplicate_phase_matrix_entries(self) -> "GatePolicyProfile":
        seen: set[tuple[PhaseEnum, FailureClass]] = set()
        for entry in self.phase_matrix:
            key = (entry.phase, entry.failure_class)
            if key in seen:
                msg = (
                    "duplicate phase_matrix entry for "
                    f"phase={entry.phase.value} "
                    f"failure_class={entry.failure_class.value}"
                )
                raise ValueError(msg)
            seen.add(key)
        return self


__all__ = ["GatePolicyProfile", "PhaseMatrixEntry"]
