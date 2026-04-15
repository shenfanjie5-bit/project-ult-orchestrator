"""Pydantic schema for gate policy profiles."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

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


__all__ = ["GatePolicyProfile", "PhaseMatrixEntry"]
