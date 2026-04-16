"""Pydantic schema for gate policy profiles."""

from collections.abc import Mapping
from datetime import datetime
from math import isfinite
from numbers import Real
from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from orchestrator.policy.contracts_adapter import FailureClass, GateAction, PhaseEnum


RerunMode: TypeAlias = Literal["repair_only", "asset_only", "phase_only"]
REQUIRED_GATE_MATRIX_SCENARIOS: tuple[str, ...] = (
    "phase0_data_readiness_delayed",
    "phase0_llm_health_check_failed",
    "phase0_dbt_test_failed",
    "phase1_graph_promotion_snapshot_failed",
    "phase2_single_stock_task_failed",
    "phase2_pool_failure_rate_exceeded",
    "phase3_formal_commit_failed",
    "phase3_manifest_write_failed",
    "infra_unavailable_hard_stop",
)
_REQUIRED_PHASE2_THRESHOLDS: tuple[str, ...] = (
    "phase2_pool_failure_rate",
    "phase2_single_stock_tolerance",
)


class PhaseMatrixEntry(BaseModel):
    """One phase/failure-class decision row from the gate policy matrix."""

    model_config = ConfigDict(extra="forbid")

    scenario_id: str
    phase: PhaseEnum
    failure_class: FailureClass
    action: GateAction
    allow_partial_rerun: bool
    description: str
    applies_to_phases: tuple[PhaseEnum, ...] | None = None
    rerun_mode: RerunMode | None = None

    @field_validator("scenario_id")
    @classmethod
    def require_scenario_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("phase_matrix scenario_id must be a non-empty string")
        return value


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

    @field_validator("thresholds", mode="before")
    @classmethod
    def validate_required_thresholds(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            raise ValueError("thresholds must be a mapping")

        missing = tuple(
            threshold_key
            for threshold_key in _REQUIRED_PHASE2_THRESHOLDS
            if threshold_key not in value
        )
        if missing:
            missing_paths = ", ".join(f"thresholds.{key}" for key in missing)
            raise ValueError(f"missing required threshold(s): {missing_paths}")

        for threshold_key in _REQUIRED_PHASE2_THRESHOLDS:
            threshold = value[threshold_key]
            if isinstance(threshold, bool) or not isinstance(threshold, Real):
                raise ValueError(_threshold_error(threshold_key))
            numeric_threshold = float(threshold)
            if (
                not isfinite(numeric_threshold)
                or numeric_threshold < 0
                or numeric_threshold > 1
            ):
                raise ValueError(_threshold_error(threshold_key))

        return value

    @field_validator("alert_channels", mode="before")
    @classmethod
    def validate_alert_channels(cls, value: object) -> object:
        if not isinstance(value, list) or not value:
            raise ValueError("alert_channels must be a non-empty list of strings")

        for channel in value:
            if not isinstance(channel, str) or not channel.strip():
                raise ValueError("alert_channels must contain only non-empty strings")

        return value

    @model_validator(mode="after")
    def reject_duplicate_phase_matrix_entries(self) -> "GatePolicyProfile":
        seen_entries: set[tuple[PhaseEnum, FailureClass, str]] = set()
        seen_scenarios: set[str] = set()
        for entry in self.phase_matrix:
            key = (entry.phase, entry.failure_class, entry.scenario_id)
            if key in seen_entries:
                msg = (
                    "duplicate phase_matrix entry for "
                    f"phase={entry.phase.value} "
                    f"failure_class={entry.failure_class.value} "
                    f"scenario_id={entry.scenario_id}"
                )
                raise ValueError(msg)
            seen_entries.add(key)

            if entry.scenario_id in seen_scenarios:
                raise ValueError(
                    f"duplicate phase_matrix scenario_id={entry.scenario_id}",
                )
            seen_scenarios.add(entry.scenario_id)

            applies_to_phases = entry.applies_to_phases
            if applies_to_phases and entry.phase not in applies_to_phases:
                msg = (
                    "phase_matrix applies_to_phases must include "
                    f"phase={entry.phase.value} "
                    f"for scenario_id={entry.scenario_id}"
                )
                raise ValueError(msg)

        required_scenarios = set(REQUIRED_GATE_MATRIX_SCENARIOS)
        if seen_scenarios != required_scenarios:
            missing = sorted(required_scenarios - seen_scenarios)
            unexpected = sorted(seen_scenarios - required_scenarios)
            details: list[str] = []
            if missing:
                details.append("missing=" + ", ".join(missing))
            if unexpected:
                details.append("unexpected=" + ", ".join(unexpected))
            raise ValueError(
                "gate matrix scenarios must match required set: "
                + "; ".join(details),
            )

        return self


def _threshold_error(threshold_key: str) -> str:
    return f"thresholds.{threshold_key} must be a finite number between 0 and 1"


__all__ = [
    "GatePolicyProfile",
    "PhaseMatrixEntry",
    "REQUIRED_GATE_MATRIX_SCENARIOS",
    "RerunMode",
]
