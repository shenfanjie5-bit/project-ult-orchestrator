"""Temporal handoff helpers for Phase 1-3 execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, Protocol, TypeAlias

from orchestrator.policy import GatePolicyProfile
from orchestrator.temporal.models import TemporalCycleRequest
from orchestrator.temporal.phase_specs import default_phase1_3_specs


TEMPORAL_HANDOFF_CLIENT_RESOURCE_KEY = "temporal_handoff_client"
TemporalFailoverMode: TypeAlias = Literal["deferred", "failed_over"]
TemporalHandoffStatus: TypeAlias = Literal["started", "deferred", "failed_over"]


class TemporalHandoffStartError(RuntimeError):
    """Controlled error raised when a Temporal cycle cannot be started."""


@dataclass(frozen=True, slots=True)
class TemporalHandoffResult:
    status: TemporalHandoffStatus
    workflow_id: str | None = None
    run_id: str | None = None
    reason: str | None = None


class TemporalHandoffClient(Protocol):
    def start_cycle(self, request: TemporalCycleRequest) -> TemporalHandoffResult:
        """Start the Phase 1-3 Temporal cycle."""


@dataclass(frozen=True, slots=True)
class DefaultTemporalHandoffClient:
    """Non-network fallback used when assembly has not injected a client."""

    failover_mode: TemporalFailoverMode = "failed_over"
    reason: str = "temporal_handoff_client resource is not configured"

    def start_cycle(self, request: TemporalCycleRequest) -> TemporalHandoffResult:
        del request
        return TemporalHandoffResult(status=self.failover_mode, reason=self.reason)


def build_temporal_cycle_request_from_phase0_run(
    *,
    policy: GatePolicyProfile,
    cycle_id: str,
    phase0_run_id: str,
    dagster_run_id: str,
    tags: Mapping[str, str],
) -> TemporalCycleRequest:
    """Convert a successful Phase 0 Dagster run into a Temporal request."""

    return TemporalCycleRequest(
        cycle_id=_required_text(cycle_id, "cycle_id"),
        dagster_run_id=_required_text(dagster_run_id, "dagster_run_id"),
        phase0_run_id=_required_text(phase0_run_id, "phase0_run_id"),
        policy_version=policy.policy_version,
        contract_version=policy.contract_version,
        phase_specs=default_phase1_3_specs(),
        tags=_request_tags(
            tags,
            cycle_id=cycle_id,
            phase0_run_id=phase0_run_id,
            dagster_run_id=dagster_run_id,
            policy=policy,
        ),
    )


def start_temporal_handoff(
    *,
    policy: GatePolicyProfile,
    phase0_run_id: str,
    dagster_run_id: str,
    cycle_id: str,
    tags: Mapping[str, str],
    client: TemporalHandoffClient,
) -> TemporalHandoffResult:
    """Start Temporal Phase 1-3 execution through the injected client."""

    request = build_temporal_cycle_request_from_phase0_run(
        policy=policy,
        cycle_id=cycle_id,
        phase0_run_id=phase0_run_id,
        dagster_run_id=dagster_run_id,
        tags=tags,
    )
    result = client.start_cycle(request)
    if result.status not in ("started", "deferred", "failed_over"):
        raise TemporalHandoffStartError(
            f"temporal handoff client returned unsupported status {result.status!r}",
        )
    return result


def _request_tags(
    tags: Mapping[str, str],
    *,
    cycle_id: str,
    phase0_run_id: str,
    dagster_run_id: str,
    policy: GatePolicyProfile,
) -> dict[str, str]:
    request_tags = dict(tags)
    request_tags.setdefault("cycle_id", cycle_id)
    request_tags.setdefault("phase0_run_id", phase0_run_id)
    request_tags.setdefault("dagster_run_id", dagster_run_id)
    request_tags.setdefault("policy_version", policy.policy_version)
    request_tags.setdefault("contract_version", policy.contract_version)
    return request_tags


def _required_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


__all__ = [
    "DefaultTemporalHandoffClient",
    "TEMPORAL_HANDOFF_CLIENT_RESOURCE_KEY",
    "TemporalFailoverMode",
    "TemporalHandoffClient",
    "TemporalHandoffResult",
    "TemporalHandoffStartError",
    "build_temporal_cycle_request_from_phase0_run",
    "start_temporal_handoff",
]
