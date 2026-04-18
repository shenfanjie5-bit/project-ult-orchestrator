"""Phase 2 gate adapters."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from math import isfinite
from typing import Protocol

from dagster import AssetCheckResult, ResourceParam, asset_check

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.decision_handler import dispatch_gate_decision_alert
from orchestrator.checks.models import GateDecision
# Import GatePolicyResource + Dagster ResourceParam at module level so the
# @asset_check decorator can resolve string annotations under
# `from __future__ import annotations`. Without this, Dagster 1.9 raises
# DagsterInvalidDefinitionError trying to resolve these from function-local scope.
from orchestrator.checks.resources import GatePolicyResource
from orchestrator.policy import FailureClass, GateAction, GatePolicyProfile, PhaseEnum

_POOL_FAILURE_RATE_KEY = "phase2_pool_failure_rate"
_SINGLE_STOCK_TOLERANCE_KEY = "phase2_single_stock_tolerance"
PHASE2_POOL_FAILURE_RATE_CHECK_NAME = "phase2_pool_failure_rate_gate"
PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY = "phase2_pool_failure_rate"


@dataclass(frozen=True, slots=True, kw_only=True)
class Phase2SingleStockFailureEvent:
    """Normalized Phase 2 single-stock task failure event."""

    failure_class: FailureClass = FailureClass.TASK_LEVEL
    stock_id: str
    failed_node: str
    failed_count: int
    total_count: int
    reason: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class Phase2PoolFailureRateEvent:
    """Normalized Phase 2 pool-level failure rate event."""

    failure_class: FailureClass = FailureClass.DATA_QUALITY
    failed_count: int
    total_count: int
    failed_nodes: tuple[str, ...]
    reason: str | None = None


class Phase2PoolFailureRateProvider(Protocol):
    """Provider resource contract for the production Phase 2 pool gate."""

    def get_phase2_pool_failure_rate_event(self) -> Phase2PoolFailureRateEvent:
        """Return the current Phase 2 pool failure-rate event."""


def phase2_failure_rate(failed_count: int, total_count: int) -> float:
    """Return the failed-stock share for a Phase 2 pool."""

    if total_count <= 0:
        raise ValueError("total_count must be greater than 0")
    if failed_count < 0:
        raise ValueError("failed_count must be greater than or equal to 0")
    if failed_count > total_count:
        raise ValueError("failed_count must be less than or equal to total_count")

    return failed_count / total_count


def classify_phase2_pool_failure_rate(
    event: Phase2PoolFailureRateEvent,
    policy: GatePolicyProfile,
) -> GateDecision:
    """Fail Phase 2 when the pool-level failure rate exceeds policy."""

    failure_rate = phase2_failure_rate(event.failed_count, event.total_count)
    threshold = _phase2_threshold(policy, _POOL_FAILURE_RATE_KEY)
    if failure_rate > threshold:
        return classify_gate_result(PhaseEnum.PHASE2, event, policy)

    return GateDecision(
        phase=PhaseEnum.PHASE2,
        failure_class=event.failure_class,
        action=GateAction.CONTINUE,
        reason=event.reason,
    )


def classify_phase2_single_stock_failure(
    event: Phase2SingleStockFailureEvent,
    policy: GatePolicyProfile,
) -> GateDecision:
    """Classify an in-tolerance Phase 2 stock failure as inconclusive."""

    failure_rate = phase2_failure_rate(event.failed_count, event.total_count)
    tolerance = _phase2_single_stock_tolerance(policy)
    if failure_rate > tolerance:
        msg = (
            "phase2 single-stock failure rate exceeds "
            f"{_SINGLE_STOCK_TOLERANCE_KEY}; use the pool threshold gate"
        )
        raise ValueError(msg)

    return classify_gate_result(PhaseEnum.PHASE2, event, policy)


def inconclusive_metadata(
    event: Phase2SingleStockFailureEvent,
    decision: GateDecision,
) -> dict[str, str | float]:
    """Build Dagster AssetCheckResult metadata for an inconclusive stock."""

    metadata: dict[str, str | float] = {
        "phase": decision.phase.value,
        "failure_class": (
            decision.failure_class.value if decision.failure_class is not None else ""
        ),
        "action": decision.action.value,
        "scenario_id": decision.scenario_id or "",
        "stock_id": event.stock_id,
        "failed_node": event.failed_node,
        "failure_rate": phase2_failure_rate(event.failed_count, event.total_count),
    }
    if event.reason is not None:
        metadata["reason"] = event.reason
    return metadata


def dispatch_phase2_pool_failure_alert(
    decision: GateDecision,
    event: Phase2PoolFailureRateEvent,
    cycle_id: str,
    channels: Iterable[str],
) -> None:
    """Dispatch the shared gate-decision alert for a Phase 2 pool failure."""

    failure_rate = phase2_failure_rate(event.failed_count, event.total_count)
    summary = (
        f"Phase 2 pool failure rate {failure_rate:.6g} "
        f"({event.failed_count}/{event.total_count})"
    )
    if event.reason:
        summary = f"{summary}: {event.reason}"

    dispatch_gate_decision_alert(
        decision,
        cycle_id=cycle_id,
        failed_node=", ".join(event.failed_nodes) if event.failed_nodes else None,
        summary=summary,
        channels=channels,
    )


def build_phase2_pool_failure_rate_check(asset: object) -> object:
    """Build the production Dagster AssetCheck for the Phase 2 pool gate."""

    @asset_check(
        asset=asset,
        name=PHASE2_POOL_FAILURE_RATE_CHECK_NAME,
        blocking=True,
    )
    def phase2_pool_failure_rate_gate(
        context: object,
        gate_policy: GatePolicyResource,
        phase2_pool_failure_rate: ResourceParam[Phase2PoolFailureRateProvider],
    ) -> AssetCheckResult:
        event = _read_phase2_pool_failure_rate_event(phase2_pool_failure_rate)
        decision = classify_phase2_pool_failure_rate(event, gate_policy.policy)
        dispatch_phase2_pool_failure_alert(
            decision,
            event,
            cycle_id=_cycle_id_from_context(context),
            channels=gate_policy.policy.alert_channels,
        )

        return AssetCheckResult(
            passed=decision.action is GateAction.CONTINUE,
            metadata=_phase2_pool_failure_metadata(event, decision),
        )

    return phase2_pool_failure_rate_gate


def _read_phase2_pool_failure_rate_event(
    provider: Phase2PoolFailureRateProvider,
) -> Phase2PoolFailureRateEvent:
    read_event = getattr(provider, "get_phase2_pool_failure_rate_event", None)
    if not callable(read_event):
        msg = (
            f"{PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY} resource must expose "
            "get_phase2_pool_failure_rate_event()"
        )
        raise TypeError(msg)

    raw_event = read_event()
    if isinstance(raw_event, Phase2PoolFailureRateEvent):
        return raw_event
    if isinstance(raw_event, Mapping):
        return _phase2_pool_failure_event_from_mapping(raw_event)

    msg = (
        "get_phase2_pool_failure_rate_event() must return "
        "Phase2PoolFailureRateEvent or a mapping"
    )
    raise TypeError(msg)


def _phase2_pool_failure_event_from_mapping(
    raw_event: Mapping[str, object],
) -> Phase2PoolFailureRateEvent:
    failed_nodes = raw_event.get("failed_nodes", ())
    if not isinstance(failed_nodes, Iterable) or isinstance(failed_nodes, str):
        msg = "phase2 pool failed_nodes must be an iterable of strings"
        raise TypeError(msg)

    return Phase2PoolFailureRateEvent(
        failed_count=_required_int(raw_event, "failed_count"),
        total_count=_required_int(raw_event, "total_count"),
        failed_nodes=tuple(_required_strings(failed_nodes, "failed_nodes")),
        reason=_optional_string(raw_event.get("reason"), "reason"),
    )


def _required_int(raw_event: Mapping[str, object], key: str) -> int:
    value = raw_event.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        msg = f"phase2 pool {key} must be an integer"
        raise TypeError(msg)
    return value


def _required_strings(values: Iterable[object], key: str) -> tuple[str, ...]:
    strings: list[str] = []
    for value in values:
        if not isinstance(value, str):
            msg = f"phase2 pool {key} must contain only strings"
            raise TypeError(msg)
        strings.append(value)
    return tuple(strings)


def _optional_string(value: object, key: str) -> str | None:
    if value is None or isinstance(value, str):
        return value
    msg = f"phase2 pool {key} must be a string or None"
    raise TypeError(msg)


def _phase2_pool_failure_metadata(
    event: Phase2PoolFailureRateEvent,
    decision: GateDecision,
) -> dict[str, str | int | float]:
    return {
        "phase": decision.phase.value,
        "failure_class": (
            decision.failure_class.value if decision.failure_class is not None else ""
        ),
        "action": decision.action.value,
        "scenario_id": decision.scenario_id or "",
        "failed_count": event.failed_count,
        "total_count": event.total_count,
        "failure_rate": phase2_failure_rate(event.failed_count, event.total_count),
        "failed_nodes": ", ".join(event.failed_nodes),
    }


def _cycle_id_from_context(context: object) -> str:
    dagster_run = getattr(context, "dagster_run", None)
    if dagster_run is None:
        dagster_run = getattr(context, "run", None)
    tags = getattr(dagster_run, "tags", {}) or {}
    cycle_id = tags.get("cycle_id")
    if isinstance(cycle_id, str) and cycle_id:
        return cycle_id

    run_id = getattr(context, "run_id", None)
    return run_id if isinstance(run_id, str) and run_id else "unknown-phase2-run"


def _phase2_single_stock_tolerance(policy: GatePolicyProfile) -> float:
    return _phase2_threshold(policy, _SINGLE_STOCK_TOLERANCE_KEY)


def _phase2_threshold(policy: GatePolicyProfile, key: str) -> float:
    try:
        threshold = policy.thresholds[key]
    except KeyError as exc:
        msg = f"gate policy missing thresholds.{key}"
        raise ValueError(msg) from exc

    if not isfinite(threshold) or threshold < 0 or threshold > 1:
        msg = f"thresholds.{key} must be finite and between 0 and 1"
        raise ValueError(msg)
    return threshold


__all__ = [
    "PHASE2_POOL_FAILURE_RATE_CHECK_NAME",
    "PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY",
    "Phase2PoolFailureRateEvent",
    "Phase2PoolFailureRateProvider",
    "Phase2SingleStockFailureEvent",
    "build_phase2_pool_failure_rate_check",
    "classify_phase2_pool_failure_rate",
    "classify_phase2_single_stock_failure",
    "dispatch_phase2_pool_failure_alert",
    "inconclusive_metadata",
    "phase2_failure_rate",
]
