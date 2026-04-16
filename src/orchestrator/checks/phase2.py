"""Phase 2 gate adapters."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from math import isfinite

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.decision_handler import dispatch_gate_decision_alert
from orchestrator.checks.models import GateDecision
from orchestrator.policy import FailureClass, GateAction, GatePolicyProfile, PhaseEnum

_POOL_FAILURE_RATE_KEY = "phase2_pool_failure_rate"
_SINGLE_STOCK_TOLERANCE_KEY = "phase2_single_stock_tolerance"


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
    "Phase2PoolFailureRateEvent",
    "Phase2SingleStockFailureEvent",
    "classify_phase2_pool_failure_rate",
    "classify_phase2_single_stock_failure",
    "dispatch_phase2_pool_failure_alert",
    "inconclusive_metadata",
    "phase2_failure_rate",
]
