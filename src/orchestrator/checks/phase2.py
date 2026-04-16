"""Phase 2 single-stock gate adapters."""

from __future__ import annotations

from dataclasses import dataclass

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.models import GateDecision
from orchestrator.policy import FailureClass, GatePolicyProfile, PhaseEnum

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


def phase2_failure_rate(failed_count: int, total_count: int) -> float:
    """Return the failed-stock share for a Phase 2 pool."""

    if total_count <= 0:
        raise ValueError("total_count must be greater than 0")
    if failed_count < 0:
        raise ValueError("failed_count must be greater than or equal to 0")
    if failed_count > total_count:
        raise ValueError("failed_count must be less than or equal to total_count")

    return failed_count / total_count


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


def _phase2_single_stock_tolerance(policy: GatePolicyProfile) -> float:
    try:
        tolerance = policy.thresholds[_SINGLE_STOCK_TOLERANCE_KEY]
    except KeyError as exc:
        msg = f"gate policy missing thresholds.{_SINGLE_STOCK_TOLERANCE_KEY}"
        raise ValueError(msg) from exc

    if tolerance < 0 or tolerance > 1:
        msg = f"thresholds.{_SINGLE_STOCK_TOLERANCE_KEY} must be between 0 and 1"
        raise ValueError(msg)
    return tolerance


__all__ = [
    "Phase2SingleStockFailureEvent",
    "classify_phase2_single_stock_failure",
    "inconclusive_metadata",
    "phase2_failure_rate",
]
