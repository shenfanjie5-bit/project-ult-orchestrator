"""Partial rerun planning facade."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class PartialRerunPlan:
    """Minimal rerun selection for a failed node."""

    run_id: str
    failed_node: str
    rerun_selection: tuple[str, ...]
    requires_manual_ack: bool
    generated_at: datetime


def plan_partial_rerun(run_id: str, failed_node: str) -> PartialRerunPlan:
    """Generate the P1a-minimal rerun plan for a failed node."""

    if not run_id:
        raise ValueError("run_id is required")
    if not failed_node:
        raise ValueError("failed_node is required")

    return PartialRerunPlan(
        run_id=run_id,
        failed_node=failed_node,
        rerun_selection=(failed_node,),
        requires_manual_ack=True,
        generated_at=datetime.now(timezone.utc),
    )


__all__ = ["PartialRerunPlan", "plan_partial_rerun"]
