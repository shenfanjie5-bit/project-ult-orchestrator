"""Phase 3 formal commit and publish manifest gate adapters."""

from __future__ import annotations

from dataclasses import dataclass

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.models import GateDecision
from orchestrator.policy import FailureClass, GatePolicyProfile, PhaseEnum
from orchestrator.rerun import (
    PartialRerunPlan,
    RunHistorySnapshot,
    compute_partial_rerun_plan,
)

PHASE3_MANIFEST_ASSET_KEY = "cycle_publish_manifest"


@dataclass(frozen=True, slots=True, kw_only=True)
class FormalCommitFailureEvent:
    """Normalized Phase 3 formal object commit failure event."""

    failure_class: FailureClass = FailureClass.PUBLISH
    failed_node: str
    table_name: str | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class ManifestWriteFailureEvent:
    """Normalized Phase 3 publish manifest write failure event."""

    failure_class: FailureClass = FailureClass.INFRA
    failed_node: str = PHASE3_MANIFEST_ASSET_KEY
    repair_node: str
    reason: str | None = None


def classify_formal_commit_failure(
    event: FormalCommitFailureEvent,
    policy: GatePolicyProfile,
) -> GateDecision:
    """Classify a formal commit failure as a Phase 3 publish gate decision."""

    return classify_gate_result(PhaseEnum.PHASE3, event, policy)


def classify_manifest_write_failure(
    event: ManifestWriteFailureEvent,
    policy: GatePolicyProfile,
) -> GateDecision:
    """Classify a manifest write failure as a Phase 3 repair gate decision."""

    return classify_gate_result(PhaseEnum.PHASE3, event, policy)


def plan_manifest_repair_rerun(
    run_id: str,
    event: ManifestWriteFailureEvent,
    policy: GatePolicyProfile,
) -> PartialRerunPlan:
    """Plan a repair-only rerun for the Phase 3 publish manifest asset."""

    if not event.repair_node.strip():
        raise ValueError("repair_node is required")

    run_history = RunHistorySnapshot(
        run_id=run_id,
        node_to_phase={
            event.failed_node: PhaseEnum.PHASE3,
            event.repair_node: PhaseEnum.PHASE3,
        },
        node_dependencies={
            event.failed_node: (),
            event.repair_node: (),
        },
        failed_nodes=(event.failed_node,),
        repairable_nodes={event.failed_node: event.repair_node},
        node_failure_classes={event.failed_node: event.failure_class},
    )
    return compute_partial_rerun_plan(run_id, event.failed_node, run_history, policy)


__all__ = [
    "FormalCommitFailureEvent",
    "ManifestWriteFailureEvent",
    "classify_formal_commit_failure",
    "classify_manifest_write_failure",
    "plan_manifest_repair_rerun",
]
