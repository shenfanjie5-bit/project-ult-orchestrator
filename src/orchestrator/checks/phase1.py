"""Phase 1 graph promotion gate adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.decision_handler import dispatch_gate_decision_alert
from orchestrator.checks.models import GateDecision
from orchestrator.policy import FailureClass, GateAction, GatePolicyProfile, PhaseEnum

_PHASE1_GRAPH_PROMOTION_ASSET_KEY = "graph_promotion"
_PHASE1_GRAPH_SNAPSHOT_ASSET_KEY = "graph_snapshot"
_PHASE1_GRAPH_FAILURE_NODES = frozenset(
    {
        _PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        _PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
    },
)


@dataclass(frozen=True, slots=True, kw_only=True)
class GraphPromotionFailureEvent:
    """Normalized Phase 1 graph promotion or snapshot failure event."""

    failure_class: FailureClass = FailureClass.PUBLISH
    failed_node: str
    snapshot_id: str | None = None
    reason: str | None = None


def classify_graph_promotion_failure(
    event: GraphPromotionFailureEvent | Mapping[str, object],
    policy: GatePolicyProfile,
) -> GateDecision:
    """Classify a graph promotion/snapshot failure as a Phase 1 gate decision."""

    return classify_gate_result(PhaseEnum.PHASE1, event, policy)


def should_advance_ready_graph(decision: GateDecision) -> bool:
    """Return whether a ready graph marker can be advanced for the decision."""

    return decision.action is GateAction.CONTINUE


def build_phase1_graph_failure_gate_hook() -> object:
    """Build a Dagster failure hook for Phase 1 graph promotion assets."""

    from dagster import failure_hook

    @failure_hook(required_resource_keys={"gate_policy"})
    def phase1_graph_failure_gate(context: object) -> None:
        event = _graph_failure_event_from_hook_context(context)
        if event is None:
            return

        policy = _policy_from_hook_context(context)
        decision = classify_graph_promotion_failure(event, policy)
        dispatch_gate_decision_alert(
            decision,
            cycle_id=_cycle_id_from_hook_context(context),
            failed_node=event.failed_node,
            summary=event.reason or decision.reason or "Phase 1 graph failure",
            channels=policy.alert_channels,
        )

    return phase1_graph_failure_gate


def _graph_failure_event_from_hook_context(
    context: object,
) -> GraphPromotionFailureEvent | None:
    failed_node = _failed_node_from_hook_context(context)
    if failed_node is None:
        return None

    return GraphPromotionFailureEvent(
        failed_node=failed_node,
        snapshot_id=_snapshot_id_from_hook_context(context),
        reason=_reason_from_hook_context(context),
    )


def _failed_node_from_hook_context(context: object) -> str | None:
    op = getattr(context, "op", None)
    for value in (
        getattr(op, "name", None),
        getattr(context, "op_name", None),
        getattr(context, "step_key", None),
    ):
        failed_node = _phase1_graph_node_name(value)
        if failed_node is not None:
            return failed_node
    return None


def _phase1_graph_node_name(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    if value in _PHASE1_GRAPH_FAILURE_NODES:
        return value
    return None


def _snapshot_id_from_hook_context(context: object) -> str | None:
    tags = _hook_context_tags(context)
    snapshot_id = tags.get("snapshot_id")
    return snapshot_id if isinstance(snapshot_id, str) and snapshot_id else None


def _reason_from_hook_context(context: object) -> str | None:
    exception = getattr(context, "op_exception", None)
    if exception is None:
        return None

    message = str(exception)
    if message:
        return f"{type(exception).__name__}: {message}"
    return type(exception).__name__


def _policy_from_hook_context(context: object) -> GatePolicyProfile:
    resources = getattr(context, "resources", None)
    gate_policy_resource = getattr(resources, "gate_policy", None)
    policy = getattr(gate_policy_resource, "policy", gate_policy_resource)
    if not isinstance(policy, GatePolicyProfile):
        msg = "gate_policy resource must expose a GatePolicyProfile policy"
        raise TypeError(msg)
    return policy


def _cycle_id_from_hook_context(context: object) -> str:
    tags = _hook_context_tags(context)
    cycle_id = tags.get("cycle_id")
    if isinstance(cycle_id, str) and cycle_id:
        return cycle_id

    run_id = getattr(context, "run_id", None)
    if isinstance(run_id, str) and run_id:
        return run_id

    run = getattr(context, "run", None)
    run_id = getattr(run, "run_id", None) if run is not None else None
    if isinstance(run_id, str) and run_id:
        return run_id

    return "unknown-phase1-run"


def _hook_context_tags(context: object) -> Mapping[str, Any]:
    step_context = getattr(context, "_step_execution_context", None)
    for container in (
        context,
        step_context,
        getattr(context, "run", None),
        getattr(context, "dagster_run", None),
        getattr(step_context, "dagster_run", None),
    ):
        if container is None:
            continue
        for attribute_name in ("run_tags", "tags"):
            tags = getattr(container, attribute_name, None)
            if isinstance(tags, Mapping):
                return tags
    return {}


__all__ = [
    "GraphPromotionFailureEvent",
    "build_phase1_graph_failure_gate_hook",
    "classify_graph_promotion_failure",
    "should_advance_ready_graph",
]
