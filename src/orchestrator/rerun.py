"""Pure partial rerun selection planner."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone

from orchestrator.policy import FailureClass, GateAction, GatePolicyProfile, PhaseEnum
from orchestrator.policy.schema import PhaseMatrixEntry, RerunMode


class PartialRerunNotAllowed(Exception):
    """Raised when policy or run history forbids a partial rerun."""


class UnknownFailedNode(Exception):
    """Raised when a failed node is absent from the run history snapshot."""


@dataclass(frozen=True, slots=True)
class PartialRerunPlan:
    """Minimal rerun selection for a failed node."""

    run_id: str
    failed_node: str
    rerun_selection: tuple[str, ...]
    requires_manual_ack: bool
    generated_at: datetime
    rerun_mode: RerunMode


@dataclass(frozen=True, slots=True)
class RunHistorySnapshot:
    """Pure run graph snapshot used to compute a rerun selection."""

    run_id: str
    node_to_phase: Mapping[str, PhaseEnum]
    node_dependencies: Mapping[str, tuple[str, ...]]
    failed_nodes: tuple[str, ...]
    repairable_nodes: Mapping[str, str]
    node_failure_classes: Mapping[str, FailureClass | str] | None = None


def compute_partial_rerun_plan(
    run_id: str,
    failed_node: str,
    run_history: RunHistorySnapshot,
    policy: GatePolicyProfile,
) -> PartialRerunPlan:
    """Compute a minimal partial rerun plan without side effects."""

    _validate_inputs(run_id, failed_node)
    if run_history.run_id != run_id:
        msg = (
            "run history snapshot does not match requested run_id: "
            f"requested={run_id} snapshot={run_history.run_id}"
        )
        raise PartialRerunNotAllowed(msg)
    if failed_node not in run_history.node_to_phase:
        raise UnknownFailedNode(
            f"unknown failed node for run_id={run_id}: failed_node={failed_node}"
        )
    if failed_node not in run_history.failed_nodes:
        msg = (
            "failed_node is not marked failed in run history: "
            f"run_id={run_id} failed_node={failed_node}"
        )
        raise PartialRerunNotAllowed(msg)

    phase = run_history.node_to_phase[failed_node]
    entry = _policy_entry_for_failed_node(phase, failed_node, run_history, policy)
    if not entry.allow_partial_rerun:
        msg = (
            "partial rerun is not allowed by policy: "
            f"run_id={run_id} failed_node={failed_node} "
            f"phase={phase.value} failure_class={entry.failure_class.value}"
        )
        raise PartialRerunNotAllowed(msg)

    rerun_mode = _rerun_mode_for_entry(entry)
    if rerun_mode == "repair_only":
        rerun_selection = _repair_only_selection(run_id, failed_node, run_history)
    elif rerun_mode == "asset_only":
        rerun_selection = (failed_node,)
    else:
        rerun_selection = _phase_only_selection(phase, run_history)

    return PartialRerunPlan(
        run_id=run_id,
        failed_node=failed_node,
        rerun_selection=rerun_selection,
        requires_manual_ack=rerun_mode != "asset_only",
        generated_at=datetime.now(timezone.utc),
        rerun_mode=rerun_mode,
    )


def plan_partial_rerun(run_id: str, failed_node: str) -> PartialRerunPlan:
    """Generate the P1a-minimal facade plan for a failed node."""

    _validate_inputs(run_id, failed_node)

    return PartialRerunPlan(
        run_id=run_id,
        failed_node=failed_node,
        rerun_selection=(failed_node,),
        requires_manual_ack=False,
        generated_at=datetime.now(timezone.utc),
        rerun_mode="asset_only",
    )


def _validate_inputs(run_id: str, failed_node: str) -> None:
    if not run_id:
        raise ValueError("run_id is required")
    if not failed_node:
        raise ValueError("failed_node is required")


def _policy_entry_for_failed_node(
    phase: PhaseEnum,
    failed_node: str,
    run_history: RunHistorySnapshot,
    policy: GatePolicyProfile,
) -> PhaseMatrixEntry:
    failure_class = _failure_class_for_failed_node(failed_node, run_history)
    phase_entries = [entry for entry in policy.phase_matrix if entry.phase is phase]

    if failure_class is not None:
        for entry in phase_entries:
            if entry.failure_class is failure_class:
                return entry
        msg = (
            "policy has no partial rerun row for failed node: "
            f"run_id={run_history.run_id} failed_node={failed_node} "
            f"phase={phase.value} failure_class={failure_class.value}"
        )
        raise PartialRerunNotAllowed(msg)

    if len(phase_entries) == 1:
        return phase_entries[0]

    partial_entries = [entry for entry in phase_entries if entry.allow_partial_rerun]
    if len(partial_entries) == 1:
        return partial_entries[0]

    msg = (
        "failed node requires an explicit failure_class for policy lookup: "
        f"run_id={run_history.run_id} failed_node={failed_node} phase={phase.value}"
    )
    raise PartialRerunNotAllowed(msg)


def _failure_class_for_failed_node(
    failed_node: str,
    run_history: RunHistorySnapshot,
) -> FailureClass | None:
    if run_history.node_failure_classes is None:
        return None
    value = run_history.node_failure_classes.get(failed_node)
    if value is None:
        return None
    if isinstance(value, FailureClass):
        return value
    try:
        return FailureClass(value)
    except (TypeError, ValueError) as exc:
        raise PartialRerunNotAllowed(
            f"unknown failure_class for failed_node={failed_node}: {value}"
        ) from exc


def _rerun_mode_for_entry(entry: PhaseMatrixEntry) -> RerunMode:
    if entry.rerun_mode is not None:
        return entry.rerun_mode
    if entry.action is GateAction.REPAIR_MANIFEST:
        return "repair_only"
    return "asset_only"


def _repair_only_selection(
    run_id: str,
    failed_node: str,
    run_history: RunHistorySnapshot,
) -> tuple[str, ...]:
    repair_node = run_history.repairable_nodes.get(failed_node)
    if repair_node is None:
        msg = (
            "repair-only rerun requires a repairable node mapping: "
            f"run_id={run_id} failed_node={failed_node}"
        )
        raise PartialRerunNotAllowed(msg)
    return (repair_node,)


def _phase_only_selection(
    phase: PhaseEnum,
    run_history: RunHistorySnapshot,
) -> tuple[str, ...]:
    seeds = tuple(
        node
        for node in run_history.failed_nodes
        if run_history.node_to_phase.get(node) is phase
    )
    if not seeds:
        raise PartialRerunNotAllowed(
            f"phase-only rerun has no failed nodes in phase={phase.value}"
        )

    downstream = _downstream_index(run_history.node_dependencies)
    selected: set[str] = set()
    stack = list(reversed(seeds))
    while stack:
        node = stack.pop()
        if node in selected:
            continue
        if run_history.node_to_phase.get(node) is not phase:
            continue
        selected.add(node)
        next_nodes = tuple(
            downstream_node
            for downstream_node in downstream.get(node, ())
            if run_history.node_to_phase.get(downstream_node) is phase
        )
        stack.extend(reversed(next_nodes))

    return _stable_topological_selection(selected, run_history)


def _downstream_index(
    node_dependencies: Mapping[str, tuple[str, ...]],
) -> dict[str, tuple[str, ...]]:
    downstream: dict[str, list[str]] = {}
    for node, dependencies in node_dependencies.items():
        for dependency in dependencies:
            downstream.setdefault(dependency, []).append(node)
    return {node: tuple(nodes) for node, nodes in downstream.items()}


def _stable_topological_selection(
    selected: set[str],
    run_history: RunHistorySnapshot,
) -> tuple[str, ...]:
    order = _stable_node_order(run_history)
    order_index = {node: index for index, node in enumerate(order)}
    visited: set[str] = set()
    output: list[str] = []

    def visit(node: str) -> None:
        if node in visited:
            return
        visited.add(node)
        dependencies = sorted(
            (
                dependency
                for dependency in run_history.node_dependencies.get(node, ())
                if dependency in selected
            ),
            key=lambda dependency: order_index.get(dependency, len(order_index)),
        )
        for dependency in dependencies:
            visit(dependency)
        output.append(node)

    for node in sorted(
        selected,
        key=lambda item: order_index.get(item, len(order_index)),
    ):
        visit(node)

    return tuple(output)


def _stable_node_order(run_history: RunHistorySnapshot) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()

    def append(node: str) -> None:
        if node not in seen:
            seen.add(node)
            ordered.append(node)

    for node, dependencies in run_history.node_dependencies.items():
        for dependency in dependencies:
            append(dependency)
        append(node)
    for node in run_history.node_to_phase:
        append(node)
    for node in run_history.failed_nodes:
        append(node)
    for repair_node in run_history.repairable_nodes.values():
        append(repair_node)

    return tuple(ordered)


__all__ = [
    "PartialRerunNotAllowed",
    "PartialRerunPlan",
    "RunHistorySnapshot",
    "UnknownFailedNode",
    "compute_partial_rerun_plan",
    "plan_partial_rerun",
]
