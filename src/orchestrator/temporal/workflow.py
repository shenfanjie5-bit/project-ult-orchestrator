"""SDK-independent async helper for the optional Phase 1-3 workflow."""

from __future__ import annotations

from typing import Protocol

from orchestrator.policy import GateAction
from orchestrator.temporal.models import (
    TemporalCycleRequest,
    TemporalCycleResult,
    TemporalPhaseResult,
    TemporalPhaseSpec,
    TemporalWorkflowStatus,
)

_BACKEND_RUNTIME_MANIFEST_FIELDS = frozenset(
    {
        "temporal_run_id",
        "temporal_workflow_id",
    }
)
_CYCLE_MANIFEST_FIELDS = frozenset(
    {
        "contract_version",
        "cycle_id",
        "phase_statuses",
        "policy_version",
        "publish_status",
    }
)


class TemporalPhaseExecutor(Protocol):
    """Protocol implemented by future Temporal handoff and parity executors."""

    async def execute_phase(
        self,
        request: TemporalCycleRequest,
        phase_spec: TemporalPhaseSpec,
    ) -> TemporalPhaseResult:
        """Execute one phase and return its normalized result."""


def phase_result_should_continue(result: TemporalPhaseResult) -> bool:
    """Return whether the workflow may advance past a phase result."""

    decision = result.gate_decision
    if decision is not None:
        return decision.action in (
            GateAction.CONTINUE,
            GateAction.MARK_INCONCLUSIVE,
        )

    return result.status == "succeeded"


async def run_phase1_3_temporal_workflow(
    request: TemporalCycleRequest,
    executor: TemporalPhaseExecutor,
) -> TemporalCycleResult:
    """Run Phase 1 -> Phase 2 -> Phase 3 through a pure async executor."""

    phase_results: list[TemporalPhaseResult] = []
    cycle_status: TemporalWorkflowStatus = "succeeded"

    for index, phase_spec in enumerate(request.phase_specs):
        result = await executor.execute_phase(request, phase_spec)
        if result.phase is not phase_spec.phase:
            msg = (
                "Temporal phase executor returned "
                f"{result.phase.value!r} for requested phase "
                f"{phase_spec.phase.value!r}"
            )
            raise ValueError(msg)

        phase_results.append(result)
        if phase_result_should_continue(result):
            continue

        cycle_status = _cycle_status_from_terminal_phase(result)
        phase_results.extend(
            _skipped_phase_results(request.phase_specs[index + 1 :]),
        )
        break

    phase_results_tuple = tuple(phase_results)
    return TemporalCycleResult(
        cycle_id=request.cycle_id,
        status=cycle_status,
        phase_results=phase_results_tuple,
        manifest_fields=_cycle_manifest_fields(
            request,
            phase_results_tuple,
        ),
    )


def _cycle_status_from_terminal_phase(
    result: TemporalPhaseResult,
) -> TemporalWorkflowStatus:
    decision = result.gate_decision
    if (
        result.status == "repair_required"
        or (decision is not None and decision.action is GateAction.REPAIR_MANIFEST)
    ):
        return "repair_required"
    return "failed"


def _skipped_phase_results(
    phase_specs: tuple[TemporalPhaseSpec, ...],
) -> tuple[TemporalPhaseResult, ...]:
    return tuple(
        TemporalPhaseResult(phase=phase_spec.phase, status="skipped")
        for phase_spec in phase_specs
    )


def _cycle_manifest_fields(
    request: TemporalCycleRequest,
    phase_results: tuple[TemporalPhaseResult, ...],
) -> dict[str, object]:
    fields: dict[str, object] = {
        "cycle_id": request.cycle_id,
        "policy_version": request.policy_version,
        "contract_version": request.contract_version,
        "publish_status": _publish_status(phase_results),
        "phase_statuses": tuple(
            {"phase": result.phase.value, "status": _manifest_phase_status(result)}
            for result in phase_results
        ),
    }
    for result in phase_results:
        for key, value in result.manifest_fields.items():
            if (
                key not in _BACKEND_RUNTIME_MANIFEST_FIELDS
                and key not in _CYCLE_MANIFEST_FIELDS
            ):
                fields[key] = value
    return fields


def _publish_status(
    phase_results: tuple[TemporalPhaseResult, ...],
) -> str:
    phase3_result = next(
        (result for result in phase_results if result.phase.value == "phase3"),
        None,
    )
    if phase3_result is None:
        return "not_published"
    decision = phase3_result.gate_decision
    if (
        phase3_result.status == "repair_required"
        or (decision is not None and decision.action is GateAction.REPAIR_MANIFEST)
    ):
        return "repair_required"
    if phase3_result.status == "succeeded":
        return "published"
    return "not_published"


def _manifest_phase_status(result: TemporalPhaseResult) -> str:
    decision = result.gate_decision
    if decision is not None and decision.action is GateAction.MARK_INCONCLUSIVE:
        return "inconclusive"
    if (
        result.status == "repair_required"
        or (decision is not None and decision.action is GateAction.REPAIR_MANIFEST)
    ):
        return "repair_required"
    return result.status


__all__ = [
    "TemporalPhaseExecutor",
    "phase_result_should_continue",
    "run_phase1_3_temporal_workflow",
]
