from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

from orchestrator.checks.models import GateDecision
from orchestrator.jobs.phase1 import (
    PHASE1_GRAPH_PROMOTION_ASSET_KEY,
    PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
)
from orchestrator.jobs.phase2 import PHASE2_STAGE_KEYS
from orchestrator.jobs.phase3 import (
    PHASE3_FORMAL_COMMIT_ASSET_KEY,
    PHASE3_MANIFEST_ASSET_KEY,
)
from orchestrator.policy import FailureClass, GateAction, PhaseEnum
from orchestrator.temporal import (
    TemporalCycleRequest,
    TemporalPhaseResult,
    TemporalPhaseSpec,
    default_phase1_3_specs,
    phase_result_should_continue,
    run_phase1_3_temporal_workflow,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


class RecordingExecutor:
    def __init__(
        self,
        results: Mapping[PhaseEnum, TemporalPhaseResult],
    ) -> None:
        self._results = dict(results)
        self.calls: list[PhaseEnum] = []

    async def execute_phase(
        self,
        request: TemporalCycleRequest,
        phase_spec: TemporalPhaseSpec,
    ) -> TemporalPhaseResult:
        del request
        self.calls.append(phase_spec.phase)
        return self._results[phase_spec.phase]


def test_temporal_facade_import_does_not_require_temporal_sdk() -> None:
    env = {
        **os.environ,
        "PYTHONPATH": str(REPO_ROOT / "src"),
    }
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import orchestrator.temporal; "
                "assert 'temporalio' not in sys.modules; "
                "print('ok')"
            ),
        ],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout.strip() == "ok"


def test_default_phase1_3_specs_use_canonical_asset_constants() -> None:
    specs = default_phase1_3_specs()

    assert tuple(spec.phase for spec in specs) == (
        PhaseEnum.PHASE1,
        PhaseEnum.PHASE2,
        PhaseEnum.PHASE3,
    )
    assert specs[0].asset_selection == (
        PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
    )
    assert specs[1].asset_selection == PHASE2_STAGE_KEYS
    assert specs[2].asset_selection == (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
    )


def test_happy_path_runs_phase1_then_phase2_then_phase3() -> None:
    request = _request()
    executor = RecordingExecutor(
        {
            PhaseEnum.PHASE1: TemporalPhaseResult(
                phase=PhaseEnum.PHASE1,
                status="succeeded",
            ),
            PhaseEnum.PHASE2: TemporalPhaseResult(
                phase=PhaseEnum.PHASE2,
                status="succeeded",
            ),
            PhaseEnum.PHASE3: TemporalPhaseResult(
                phase=PhaseEnum.PHASE3,
                status="succeeded",
            ),
        }
    )

    result = asyncio.run(run_phase1_3_temporal_workflow(request, executor))

    assert executor.calls == [PhaseEnum.PHASE1, PhaseEnum.PHASE2, PhaseEnum.PHASE3]
    assert result.status == "succeeded"
    assert result.manifest_fields["cycle_id"] == request.cycle_id
    assert result.manifest_fields["policy_version"] == request.policy_version
    assert result.manifest_fields["contract_version"] == request.contract_version
    assert result.manifest_fields["phase_statuses"] == (
        {"phase": "phase1", "status": "succeeded"},
        {"phase": "phase2", "status": "succeeded"},
        {"phase": "phase3", "status": "succeeded"},
    )


def test_phase1_fail_run_stops_and_marks_phase2_and_phase3_skipped() -> None:
    request = _request()
    decision = GateDecision(
        phase=PhaseEnum.PHASE1,
        failure_class=FailureClass.PUBLISH,
        action=GateAction.FAIL_RUN,
        reason="retain previous graph",
        scenario_id="phase1_graph_promotion_snapshot_failed",
    )
    executor = RecordingExecutor(
        {
            PhaseEnum.PHASE1: TemporalPhaseResult(
                phase=PhaseEnum.PHASE1,
                status="failed",
                gate_decision=decision,
            ),
        }
    )

    result = asyncio.run(run_phase1_3_temporal_workflow(request, executor))

    assert executor.calls == [PhaseEnum.PHASE1]
    assert result.status == "failed"
    assert tuple(phase_result.status for phase_result in result.phase_results) == (
        "failed",
        "skipped",
        "skipped",
    )
    assert result.phase_results[0].gate_decision == decision


def test_phase2_mark_inconclusive_continues_to_phase3() -> None:
    request = _request()
    decision = GateDecision(
        phase=PhaseEnum.PHASE2,
        failure_class=FailureClass.TASK_LEVEL,
        action=GateAction.MARK_INCONCLUSIVE,
        reason="single stock failed within tolerance",
        scenario_id="phase2_single_stock_task_failed",
    )
    executor = RecordingExecutor(
        {
            PhaseEnum.PHASE1: TemporalPhaseResult(
                phase=PhaseEnum.PHASE1,
                status="succeeded",
            ),
            PhaseEnum.PHASE2: TemporalPhaseResult(
                phase=PhaseEnum.PHASE2,
                status="failed",
                gate_decision=decision,
            ),
            PhaseEnum.PHASE3: TemporalPhaseResult(
                phase=PhaseEnum.PHASE3,
                status="succeeded",
            ),
        }
    )

    result = asyncio.run(run_phase1_3_temporal_workflow(request, executor))

    assert executor.calls == [PhaseEnum.PHASE1, PhaseEnum.PHASE2, PhaseEnum.PHASE3]
    assert result.status == "succeeded"
    assert result.phase_results[1].gate_decision == decision
    assert phase_result_should_continue(result.phase_results[1]) is True


def test_phase3_repair_manifest_returns_repair_required_with_scenario_id() -> None:
    request = _request()
    decision = GateDecision(
        phase=PhaseEnum.PHASE3,
        failure_class=FailureClass.INFRA,
        action=GateAction.REPAIR_MANIFEST,
        reason="manifest write failed",
        scenario_id="phase3_manifest_write_failed",
    )
    executor = RecordingExecutor(
        {
            PhaseEnum.PHASE1: TemporalPhaseResult(
                phase=PhaseEnum.PHASE1,
                status="succeeded",
            ),
            PhaseEnum.PHASE2: TemporalPhaseResult(
                phase=PhaseEnum.PHASE2,
                status="succeeded",
            ),
            PhaseEnum.PHASE3: TemporalPhaseResult(
                phase=PhaseEnum.PHASE3,
                status="repair_required",
                gate_decision=decision,
            ),
        }
    )

    result = asyncio.run(run_phase1_3_temporal_workflow(request, executor))

    assert executor.calls == [PhaseEnum.PHASE1, PhaseEnum.PHASE2, PhaseEnum.PHASE3]
    assert result.status == "repair_required"
    assert result.phase_results[2].gate_decision == decision
    assert result.phase_results[2].gate_decision is not None
    assert result.phase_results[2].gate_decision.scenario_id == (
        "phase3_manifest_write_failed"
    )


def test_non_continue_gate_decision_retains_full_runtime_identity() -> None:
    decision = GateDecision(
        phase=PhaseEnum.PHASE3,
        failure_class=FailureClass.PUBLISH,
        action=GateAction.FAIL_RUN,
        reason="formal commit failed",
        scenario_id="phase3_formal_commit_failed",
    )
    phase_result = TemporalPhaseResult(
        phase=PhaseEnum.PHASE3,
        status="failed",
        gate_decision=decision,
    )

    assert phase_result.gate_decision == decision
    assert phase_result.gate_decision.scenario_id == "phase3_formal_commit_failed"
    assert phase_result.gate_decision.failure_class is FailureClass.PUBLISH
    assert phase_result.gate_decision.action is GateAction.FAIL_RUN
    assert phase_result.gate_decision.reason == "formal commit failed"


def test_cycle_manifest_excludes_backend_runtime_ids() -> None:
    request = _request(
        tags={
            "temporal_workflow_id": "workflow-cycle-20260416",
            "temporal_run_id": "temporal-run-1",
        }
    )
    executor = RecordingExecutor(
        {
            PhaseEnum.PHASE1: TemporalPhaseResult(
                phase=PhaseEnum.PHASE1,
                status="succeeded",
                manifest_fields={"temporal_workflow_id": "phase1-workflow"},
            ),
            PhaseEnum.PHASE2: TemporalPhaseResult(
                phase=PhaseEnum.PHASE2,
                status="succeeded",
                manifest_fields={"temporal_run_id": "phase2-run"},
            ),
            PhaseEnum.PHASE3: TemporalPhaseResult(
                phase=PhaseEnum.PHASE3,
                status="succeeded",
            ),
        }
    )

    result = asyncio.run(run_phase1_3_temporal_workflow(request, executor))

    assert "temporal_workflow_id" not in result.manifest_fields
    assert "temporal_run_id" not in result.manifest_fields
    assert result.manifest_fields["cycle_id"] == "cycle-20260416"


def _request(
    tags: Mapping[str, str] | None = None,
) -> TemporalCycleRequest:
    return TemporalCycleRequest(
        cycle_id="cycle-20260416",
        dagster_run_id="dagster-run-1",
        phase0_run_id="phase0-run-1",
        policy_version="lite-0.1",
        contract_version="stub-0.1",
        phase_specs=default_phase1_3_specs(),
        tags=tags or {},
    )
