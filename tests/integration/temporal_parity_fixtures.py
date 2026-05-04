from __future__ import annotations

import dataclasses
import os
import shutil
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import yaml

from orchestrator.alerting import runbook_url_for
from orchestrator.checks.models import DataReadinessSignal, GateDecision
# Import GatePolicyResource at module level so Dagster 1.9 can resolve the
# `gate_policy: GatePolicyResource` annotation on @asset_check-decorated
# functions defined inside class methods. Without this, the function-local
# import (in _build_checks) is not in the function's __globals__, and
# Dagster's type-hint resolution fails with `Failed to resolve type
# annotation "GatePolicyResource"`.
from orchestrator.checks.resources import GatePolicyResource
from orchestrator.jobs.audit import AUDIT_EVAL_GROUP_NAME, RETROSPECTIVE_HOOK_ASSET_KEY
from orchestrator.jobs.phase0_constants import (
    PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
    PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
    PHASE0_GRAPH_STATUS_ASSET_KEY,
    PHASE0_GROUP_NAME,
    PHASE0_READINESS_ASSET_KEY,
)
from orchestrator.jobs.phase1 import (
    PHASE1_GRAPH_PROMOTION_ASSET_KEY,
    PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
    PHASE1_GROUP_NAME,
)
from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME, PHASE2_STAGE_KEYS
from orchestrator.jobs.phase3 import (
    PHASE3_FORMAL_COMMIT_ASSET_KEY,
    PHASE3_GROUP_NAME,
    PHASE3_MANIFEST_ASSET_KEY,
)
from orchestrator.policy import (
    FailureClass,
    GateAction,
    GatePolicyProfile,
    PhaseEnum,
    load_gate_policy,
)
from orchestrator.rerun import RunHistorySnapshot, compute_partial_rerun_plan
from orchestrator.rerun_request import write_rerun_request
from orchestrator.temporal import (
    TemporalCycleRequest,
    TemporalHandoffResult,
    TemporalPhaseResult,
    TemporalPhaseSpec,
    run_phase1_3_temporal_workflow,
)
from orchestrator.temporal.parity import CycleParitySnapshot

if TYPE_CHECKING:
    from orchestrator.resources import AssetFactoryProvider

REPAIR_MANIFEST_ASSET_KEY = "repair_cycle_publish_manifest"
PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY = "phase2_stock_AAPL"
PHASE2_SINGLE_STOCK_MSFT_ASSET_KEY = "phase2_stock_MSFT"
_PHASES = (
    PhaseEnum.PHASE0,
    PhaseEnum.PHASE1,
    PhaseEnum.PHASE2,
    PhaseEnum.PHASE3,
)
_RERUN_REQUEST_DIR_ENV = "ORCHESTRATOR_RERUN_REQUEST_DIR"
_MANIFEST_REPAIR_ASSET_KEY_ENV = "ORCHESTRATOR_MANIFEST_REPAIR_ASSET_KEY"
_PHASE_TERMINAL_ASSETS = {
    PhaseEnum.PHASE0: PHASE0_GRAPH_STATUS_ASSET_KEY,
    PhaseEnum.PHASE1: PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
    PhaseEnum.PHASE2: PHASE2_STAGE_KEYS[-1],
    PhaseEnum.PHASE3: PHASE3_MANIFEST_ASSET_KEY,
}
_PHASE_ASSET_NAMES = {
    PhaseEnum.PHASE0: frozenset(
        {
            PHASE0_READINESS_ASSET_KEY,
            PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
            PHASE0_GRAPH_STATUS_ASSET_KEY,
            "heartbeat",
        }
    ),
    PhaseEnum.PHASE1: frozenset(
        {
            PHASE1_GRAPH_PROMOTION_ASSET_KEY,
            PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        }
    ),
    PhaseEnum.PHASE2: frozenset(PHASE2_STAGE_KEYS),
    PhaseEnum.PHASE3: frozenset(
        {
            PHASE3_FORMAL_COMMIT_ASSET_KEY,
            PHASE3_MANIFEST_ASSET_KEY,
        }
    ),
}
_PARITY_MANIFEST_FIELD_KEYS = frozenset(
    {
        "cycle_id",
        "policy_version",
        "contract_version",
        "publish_status",
        "phase_statuses",
        "inconclusive",
    }
)
_BACKEND_RUNTIME_MANIFEST_FIELD_KEYS = frozenset(
    {
        "temporal_run_id",
        "temporal_workflow_id",
    }
)


@dataclass(frozen=True, slots=True)
class _ObservedCheckEvaluation:
    name: str
    passed: bool | None
    metadata: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class GateMatrixParityCase:
    scenario_id: str
    phase: PhaseEnum
    failure_class: FailureClass
    action: GateAction
    failed_node: str

    @property
    def case_id(self) -> str:
        return f"{self.scenario_id}:{self.phase.value}"


class TemporalParityFakeProvider:
    """Single fake provider used by both backend parity branches."""

    def __init__(
        self,
        dagster: Any,
        *,
        heartbeat_key: object,
        request_root: Path,
        case: GateMatrixParityCase | None = None,
    ) -> None:
        self._dagster = dagster
        self._heartbeat_key = heartbeat_key
        self.request_root = request_root
        self.case = case
        self.active_request_dir = request_root
        self.cycle_id = "cycle-unset"
        self.executed_phases: list[str] = []
        self.temporal_requests: list[TemporalCycleRequest] = []
        self.active_policy: GatePolicyProfile | None = None
        self.phase_status_overrides: dict[str, str] = {}
        self._assets = self._build_assets()
        self._checks = self._build_checks()

    def reset_runtime(
        self,
        *,
        backend: Literal["dagster_only", "dagster_plus_temporal"],
        cycle_id: str,
        policy: GatePolicyProfile,
    ) -> None:
        case_part = self.case.case_id.replace(":", "-") if self.case else "happy"
        self.active_request_dir = self.request_root / backend / case_part
        if self.active_request_dir.exists():
            shutil.rmtree(self.active_request_dir)
        self.active_request_dir.mkdir(parents=True)
        self.cycle_id = cycle_id
        self.active_policy = policy
        self.executed_phases.clear()
        self.temporal_requests.clear()
        self.phase_status_overrides.clear()

    def get_assets(self) -> tuple[object, ...]:
        return self._assets

    def get_checks(self) -> tuple[object, ...]:
        return self._checks

    def get_resources(self) -> dict[str, object]:
        dagster = self._dagster
        owner = self

        class FakeDataReadinessResource(dagster.ConfigurableResource):
            def create_resource(self, context: object) -> object:
                del context
                return _ReadinessProvider(owner)

        class FakeLLMHealthProbeResource(dagster.ConfigurableResource):
            def create_resource(self, context: object) -> object:
                del context
                return _LLMHealthProbe(owner)

        class FakePhase2PoolFailureRateResource(dagster.ConfigurableResource):
            def get_phase2_pool_failure_rate_event(self) -> object:
                from orchestrator.checks import Phase2PoolFailureRateEvent

                if owner._is_case("phase2_pool_failure_rate_exceeded"):
                    return Phase2PoolFailureRateEvent(
                        failed_count=4,
                        total_count=10,
                        failed_nodes=(
                            PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY,
                            PHASE2_SINGLE_STOCK_MSFT_ASSET_KEY,
                        ),
                        reason="fake pool failures",
                    )
                return Phase2PoolFailureRateEvent(
                    failed_count=0,
                    total_count=10,
                    failed_nodes=(),
                    reason="healthy fake pool",
                )

        class FakeTemporalHandoffClientResource(dagster.ConfigurableResource):
            def create_resource(self, context: object) -> object:
                del context
                return _RecordingTemporalHandoffClient(owner)

        return {
            "data_readiness": FakeDataReadinessResource(),
            "llm_health_probe": FakeLLMHealthProbeResource(),
            "phase2_pool_failure_rate": FakePhase2PoolFailureRateResource(),
            "temporal_handoff_client": FakeTemporalHandoffClientResource(),
        }

    def temporal_phase_result(
        self,
        request: TemporalCycleRequest,
        phase_spec: TemporalPhaseSpec,
        policy: GatePolicyProfile,
    ) -> TemporalPhaseResult:
        phase = phase_spec.phase
        self._mark_phase(phase)
        if self.case is None or self.case.phase is not phase:
            return TemporalPhaseResult(phase=phase, status="succeeded")

        decision = _classify_case(self.case, policy)
        if decision.action is GateAction.MARK_INCONCLUSIVE:
            self.phase_status_overrides[phase.value] = "inconclusive"
            _dispatch_case_alert(
                decision,
                cycle_id=request.cycle_id,
                failed_node=self.case.failed_node,
                summary="fake single-stock task failed",
                channels=policy.alert_channels,
            )
            return TemporalPhaseResult(
                phase=phase,
                status="failed",
                gate_decision=decision,
                manifest_fields={"inconclusive": True},
            )
        if decision.action is GateAction.REPAIR_MANIFEST:
            request_path = self._write_manifest_repair_request(
                request.dagster_run_id,
                policy,
            )
            _dispatch_case_alert(
                decision,
                cycle_id=request.cycle_id,
                failed_node=self.case.failed_node,
                summary=f"fake manifest write failed (rerun request: {request_path})",
                channels=policy.alert_channels,
            )
            return TemporalPhaseResult(
                phase=phase,
                status="repair_required",
                gate_decision=decision,
            )

        _dispatch_case_alert(
            decision,
            cycle_id=request.cycle_id,
            failed_node=self.case.failed_node,
            summary=f"fake {self.case.scenario_id} failure",
            channels=policy.alert_channels,
        )
        return TemporalPhaseResult(
            phase=phase,
            status="failed",
            gate_decision=decision,
        )

    def _build_assets(self) -> tuple[object, ...]:
        dagster = self._dagster
        owner = self

        @dagster.asset(
            name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
            group_name=PHASE0_GROUP_NAME,
            deps=[
                dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
                self._heartbeat_key,
            ],
        )
        def candidate_freeze() -> str:
            owner._mark_phase(PhaseEnum.PHASE0)
            return "frozen"

        @dagster.asset(
            name=PHASE0_GRAPH_STATUS_ASSET_KEY,
            group_name=PHASE0_GROUP_NAME,
        )
        def graph_status(candidate_freeze: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE0)
            return f"{candidate_freeze}:ready"

        @dagster.asset(
            name=PHASE1_GRAPH_PROMOTION_ASSET_KEY,
            group_name=PHASE1_GROUP_NAME,
            deps=[
                dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
                dagster.AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
                dagster.AssetKey([PHASE0_GRAPH_STATUS_ASSET_KEY]),
            ],
        )
        def graph_promotion() -> str:
            owner._mark_phase(PhaseEnum.PHASE1)
            return "promoted"

        @dagster.asset(
            name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
            group_name=PHASE1_GROUP_NAME,
        )
        def graph_snapshot(graph_promotion: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE1)
            if owner._is_case("phase1_graph_promotion_snapshot_failed"):
                raise RuntimeError(f"snapshot writer failed after {graph_promotion}")
            return f"{graph_promotion}:snapshot"

        @dagster.asset(name=PHASE2_STAGE_KEYS[0], group_name=PHASE2_GROUP_NAME)
        def l1(graph_snapshot: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE2)
            return f"{graph_snapshot}:l1"

        @dagster.asset(name=PHASE2_STAGE_KEYS[1], group_name=PHASE2_GROUP_NAME)
        def l2(l1: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE2)
            return f"{l1}:l2"

        @dagster.asset(name=PHASE2_STAGE_KEYS[2], group_name=PHASE2_GROUP_NAME)
        def l3(l2: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE2)
            return f"{l2}:l3"

        @dagster.asset(name=PHASE2_STAGE_KEYS[3], group_name=PHASE2_GROUP_NAME)
        def l4(l3: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE2)
            return f"{l3}:l4"

        @dagster.asset(name=PHASE2_STAGE_KEYS[4], group_name=PHASE2_GROUP_NAME)
        def l5(l4: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE2)
            return f"{l4}:l5"

        @dagster.asset(name=PHASE2_STAGE_KEYS[5], group_name=PHASE2_GROUP_NAME)
        def l6(l5: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE2)
            return f"{l5}:l6"

        @dagster.asset(name=PHASE2_STAGE_KEYS[6], group_name=PHASE2_GROUP_NAME)
        def l7(l6: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE2)
            return f"{l6}:l7"

        @dagster.asset(name=PHASE2_STAGE_KEYS[7], group_name=PHASE2_GROUP_NAME)
        def l8(l7: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE2)
            return f"{l7}:l8"

        @dagster.asset(
            name=PHASE3_FORMAL_COMMIT_ASSET_KEY,
            group_name=PHASE3_GROUP_NAME,
        )
        def formal_objects_commit(l8: str) -> str:
            owner._mark_phase(PhaseEnum.PHASE3)
            return f"{l8}:formal"

        @dagster.asset(
            name=PHASE3_MANIFEST_ASSET_KEY,
            group_name=PHASE3_GROUP_NAME,
        )
        def cycle_publish_manifest(
            context,
            formal_objects_commit: str,
        ) -> str:
            owner._mark_phase(PhaseEnum.PHASE3)
            if owner._is_case("phase3_manifest_write_failed"):
                raise RuntimeError(
                    f"fake manifest write failed after {formal_objects_commit}",
                )
            owner._add_manifest_metadata(context)
            return f"{formal_objects_commit}:manifest"

        @dagster.asset(
            name=RETROSPECTIVE_HOOK_ASSET_KEY,
            group_name=AUDIT_EVAL_GROUP_NAME,
        )
        def retrospective_hook(cycle_publish_manifest: str) -> str:
            return f"{cycle_publish_manifest}:audit"

        return (
            candidate_freeze,
            graph_status,
            graph_promotion,
            graph_snapshot,
            l1,
            l2,
            l3,
            l4,
            l5,
            l6,
            l7,
            l8,
            formal_objects_commit,
            cycle_publish_manifest,
            retrospective_hook,
        )

    def _build_checks(self) -> tuple[object, ...]:
        dagster = self._dagster
        assets = {next(iter(asset.keys)).path[-1]: asset for asset in self._assets}
        candidate_freeze = assets[PHASE0_CANDIDATE_FREEZE_ASSET_KEY]
        graph_status = assets[PHASE0_GRAPH_STATUS_ASSET_KEY]
        graph_snapshot = assets[PHASE1_GRAPH_SNAPSHOT_ASSET_KEY]
        l7 = assets[PHASE2_STAGE_KEYS[-2]]
        l8 = assets[PHASE2_STAGE_KEYS[-1]]
        formal_objects_commit = assets[PHASE3_FORMAL_COMMIT_ASSET_KEY]
        cycle_publish_manifest = assets[PHASE3_MANIFEST_ASSET_KEY]
        retrospective_hook = assets[RETROSPECTIVE_HOOK_ASSET_KEY]
        owner = self

        @dagster.asset_check(
            asset=graph_status,
            name=PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
            blocking=True,
        )
        def neo4j_graph_consistency_check() -> object:
            return dagster.AssetCheckResult(passed=True)

        @dagster.asset_check(
            asset=candidate_freeze,
            name="phase0_dbt_partial_rerun_gate",
            blocking=True,
        )
        def phase0_dbt_partial_rerun_gate(
            context,
            gate_policy: GatePolicyResource,
        ) -> object:
            if not owner._is_case("phase0_dbt_test_failed"):
                return dagster.AssetCheckResult(passed=True)
            decision = _classify_case(
                cast(GateMatrixParityCase, owner.case),
                gate_policy.policy,
            )
            plan = owner._write_asset_only_rerun_request(
                _run_id_from_context(context),
                failed_node=cast(GateMatrixParityCase, owner.case).failed_node,
                phase=PhaseEnum.PHASE0,
                failure_class=FailureClass.TASK_LEVEL,
                policy=gate_policy.policy,
            )
            _dispatch_case_alert(
                decision,
                cycle_id=_cycle_id_from_context(context, owner.cycle_id),
                failed_node=cast(GateMatrixParityCase, owner.case).failed_node,
                summary="fake dbt test failed",
                channels=gate_policy.policy.alert_channels,
            )
            return dagster.AssetCheckResult(
                passed=False,
                metadata=_decision_metadata(
                    decision,
                    failed_node=cast(GateMatrixParityCase, owner.case).failed_node,
                    rerun_request_status="present",
                    rerun_selection=plan.rerun_selection,
                ),
            )

        @dagster.asset_check(
            asset=candidate_freeze,
            name="phase0_infra_hard_stop_gate",
            blocking=True,
        )
        def phase0_infra_hard_stop_gate(
            context,
            gate_policy: GatePolicyResource,
        ) -> object:
            return owner._hard_stop_check_result(
                dagster,
                context,
                gate_policy.policy,
                PhaseEnum.PHASE0,
            )

        @dagster.asset_check(
            asset=graph_snapshot,
            name="phase1_infra_hard_stop_gate",
            blocking=True,
        )
        def phase1_infra_hard_stop_gate(
            context,
            gate_policy: GatePolicyResource,
        ) -> object:
            return owner._hard_stop_check_result(
                dagster,
                context,
                gate_policy.policy,
                PhaseEnum.PHASE1,
            )

        @dagster.asset_check(
            asset=l7,
            name="phase2_single_stock_gate",
            blocking=False,
        )
        def phase2_single_stock_gate(
            context,
            gate_policy: GatePolicyResource,
        ) -> object:
            if not owner._is_case("phase2_single_stock_task_failed"):
                return dagster.AssetCheckResult(passed=True)
            decision = _classify_case(cast(GateMatrixParityCase, owner.case), gate_policy.policy)
            owner.phase_status_overrides[PhaseEnum.PHASE2.value] = "inconclusive"
            _dispatch_case_alert(
                decision,
                cycle_id=_cycle_id_from_context(context, owner.cycle_id),
                failed_node=cast(GateMatrixParityCase, owner.case).failed_node,
                summary="fake single-stock task failed",
                channels=gate_policy.policy.alert_channels,
            )
            return dagster.AssetCheckResult(
                passed=False,
                metadata=_decision_metadata(
                    decision,
                    failed_node=cast(GateMatrixParityCase, owner.case).failed_node,
                    inconclusive="present",
                ),
            )

        @dagster.asset_check(
            asset=l7,
            name="phase2_infra_hard_stop_gate",
            blocking=True,
        )
        def phase2_infra_hard_stop_gate(
            context,
            gate_policy: GatePolicyResource,
        ) -> object:
            return owner._hard_stop_check_result(
                dagster,
                context,
                gate_policy.policy,
                PhaseEnum.PHASE2,
            )

        @dagster.asset_check(
            asset=formal_objects_commit,
            name="phase3_formal_commit_gate",
            blocking=True,
        )
        def phase3_formal_commit_gate(
            context,
            gate_policy: GatePolicyResource,
        ) -> object:
            if not owner._is_case("phase3_formal_commit_failed"):
                return dagster.AssetCheckResult(passed=True)
            decision = _classify_case(cast(GateMatrixParityCase, owner.case), gate_policy.policy)
            _dispatch_case_alert(
                decision,
                cycle_id=_cycle_id_from_context(context, owner.cycle_id),
                failed_node=cast(GateMatrixParityCase, owner.case).failed_node,
                summary="fake formal commit failed",
                channels=gate_policy.policy.alert_channels,
            )
            return dagster.AssetCheckResult(
                passed=False,
                metadata=_decision_metadata(
                    decision,
                    failed_node=cast(GateMatrixParityCase, owner.case).failed_node,
                ),
            )

        @dagster.asset_check(
            asset=formal_objects_commit,
            name="phase3_infra_hard_stop_gate",
            blocking=True,
        )
        def phase3_infra_hard_stop_gate(
            context,
            gate_policy: GatePolicyResource,
        ) -> object:
            return owner._hard_stop_check_result(
                dagster,
                context,
                gate_policy.policy,
                PhaseEnum.PHASE3,
            )

        @dagster.asset_check(
            asset=graph_snapshot,
            name="fake_phase1_graph_snapshot_pure_check",
        )
        def fake_phase1_graph_snapshot_pure_check() -> object:
            return dagster.AssetCheckResult(passed=True)

        @dagster.asset_check(asset=l8, name="fake_phase2_l8_pure_check")
        def fake_phase2_l8_pure_check() -> object:
            return dagster.AssetCheckResult(passed=True)

        @dagster.asset_check(
            asset=cycle_publish_manifest,
            name="fake_phase3_manifest_pure_check",
        )
        def fake_phase3_manifest_pure_check() -> object:
            return dagster.AssetCheckResult(passed=True)

        @dagster.asset_check(asset=retrospective_hook, name="fake_audit_eval_check")
        def fake_audit_eval_check() -> object:
            return dagster.AssetCheckResult(passed=True)

        return (
            neo4j_graph_consistency_check,
            phase0_dbt_partial_rerun_gate,
            phase0_infra_hard_stop_gate,
            phase1_infra_hard_stop_gate,
            phase2_single_stock_gate,
            phase2_infra_hard_stop_gate,
            phase3_formal_commit_gate,
            phase3_infra_hard_stop_gate,
            fake_phase1_graph_snapshot_pure_check,
            fake_phase2_l8_pure_check,
            fake_phase3_manifest_pure_check,
            fake_audit_eval_check,
        )

    def _hard_stop_check_result(
        self,
        dagster: Any,
        context: object,
        policy: GatePolicyProfile,
        phase: PhaseEnum,
    ) -> object:
        if not self._is_case("infra_unavailable_hard_stop", phase=phase):
            return dagster.AssetCheckResult(passed=True)
        case = cast(GateMatrixParityCase, self.case)
        decision = _classify_case(case, policy)
        _dispatch_case_alert(
            decision,
            cycle_id=_cycle_id_from_context(context, self.cycle_id),
            failed_node=case.failed_node,
            summary=f"fake {case.failed_node} unavailable",
            channels=policy.alert_channels,
        )
        return dagster.AssetCheckResult(
            passed=False,
            metadata=_decision_metadata(decision, failed_node=case.failed_node),
        )

    def _write_asset_only_rerun_request(
        self,
        run_id: str,
        *,
        failed_node: str,
        phase: PhaseEnum,
        failure_class: FailureClass,
        policy: GatePolicyProfile,
    ) -> object:
        plan = compute_partial_rerun_plan(
            run_id,
            failed_node,
            RunHistorySnapshot(
                run_id=run_id,
                node_to_phase={failed_node: phase},
                node_dependencies={failed_node: ()},
                failed_nodes=(failed_node,),
                repairable_nodes={},
                node_failure_classes={failed_node: failure_class},
            ),
            policy,
        )
        write_rerun_request(plan, self.active_request_dir)
        return plan

    def _write_manifest_repair_request(
        self,
        run_id: str,
        policy: GatePolicyProfile,
    ) -> Path:
        from orchestrator.checks import ManifestWriteFailureEvent
        from orchestrator.checks.phase3 import plan_manifest_repair_rerun

        event = ManifestWriteFailureEvent(
            repair_node=REPAIR_MANIFEST_ASSET_KEY,
            reason="fake manifest write failed",
        )
        plan = plan_manifest_repair_rerun(run_id, event, policy)
        return write_rerun_request(plan, self.active_request_dir)

    def _is_case(
        self,
        scenario_id: str,
        *,
        phase: PhaseEnum | None = None,
    ) -> bool:
        return self.case is not None and self.case.scenario_id == scenario_id and (
            phase is None or self.case.phase is phase
        )

    def _mark_phase(self, phase: PhaseEnum) -> None:
        if phase.value not in self.executed_phases:
            self.executed_phases.append(phase.value)

    def _add_manifest_metadata(self, context: object) -> None:
        add_output_metadata = getattr(context, "add_output_metadata", None)
        if not callable(add_output_metadata):
            return

        policy = self.active_policy
        if policy is None:
            raise RuntimeError("parity provider active_policy was not initialized")

        phase_statuses = {
            phase.value: self.phase_status_overrides.get(phase.value, "succeeded")
            for phase in _PHASES
        }
        metadata = _manifest_fields(
            self.cycle_id,
            policy,
            phase_statuses,
        )
        metadata["phase_statuses"] = list(metadata["phase_statuses"])
        add_output_metadata(metadata)


class InProcessTemporalParityExecutor:
    """In-process executor that runs the parity fake through the workflow seam."""

    def __init__(
        self,
        *,
        policy: GatePolicyProfile,
        module_factories: Sequence[AssetFactoryProvider],
    ) -> None:
        self._policy = policy
        self._provider = _parity_provider(module_factories)
        self.calls: list[PhaseEnum] = []

    async def execute_phase(
        self,
        request: TemporalCycleRequest,
        phase_spec: TemporalPhaseSpec,
    ) -> TemporalPhaseResult:
        self.calls.append(phase_spec.phase)
        return self._provider.temporal_phase_result(
            request,
            phase_spec,
            self._policy,
        )


def build_temporal_parity_fake_provider(
    dagster: Any,
    *,
    request_root: Path,
    case: GateMatrixParityCase | None = None,
) -> TemporalParityFakeProvider:
    from orchestrator.jobs.phase0 import dbt_phase0_assets

    return TemporalParityFakeProvider(
        dagster,
        heartbeat_key=_heartbeat_asset_key(dbt_phase0_assets),
        request_root=request_root,
        case=case,
    )


def parity_policy_path(
    tmp_path: Path,
    base_policy_path: str,
    *,
    execution_backend: Literal["dagster_only", "dagster_plus_temporal"],
) -> str:
    tmp_path.mkdir(parents=True, exist_ok=True)
    policy = load_gate_policy(base_policy_path).model_copy(
        update={"execution_backend": execution_backend},
    )
    path = tmp_path / f"gate_policy.{execution_backend}.yaml"
    path.write_text(
        yaml.safe_dump(policy.model_dump(mode="json"), sort_keys=False),
        encoding="utf-8",
    )
    return str(path)


def gate_matrix_parity_cases(
    policy: GatePolicyProfile,
) -> tuple[GateMatrixParityCase, ...]:
    cases: list[GateMatrixParityCase] = []
    for entry in policy.phase_matrix:
        phases = entry.applies_to_phases or (entry.phase,)
        for phase in phases:
            cases.append(
                GateMatrixParityCase(
                    scenario_id=entry.scenario_id,
                    phase=phase,
                    failure_class=entry.failure_class,
                    action=entry.action,
                    failed_node=_failed_node_for(entry.scenario_id, phase),
                ),
            )
    return tuple(cases)


def execute_dagster_only_snapshot(
    *,
    dagster: Any,
    instance: object,
    policy_path: str,
    module_factories: Sequence[AssetFactoryProvider],
    cycle_id: str,
) -> CycleParitySnapshot:
    from orchestrator.definitions import build_definitions

    policy = load_gate_policy(policy_path)
    provider = _parity_provider(module_factories)
    provider.reset_runtime(
        backend="dagster_only",
        cycle_id=cycle_id,
        policy=policy,
    )
    result: object | None = None

    with _provider_runtime_env(provider), _capture_gate_alerts() as alerts:
        if provider._is_case("phase0_data_readiness_delayed"):
            from orchestrator.sensors.data_readiness import (
                evaluate_data_readiness_sensor,
            )

            context = dagster.build_sensor_context(
                resources={
                    "data_readiness": _ReadinessProvider(provider),
                    "gate_policy": _GatePolicy(policy),
                },
            )
            evaluate_data_readiness_sensor(context)
        else:
            defs = build_definitions(
                module_factories=module_factories,
                policy_path=policy_path,
            )
            dagster.Definitions.validate_loadable(defs)
            result = defs.get_job_def("daily_cycle_job").execute_in_process(
                instance=instance,
                raise_on_error=False,
                tags={"cycle_id": cycle_id},
            )

    return _snapshot_from_runtime(
        cycle_id=cycle_id,
        policy=policy,
        provider=provider,
        result=result,
        alert_records=alerts,
    )


async def execute_temporal_snapshot(
    *,
    policy_path: str,
    module_factories: Sequence[AssetFactoryProvider],
    cycle_id: str,
) -> CycleParitySnapshot:
    from dagster import DagsterInstance
    from orchestrator.definitions import build_definitions

    policy = load_gate_policy(policy_path)
    provider = _parity_provider(module_factories)
    provider.reset_runtime(
        backend="dagster_plus_temporal",
        cycle_id=cycle_id,
        policy=policy,
    )
    result: object | None = None
    temporal_result: object | None = None

    with DagsterInstance.ephemeral() as instance:
        with _provider_runtime_env(provider), _capture_gate_alerts() as alerts:
            if provider._is_case("phase0_data_readiness_delayed"):
                from dagster import build_sensor_context
                from orchestrator.sensors.data_readiness import (
                    evaluate_data_readiness_sensor,
                )

                context = build_sensor_context(
                    resources={
                        "data_readiness": _ReadinessProvider(provider),
                        "gate_policy": _GatePolicy(policy),
                    },
                )
                evaluate_data_readiness_sensor(context)
            else:
                defs = build_definitions(
                    module_factories=module_factories,
                    policy_path=policy_path,
                )
                result = defs.get_job_def("daily_cycle_phase0_job").execute_in_process(
                    instance=instance,
                    raise_on_error=False,
                    tags={"cycle_id": cycle_id},
                )
                if bool(getattr(result, "success", False)):
                    from orchestrator.sensors.temporal_handoff import (
                        evaluate_temporal_handoff_sensor,
                    )

                    client = _RecordingTemporalHandoffClient(provider)
                    evaluate_temporal_handoff_sensor(
                        policy=policy,
                        phase0_run_id=str(getattr(result, "run_id")),
                        dagster_run_id=str(getattr(result, "run_id")),
                        cycle_id=cycle_id,
                        tags={
                            "cycle_id": cycle_id,
                            "temporal_workflow_id": f"workflow-{cycle_id}",
                            "temporal_run_id": f"temporal-run-{cycle_id}",
                        },
                        client=client,
                    )
                    request = client.requests[0]
                    executor = InProcessTemporalParityExecutor(
                        policy=policy,
                        module_factories=module_factories,
                    )
                    temporal_result = await run_phase1_3_temporal_workflow(
                        request,
                        executor,
                    )

    return _snapshot_from_runtime(
        cycle_id=cycle_id,
        policy=policy,
        provider=provider,
        result=result,
        alert_records=alerts,
        temporal_result=temporal_result,
    )


def _snapshot_from_runtime(
    *,
    cycle_id: str,
    policy: GatePolicyProfile,
    provider: TemporalParityFakeProvider,
    result: object | None,
    alert_records: Sequence[Mapping[str, object]],
    temporal_result: object | None = None,
) -> CycleParitySnapshot:
    _assert_expected_success(provider.case, result, temporal_result)
    alert_payloads = tuple(_stable_alert_payload(record) for record in alert_records)
    check_evaluations = _asset_check_evaluations(result)
    materializations = _asset_materialization_metadata_by_name(result)
    gate_decisions = _runtime_gate_decisions(
        policy=policy,
        alert_payloads=alert_payloads,
        check_evaluations=check_evaluations,
        temporal_result=temporal_result,
    )
    if temporal_result is None:
        phase_statuses = _dagster_phase_statuses(
            result=result,
            gate_decisions=gate_decisions,
            materialized_asset_names=frozenset(materializations),
        )
        manifest_fields = _dagster_manifest_fields(
            materializations=materializations,
            cycle_id=cycle_id,
            policy=policy,
            phase_statuses=phase_statuses,
        )
    else:
        phase_statuses = _temporal_phase_statuses(
            result=result,
            temporal_result=temporal_result,
            phase0_gate_decisions=tuple(
                decision
                for decision in gate_decisions
                if decision.phase is PhaseEnum.PHASE0
            ),
            phase0_materialized_asset_names=frozenset(materializations),
        )
        manifest_fields = _temporal_manifest_fields(
            temporal_result=temporal_result,
            phase_statuses=phase_statuses,
        )
    request_payloads = _rerun_request_payloads(provider.active_request_dir)
    rerun_status = _rerun_request_status(request_payloads)
    failed_node = _diagnostic_failed_node(
        alert_payloads=alert_payloads,
        check_evaluations=check_evaluations,
    )
    runbook_url = _diagnostic_runbook_url(
        alert_payloads=alert_payloads,
        gate_decisions=gate_decisions,
    )
    diagnostics = {
        "failed_node": failed_node,
        "runbook_url": runbook_url,
        "rerun_request_status": rerun_status,
        "rerun_selection": _rerun_selection(request_payloads),
        "inconclusive": (
            "present"
            if any(
                decision.action is GateAction.MARK_INCONCLUSIVE
                for decision in gate_decisions
            )
            else "not_applicable"
        ),
        "phase1_3_started": (
            "yes"
            if any(
                phase_statuses[phase.value] != "skipped"
                for phase in (PhaseEnum.PHASE1, PhaseEnum.PHASE2, PhaseEnum.PHASE3)
            )
            else "no"
        ),
    }
    snapshot = CycleParitySnapshot(
        cycle_id=cycle_id,
        phase_statuses=phase_statuses,
        gate_decisions=gate_decisions,
        manifest_fields=manifest_fields,
        alert_payloads=alert_payloads,
        diagnostic_fields=diagnostics,
        rerun_request_statuses={
            provider.case.case_id if provider.case else "happy_path": rerun_status,
        },
    )
    _assert_expected_snapshot(provider.case, snapshot, policy)
    return snapshot


def _runtime_gate_decisions(
    *,
    policy: GatePolicyProfile,
    alert_payloads: Sequence[Mapping[str, object]],
    check_evaluations: Sequence[_ObservedCheckEvaluation],
    temporal_result: object | None,
) -> tuple[GateDecision, ...]:
    decisions: list[GateDecision] = []
    if temporal_result is not None:
        for phase_result in getattr(temporal_result, "phase_results", ()) or ():
            decision = getattr(phase_result, "gate_decision", None)
            if isinstance(decision, GateDecision):
                decisions.append(decision)

    for evaluation in check_evaluations:
        decision = _decision_from_check_evaluation(evaluation, policy)
        if decision is not None:
            decisions.append(decision)

    decisions.extend(
        _decision_from_alert(payload, policy) for payload in alert_payloads
    )
    return _unique_gate_decisions(decisions)


def _decision_from_check_evaluation(
    evaluation: _ObservedCheckEvaluation,
    policy: GatePolicyProfile,
) -> GateDecision | None:
    action = evaluation.metadata.get("action")
    if (
        evaluation.passed is not False
        and action != GateAction.MARK_INCONCLUSIVE.value
    ):
        return None
    if action in (None, "", GateAction.CONTINUE.value):
        return None

    failure_class = evaluation.metadata.get("failure_class")
    if not isinstance(failure_class, str) or not failure_class:
        return None

    event: dict[str, object] = {"failure_class": failure_class}
    scenario_id = evaluation.metadata.get("scenario_id")
    if isinstance(scenario_id, str) and scenario_id:
        event["scenario_id"] = scenario_id

    from orchestrator.checks.classifier import classify_gate_result

    return classify_gate_result(
        _phase_from_check_evaluation(evaluation),
        event,
        policy,
    )


def _phase_from_check_evaluation(
    evaluation: _ObservedCheckEvaluation,
) -> PhaseEnum:
    phase_value = evaluation.metadata.get("phase")
    if isinstance(phase_value, str) and phase_value:
        return PhaseEnum(phase_value)

    if evaluation.name == "llm_health_check":
        return PhaseEnum.PHASE0
    for phase in _PHASES:
        if evaluation.name.startswith(phase.value):
            return phase
    if "phase2" in evaluation.name:
        return PhaseEnum.PHASE2

    raise AssertionError(
        f"failed parity check {evaluation.name!r} did not expose phase metadata",
    )


def _unique_gate_decisions(
    decisions: Iterable[GateDecision],
) -> tuple[GateDecision, ...]:
    unique: list[GateDecision] = []
    seen: set[tuple[object, ...]] = set()
    for decision in decisions:
        if decision.action is GateAction.CONTINUE:
            continue
        key = (
            decision.phase,
            decision.failure_class,
            decision.action,
            decision.scenario_id,
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(decision)
    return tuple(unique)


def _dagster_phase_statuses(
    *,
    result: object | None,
    gate_decisions: Sequence[GateDecision],
    materialized_asset_names: frozenset[str],
) -> dict[str, str]:
    failed_step_keys = _failed_step_keys(result)
    statuses: dict[str, str] = {}
    terminal_seen = False

    for phase in _PHASES:
        if terminal_seen:
            statuses[phase.value] = "skipped"
            continue

        decision = _decision_for_phase(gate_decisions, phase)
        if decision is not None:
            status = _phase_status_from_decision(decision)
            statuses[phase.value] = status
            terminal_seen = _is_terminal_phase_status(status)
            continue

        if _PHASE_TERMINAL_ASSETS[phase] in materialized_asset_names:
            statuses[phase.value] = "succeeded"
            continue

        if any(
            _step_key_matches_phase(step_key, phase)
            for step_key in failed_step_keys
        ):
            statuses[phase.value] = "failed"
            terminal_seen = True
            continue

        statuses[phase.value] = "skipped"

    return statuses


def _temporal_phase_statuses(
    *,
    result: object | None,
    temporal_result: object,
    phase0_gate_decisions: Sequence[GateDecision],
    phase0_materialized_asset_names: frozenset[str],
) -> dict[str, str]:
    phase0_status = _dagster_phase_statuses(
        result=result,
        gate_decisions=phase0_gate_decisions,
        materialized_asset_names=phase0_materialized_asset_names,
    )[PhaseEnum.PHASE0.value]
    phase_results_by_phase = {
        getattr(phase_result, "phase", None): phase_result
        for phase_result in getattr(temporal_result, "phase_results", ()) or ()
    }
    statuses = {PhaseEnum.PHASE0.value: phase0_status}

    for phase in (PhaseEnum.PHASE1, PhaseEnum.PHASE2, PhaseEnum.PHASE3):
        phase_result = phase_results_by_phase.get(phase)
        if phase_result is None:
            statuses[phase.value] = "missing"
        else:
            statuses[phase.value] = _phase_status_from_temporal_result(phase_result)

    return statuses


def _decision_for_phase(
    gate_decisions: Sequence[GateDecision],
    phase: PhaseEnum,
) -> GateDecision | None:
    return next(
        (
            decision
            for decision in gate_decisions
            if decision.phase is phase and decision.action is not GateAction.CONTINUE
        ),
        None,
    )


def _phase_status_from_decision(decision: GateDecision) -> str:
    if decision.action is GateAction.MARK_INCONCLUSIVE:
        return "inconclusive"
    if decision.action is GateAction.REPAIR_MANIFEST:
        return "repair_required"
    return "failed"


def _phase_status_from_temporal_result(phase_result: object) -> str:
    decision = getattr(phase_result, "gate_decision", None)
    if (
        isinstance(decision, GateDecision)
        and decision.action is not GateAction.CONTINUE
    ):
        return _phase_status_from_decision(decision)
    status = getattr(phase_result, "status", None)
    return status if isinstance(status, str) else "missing"


def _is_terminal_phase_status(status: str) -> bool:
    return status in {"failed", "repair_required"}


def _step_key_matches_phase(step_key: str, phase: PhaseEnum) -> bool:
    if step_key in _PHASE_ASSET_NAMES[phase]:
        return True
    return any(asset_name in step_key for asset_name in _PHASE_ASSET_NAMES[phase])


def _dagster_manifest_fields(
    *,
    materializations: Mapping[str, Mapping[str, object]],
    cycle_id: str,
    policy: GatePolicyProfile,
    phase_statuses: Mapping[str, str],
) -> dict[str, object]:
    metadata = materializations.get(PHASE3_MANIFEST_ASSET_KEY)
    if metadata:
        return _normalize_manifest_fields(metadata)
    return _manifest_fields(cycle_id, policy, phase_statuses)


def _temporal_manifest_fields(
    *,
    temporal_result: object,
    phase_statuses: Mapping[str, str],
) -> dict[str, object]:
    manifest_fields = _plain_mapping(
        getattr(temporal_result, "manifest_fields", {}) or {},
    )
    _assert_temporal_manifest_phase_statuses(manifest_fields, phase_statuses)
    normalized = _normalize_manifest_fields(manifest_fields)
    normalized["phase_statuses"] = _phase_status_rows(phase_statuses)
    return normalized


def _assert_temporal_manifest_phase_statuses(
    manifest_fields: Mapping[str, object],
    phase_statuses: Mapping[str, str],
) -> None:
    actual_phase_statuses = _phase_status_rows_from_value(
        manifest_fields.get("phase_statuses"),
    )
    expected_phase_statuses = tuple(
        row
        for row in _phase_status_rows(phase_statuses)
        if row["phase"] != PhaseEnum.PHASE0.value
    )
    if actual_phase_statuses != expected_phase_statuses:
        raise AssertionError(
            "Temporal manifest phase_statuses did not match "
            "TemporalCycleResult.phase_results",
        )


def _normalize_manifest_fields(
    metadata: Mapping[str, object],
) -> dict[str, object]:
    return {
        key: _normalize_manifest_field(key, value)
        for key, value in metadata.items()
        if key in _PARITY_MANIFEST_FIELD_KEYS
        and key not in _BACKEND_RUNTIME_MANIFEST_FIELD_KEYS
    }


def _normalize_manifest_field(key: str, value: object) -> object:
    if key == "phase_statuses":
        return _phase_status_rows_from_value(value)
    return _plain_metadata_value(value)


def _phase_status_rows(
    phase_statuses: Mapping[str, str],
) -> tuple[dict[str, str], ...]:
    return tuple(
        {"phase": phase, "status": status}
        for phase, status in phase_statuses.items()
    )


def _phase_status_rows_from_value(value: object) -> tuple[dict[str, str], ...]:
    value = _plain_metadata_value(value)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise AssertionError("manifest phase_statuses must be a sequence")

    rows: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise AssertionError("manifest phase_statuses entries must be mappings")
        phase = item.get("phase")
        status = item.get("status")
        if not isinstance(phase, str) or not isinstance(status, str):
            raise AssertionError(
                "manifest phase_statuses entries must include phase and status",
            )
        rows.append({"phase": phase, "status": status})
    return tuple(rows)


def _assert_expected_snapshot(
    case: GateMatrixParityCase | None,
    snapshot: CycleParitySnapshot,
    policy: GatePolicyProfile,
) -> None:
    expected_phase_statuses = _phase_statuses(case)
    actual_phase_statuses = dict(snapshot.phase_statuses)
    if actual_phase_statuses != expected_phase_statuses:
        raise AssertionError(
            f"runtime phase_statuses for {case.case_id if case else 'happy'} "
            f"did not match expected policy outcome: {actual_phase_statuses!r}",
        )

    expected_manifest_fields = _manifest_fields(
        snapshot.cycle_id,
        policy,
        actual_phase_statuses,
    )
    actual_manifest_fields = dict(snapshot.manifest_fields)
    if actual_manifest_fields != expected_manifest_fields:
        raise AssertionError(
            f"runtime manifest_fields for {case.case_id if case else 'happy'} "
            f"did not match expected policy outcome: {actual_manifest_fields!r}",
        )

    expected_rerun_status = (
        "present"
        if case is not None
        and case.action in (GateAction.PARTIAL_RERUN, GateAction.REPAIR_MANIFEST)
        else "not_applicable"
    )
    rerun_key = case.case_id if case is not None else "happy_path"
    actual_rerun_status = snapshot.rerun_request_statuses[rerun_key]
    if actual_rerun_status != expected_rerun_status:
        raise AssertionError(
            f"runtime rerun request status for {rerun_key} was "
            f"{actual_rerun_status!r}, expected {expected_rerun_status!r}",
        )


def _assert_expected_success(
    case: GateMatrixParityCase | None,
    result: object | None,
    temporal_result: object | None,
) -> None:
    if temporal_result is not None:
        expected_success = case is None or case.action is GateAction.MARK_INCONCLUSIVE
        actual_success = getattr(temporal_result, "status", None) == "succeeded"
        if actual_success is not expected_success:
            raise AssertionError(
                f"unexpected workflow success for {case.case_id if case else 'happy'}: "
                f"expected {expected_success}, got {actual_success}",
            )
        return

    if result is None:
        return
    expected_success = case is None or case.action is GateAction.MARK_INCONCLUSIVE
    actual_success = bool(getattr(result, "success", False))
    if actual_success is not expected_success:
        raise AssertionError(
            f"unexpected run success for {case.case_id if case else 'happy'}: "
            f"expected {expected_success}, got {actual_success}",
        )


def _phase_statuses(case: GateMatrixParityCase | None) -> dict[str, str]:
    if case is None:
        return {phase.value: "succeeded" for phase in _PHASES}

    statuses: dict[str, str] = {}
    terminal_seen = False
    for phase in _PHASES:
        if terminal_seen:
            statuses[phase.value] = "skipped"
            continue
        if phase is not case.phase:
            statuses[phase.value] = "succeeded"
            continue
        if case.action is GateAction.MARK_INCONCLUSIVE:
            statuses[phase.value] = "inconclusive"
            continue
        if case.action is GateAction.REPAIR_MANIFEST:
            statuses[phase.value] = "repair_required"
        else:
            statuses[phase.value] = "failed"
        terminal_seen = True
    return statuses


def _manifest_fields(
    cycle_id: str,
    policy: GatePolicyProfile,
    phase_statuses: Mapping[str, str],
) -> dict[str, object]:
    phase3_status = phase_statuses["phase3"]
    if phase3_status == "succeeded":
        publish_status = "published"
    elif phase3_status == "repair_required":
        publish_status = "repair_required"
    else:
        publish_status = "not_published"
    fields: dict[str, object] = {
        "cycle_id": cycle_id,
        "policy_version": policy.policy_version,
        "contract_version": policy.contract_version,
        "publish_status": publish_status,
        "phase_statuses": tuple(
            {"phase": phase, "status": status}
            for phase, status in phase_statuses.items()
        ),
    }
    if any(status == "inconclusive" for status in phase_statuses.values()):
        fields["inconclusive"] = True
    return fields


def _asset_check_evaluations(
    result: object | None,
) -> tuple[_ObservedCheckEvaluation, ...]:
    evaluations: list[_ObservedCheckEvaluation] = []
    for event in _result_events(result):
        if not _is_asset_check_evaluation_event(event):
            continue

        evaluation = _asset_check_evaluation_from_event(event)
        if evaluation is None:
            continue

        name = _check_evaluation_name(evaluation)
        if name is None:
            continue

        passed = getattr(evaluation, "passed", None)
        evaluations.append(
            _ObservedCheckEvaluation(
                name=name,
                passed=passed if isinstance(passed, bool) else None,
                metadata=_plain_mapping(getattr(evaluation, "metadata", {}) or {}),
            )
        )
    return tuple(evaluations)


def _asset_materialization_metadata_by_name(
    result: object | None,
) -> dict[str, Mapping[str, object]]:
    materializations: dict[str, Mapping[str, object]] = {}
    for event in _result_events(result):
        if not _is_asset_materialization_event(event):
            continue

        materialization = _asset_materialization_from_event(event)
        asset_key = getattr(event, "asset_key", None)
        if asset_key is None and materialization is not None:
            asset_key = getattr(materialization, "asset_key", None)
        asset_name = _asset_key_name(asset_key)
        if asset_name is None:
            continue

        raw_metadata = (
            getattr(materialization, "metadata", None)
            if materialization is not None
            else None
        )
        if raw_metadata is None:
            raw_metadata = getattr(event, "metadata", {}) or {}
        materializations[asset_name] = _plain_mapping(raw_metadata or {})
    return materializations


def _failed_step_keys(result: object | None) -> frozenset[str]:
    step_keys: set[str] = set()
    for event in _result_events(result):
        if not (
            getattr(event, "is_step_failure", False)
            or getattr(event, "event_type_value", None) == "STEP_FAILURE"
        ):
            continue
        step_key = getattr(event, "step_key", None)
        if isinstance(step_key, str) and step_key:
            step_keys.add(step_key)
    return frozenset(step_keys)


def _result_events(result: object | None) -> tuple[object, ...]:
    if result is None:
        return ()
    return tuple(getattr(result, "all_events", ()) or ())


def _is_asset_check_evaluation_event(event: object) -> bool:
    return bool(
        getattr(event, "is_asset_check_evaluation", False)
        or getattr(event, "event_type_value", None) == "ASSET_CHECK_EVALUATION"
    )


def _is_asset_materialization_event(event: object) -> bool:
    return bool(
        getattr(event, "is_step_materialization", False)
        or getattr(event, "event_type_value", None) == "ASSET_MATERIALIZATION"
    )


def _asset_check_evaluation_from_event(event: object) -> object | None:
    event_specific_data = getattr(event, "event_specific_data", None)
    for value in (
        getattr(event_specific_data, "asset_check_evaluation", None),
        getattr(event_specific_data, "evaluation", None),
        (
            event_specific_data
            if _looks_like_asset_check_evaluation(event_specific_data)
            else None
        ),
        getattr(event, "asset_check_evaluation", None),
    ):
        if value is not None:
            return value
    return None


def _asset_materialization_from_event(event: object) -> object | None:
    event_specific_data = getattr(event, "event_specific_data", None)
    return getattr(event_specific_data, "materialization", None)


def _looks_like_asset_check_evaluation(value: object) -> bool:
    return value is not None and (
        hasattr(value, "check_name") or hasattr(value, "check_key")
    )


def _check_evaluation_name(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str) and check_name:
        return check_name

    check_key = getattr(evaluation, "check_key", None)
    check_name = getattr(check_key, "name", None)
    if isinstance(check_name, str) and check_name:
        return check_name
    return None


def _asset_key_name(asset_key: object | None) -> str | None:
    if asset_key is None:
        return None
    path = getattr(asset_key, "path", None)
    if isinstance(path, Sequence) and path:
        return str(path[-1])
    if isinstance(asset_key, str) and asset_key:
        return asset_key.rsplit("/", maxsplit=1)[-1]
    return None


def _plain_mapping(metadata: Mapping[object, object]) -> dict[str, object]:
    return {
        str(key): _plain_metadata_value(value)
        for key, value in metadata.items()
    }


def _plain_metadata_value(value: object) -> object:
    for attribute_name in ("value", "text", "data", "path", "url"):
        if not hasattr(value, attribute_name):
            continue
        attribute_value = getattr(value, attribute_name)
        if not callable(attribute_value):
            return _plain_metadata_value(attribute_value)

    if isinstance(value, Mapping):
        return {
            str(key): _plain_metadata_value(item)
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(_plain_metadata_value(item) for item in value)
    return value


def _diagnostic_failed_node(
    *,
    alert_payloads: Sequence[Mapping[str, object]],
    check_evaluations: Sequence[_ObservedCheckEvaluation],
) -> object | None:
    if alert_payloads:
        failed_node = alert_payloads[0].get("failed_node")
        if failed_node:
            return failed_node

    for evaluation in check_evaluations:
        if evaluation.passed is not False:
            continue
        failed_node = evaluation.metadata.get("failed_node")
        if failed_node:
            return failed_node
        failed_nodes = evaluation.metadata.get("failed_nodes")
        if failed_nodes:
            return failed_nodes
    return None


def _diagnostic_runbook_url(
    *,
    alert_payloads: Sequence[Mapping[str, object]],
    gate_decisions: Sequence[GateDecision],
) -> str | None:
    if alert_payloads:
        runbook_url = alert_payloads[0].get("runbook_url")
        if isinstance(runbook_url, str) and runbook_url:
            return runbook_url

    if not gate_decisions:
        return None
    decision = gate_decisions[0]
    if decision.failure_class is None:
        return None
    return runbook_url_for(
        decision.phase.value,
        decision.failure_class.value,
        decision.action.value,
        scenario_id=decision.scenario_id,
    )


def _decision_from_alert(
    alert_payload: Mapping[str, object],
    policy: GatePolicyProfile,
) -> GateDecision:
    from orchestrator.checks.classifier import classify_gate_result

    return classify_gate_result(
        PhaseEnum(str(alert_payload["phase"])),
        {
            "failure_class": str(alert_payload["failure_class"]),
            "scenario_id": str(alert_payload["scenario_id"]),
        },
        policy,
    )


def _stable_alert_payload(record: Mapping[str, object]) -> dict[str, object]:
    payload = cast(Mapping[str, object], record["payload"])
    return {
        "phase": payload.get("phase"),
        "status": payload.get("status"),
        "failed_node": payload.get("failed_node"),
        "action": payload.get("action"),
        "failure_class": payload.get("failure_class"),
        "scenario_id": payload.get("scenario_id"),
        "runbook_url": payload.get("runbook_url"),
        "dispatch_results": tuple(record["dispatch_results"]),
    }


def _rerun_request_payloads(request_dir: Path) -> tuple[Mapping[str, object], ...]:
    if not request_dir.exists():
        return ()
    payloads: list[Mapping[str, object]] = []
    for path in sorted(request_dir.glob("*.json")):
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(loaded, Mapping):
            payloads.append(dict(loaded))
    return tuple(payloads)


def _rerun_request_status(payloads: Sequence[Mapping[str, object]]) -> str:
    if not payloads:
        return "not_applicable"
    if len(payloads) != 1:
        return "invalid"
    selection = _rerun_selection(payloads)
    if not selection:
        return "invalid"
    if PHASE3_FORMAL_COMMIT_ASSET_KEY in selection:
        return "invalid"
    return "present"


def _rerun_selection(payloads: Sequence[Mapping[str, object]]) -> tuple[str, ...]:
    if len(payloads) != 1:
        return ()
    selection = payloads[0].get("rerun_selection")
    if not isinstance(selection, Sequence) or isinstance(selection, (str, bytes)):
        return ()
    return tuple(str(item) for item in selection)


def _classify_case(
    case: GateMatrixParityCase,
    policy: GatePolicyProfile,
) -> GateDecision:
    from orchestrator.checks.classifier import classify_gate_result

    return classify_gate_result(
        case.phase,
        {
            "failure_class": case.failure_class,
            "scenario_id": case.scenario_id,
        },
        policy,
    )


def _dispatch_case_alert(
    decision: GateDecision,
    *,
    cycle_id: str,
    failed_node: str,
    summary: str,
    channels: Iterable[str],
) -> None:
    from orchestrator.checks import dispatch_gate_decision_alert

    dispatch_gate_decision_alert(
        decision,
        cycle_id=cycle_id,
        failed_node=failed_node,
        summary=summary,
        channels=channels,
    )


def _decision_metadata(
    decision: GateDecision,
    *,
    failed_node: str,
    rerun_request_status: str = "not_applicable",
    rerun_selection: Sequence[str] = (),
    inconclusive: str = "not_applicable",
) -> dict[str, object]:
    return {
        "phase": decision.phase.value,
        "failure_class": decision.failure_class.value if decision.failure_class else "",
        "action": decision.action.value,
        "scenario_id": decision.scenario_id or "",
        "failed_node": failed_node,
        "rerun_request_status": rerun_request_status,
        "rerun_selection": list(rerun_selection),
        "inconclusive": inconclusive,
    }


def _failed_node_for(scenario_id: str, phase: PhaseEnum) -> str:
    if scenario_id == "phase0_data_readiness_delayed":
        return PHASE0_READINESS_ASSET_KEY
    if scenario_id == "phase0_llm_health_check_failed":
        return "llm_health_check"
    if scenario_id == "phase0_dbt_test_failed":
        return "heartbeat"
    if scenario_id == "phase1_graph_promotion_snapshot_failed":
        return PHASE1_GRAPH_SNAPSHOT_ASSET_KEY
    if scenario_id == "phase2_single_stock_task_failed":
        return PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY
    if scenario_id == "phase2_pool_failure_rate_exceeded":
        return f"{PHASE2_SINGLE_STOCK_AAPL_ASSET_KEY}, {PHASE2_SINGLE_STOCK_MSFT_ASSET_KEY}"
    if scenario_id == "phase3_formal_commit_failed":
        return PHASE3_FORMAL_COMMIT_ASSET_KEY
    if scenario_id == "phase3_manifest_write_failed":
        return PHASE3_MANIFEST_ASSET_KEY
    if scenario_id == "infra_unavailable_hard_stop":
        return f"{phase.value}_core_resource"
    raise ValueError(f"unhandled scenario_id={scenario_id}")


def _heartbeat_asset_key(dbt_phase0_assets: object) -> object:
    for asset_key in getattr(dbt_phase0_assets, "keys", ()):
        path = tuple(getattr(asset_key, "path", ()))
        if path and path[-1] == "heartbeat":
            return asset_key
    raise AssertionError("dbt heartbeat model asset key was not registered")


def _parity_provider(
    module_factories: Sequence[AssetFactoryProvider],
) -> TemporalParityFakeProvider:
    for module_factory in module_factories:
        if isinstance(module_factory, TemporalParityFakeProvider):
            return module_factory
    raise TypeError("module_factories must include TemporalParityFakeProvider")


def _cycle_id_from_context(context: object, fallback: str) -> str:
    step_context = getattr(context, "_step_execution_context", None)
    for container in (
        context,
        getattr(context, "op_execution_context", None),
        step_context,
        getattr(context, "dagster_run", None),
        getattr(context, "run", None),
        getattr(step_context, "dagster_run", None),
    ):
        if container is None:
            continue
        for attribute_name in ("run_tags", "tags"):
            tags = getattr(container, attribute_name, None)
            if isinstance(tags, Mapping):
                cycle_id = tags.get("cycle_id")
                if isinstance(cycle_id, str) and cycle_id:
                    return cycle_id
    return fallback


def _run_id_from_context(context: object) -> str:
    step_context = getattr(context, "_step_execution_context", None)
    for container in (
        context,
        getattr(context, "op_execution_context", None),
        step_context,
        getattr(context, "dagster_run", None),
        getattr(context, "run", None),
        getattr(step_context, "dagster_run", None),
    ):
        if container is None:
            continue
        run_id = getattr(container, "run_id", None)
        if isinstance(run_id, str) and run_id:
            return run_id
    raise RuntimeError("Dagster context did not expose run_id")


@contextmanager
def _provider_runtime_env(provider: TemporalParityFakeProvider) -> Iterable[None]:
    old_request_dir = os.environ.get(_RERUN_REQUEST_DIR_ENV)
    old_repair_key = os.environ.get(_MANIFEST_REPAIR_ASSET_KEY_ENV)
    os.environ[_RERUN_REQUEST_DIR_ENV] = str(provider.active_request_dir)
    os.environ[_MANIFEST_REPAIR_ASSET_KEY_ENV] = REPAIR_MANIFEST_ASSET_KEY
    try:
        yield
    finally:
        _restore_env(_RERUN_REQUEST_DIR_ENV, old_request_dir)
        _restore_env(_MANIFEST_REPAIR_ASSET_KEY_ENV, old_repair_key)


def _restore_env(key: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value


@contextmanager
def _capture_gate_alerts() -> Iterable[list[Mapping[str, object]]]:
    import orchestrator.checks.decision_handler as decision_handler

    original = decision_handler.dispatch_alert
    records: list[Mapping[str, object]] = []

    def capturing_dispatch_alert(
        payload: object,
        channels: Iterable[str] = ("logging",),
    ) -> object:
        results = original(payload, channels=channels)
        records.append(
            {
                "payload": dataclasses.asdict(payload),
                "dispatch_results": tuple(
                    dataclasses.asdict(result) for result in results
                ),
            },
        )
        return results

    decision_handler.dispatch_alert = capturing_dispatch_alert
    try:
        yield records
    finally:
        decision_handler.dispatch_alert = original


class _ReadinessProvider:
    def __init__(self, owner: TemporalParityFakeProvider) -> None:
        self._owner = owner

    def get_data_readiness_signal(self) -> DataReadinessSignal:
        if self._owner._is_case("phase0_data_readiness_delayed"):
            return DataReadinessSignal(
                ready=False,
                cycle_id=self._owner.cycle_id,
                reason="market data delayed",
                failed_node=PHASE0_READINESS_ASSET_KEY,
            )
        return DataReadinessSignal(ready=True, cycle_id=self._owner.cycle_id)


class _LLMHealthProbe:
    provider = "fake-llm"

    def __init__(self, owner: TemporalParityFakeProvider) -> None:
        self._owner = owner

    def check_health(self) -> object:
        healthy = not self._owner._is_case("phase0_llm_health_check_failed")
        return _LLMHealthReport(
            provider_statuses=(
                _ProviderHealthStatus(
                    provider=self.provider,
                    model="critical-model",
                    reachable=healthy,
                    latency_ms=12.0 if healthy else None,
                    quota_status="available" if healthy else "unavailable",
                    error=None if healthy else "provider unavailable",
                ),
            ),
            all_critical_targets_available=healthy,
            summary="provider ready" if healthy else "provider unavailable",
        )


@dataclass(frozen=True, slots=True)
class _ProviderHealthStatus:
    provider: str
    model: str
    reachable: bool
    latency_ms: float | None
    quota_status: str
    error: str | None


@dataclass(frozen=True, slots=True)
class _LLMHealthReport:
    provider_statuses: tuple[_ProviderHealthStatus, ...]
    all_critical_targets_available: bool
    summary: str


class _GatePolicy:
    def __init__(self, policy: GatePolicyProfile) -> None:
        self.policy = policy


class _RecordingTemporalHandoffClient:
    def __init__(self, owner: TemporalParityFakeProvider) -> None:
        self._owner = owner
        self.requests: list[TemporalCycleRequest] = []

    def start_cycle(self, request: TemporalCycleRequest) -> TemporalHandoffResult:
        self.requests.append(request)
        self._owner.temporal_requests.append(request)
        return TemporalHandoffResult(
            status="started",
            workflow_id=f"workflow-{request.cycle_id}",
            run_id=f"temporal-run-{request.cycle_id}",
        )

__all__ = [
    "GateMatrixParityCase",
    "InProcessTemporalParityExecutor",
    "TemporalParityFakeProvider",
    "build_temporal_parity_fake_provider",
    "execute_dagster_only_snapshot",
    "execute_temporal_snapshot",
    "gate_matrix_parity_cases",
    "parity_policy_path",
]
