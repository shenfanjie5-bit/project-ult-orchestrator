from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from orchestrator.alerting import runbook_url_for
from orchestrator.jobs.phase3 import PHASE3_FORMAL_COMMIT_ASSET_KEY
from orchestrator.policy import GateAction, PhaseEnum, load_gate_policy
from orchestrator.temporal.parity import CycleParitySnapshot
from tests.integration.temporal_parity_fixtures import (
    GateMatrixParityCase,
    build_temporal_parity_fake_provider,
    execute_dagster_only_snapshot,
    execute_temporal_snapshot,
    gate_matrix_parity_cases,
    parity_policy_path,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BASE_POLICY_PATH = _REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"
_POLICY_CASES = gate_matrix_parity_cases(load_gate_policy(_BASE_POLICY_PATH))
_INFRA_CASES = tuple(
    case
    for case in _POLICY_CASES
    if case.scenario_id == "infra_unavailable_hard_stop"
)


def test_happy_path_manifest_and_phase_status_parity(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    tmp_path: Path,
) -> None:
    del tmp_dbt_project
    dagster_snapshot, temporal_snapshot, provider = _execute_pair(
        dagster_module,
        dagster_instance,
        stub_policy_path=stub_policy_path,
        tmp_path=tmp_path,
        cycle_id="cycle-happy-parity",
        case=None,
    )

    assert dagster_snapshot.phase_statuses == temporal_snapshot.phase_statuses
    assert dagster_snapshot.phase_statuses == {
        "phase0": "succeeded",
        "phase1": "succeeded",
        "phase2": "succeeded",
        "phase3": "succeeded",
    }
    assert dagster_snapshot.manifest_fields == temporal_snapshot.manifest_fields
    assert dagster_snapshot.manifest_fields["cycle_id"] == "cycle-happy-parity"
    assert dagster_snapshot.manifest_fields["policy_version"] == "lite-0.1"
    assert dagster_snapshot.manifest_fields["contract_version"] == "stub-0.1"
    assert dagster_snapshot.manifest_fields["publish_status"] == "published"
    assert "temporal_workflow_id" not in dagster_snapshot.manifest_fields
    assert "temporal_run_id" not in dagster_snapshot.manifest_fields
    assert dagster_snapshot.gate_decisions == ()
    assert temporal_snapshot.gate_decisions == ()
    assert dagster_snapshot.alert_payloads == temporal_snapshot.alert_payloads == ()


@pytest.mark.parametrize(
    "case",
    _POLICY_CASES,
    ids=lambda case: case.case_id,
)
def test_gate_matrix_failure_parity(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    tmp_path: Path,
    case: GateMatrixParityCase,
) -> None:
    del tmp_dbt_project
    policy = load_gate_policy(stub_policy_path)
    assert {entry.scenario_id for entry in policy.phase_matrix} <= {
        parity_case.scenario_id for parity_case in _POLICY_CASES
    }

    dagster_snapshot, temporal_snapshot, _provider = _execute_pair(
        dagster_module,
        dagster_instance,
        stub_policy_path=stub_policy_path,
        tmp_path=tmp_path,
        cycle_id=f"cycle-{case.scenario_id}-{case.phase.value}",
        case=case,
    )

    _assert_snapshot_parity(dagster_snapshot, temporal_snapshot)
    _assert_gate_decision(case, dagster_snapshot)
    _assert_alert_payload(case, dagster_snapshot)
    _assert_diagnostics(case, dagster_snapshot)
    if case.phase is PhaseEnum.PHASE0 and case.action is not GateAction.CONTINUE:
        assert provider.temporal_requests == []


@pytest.mark.parametrize(
    "case",
    _INFRA_CASES,
    ids=lambda case: case.case_id,
)
def test_infra_hard_stop_applies_to_phases_parity(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    tmp_path: Path,
    case: GateMatrixParityCase,
) -> None:
    del tmp_dbt_project
    assert {infra_case.phase for infra_case in _INFRA_CASES} == {
        PhaseEnum.PHASE0,
        PhaseEnum.PHASE1,
        PhaseEnum.PHASE2,
        PhaseEnum.PHASE3,
    }

    dagster_snapshot, temporal_snapshot, _provider = _execute_pair(
        dagster_module,
        dagster_instance,
        stub_policy_path=stub_policy_path,
        tmp_path=tmp_path,
        cycle_id=f"cycle-infra-{case.phase.value}",
        case=case,
    )

    _assert_snapshot_parity(dagster_snapshot, temporal_snapshot)
    assert dagster_snapshot.gate_decisions[0].scenario_id == (
        "infra_unavailable_hard_stop"
    )

    if case.phase is PhaseEnum.PHASE0:
        assert dagster_snapshot.phase_statuses["phase0"] == "failed"
        assert dagster_snapshot.phase_statuses["phase1"] == "skipped"
        assert dagster_snapshot.diagnostic_fields["phase1_3_started"] == "no"
    if case.phase is PhaseEnum.PHASE1:
        assert dagster_snapshot.phase_statuses["phase2"] == "skipped"
        assert dagster_snapshot.phase_statuses["phase3"] == "skipped"
    if case.phase is PhaseEnum.PHASE2:
        assert dagster_snapshot.phase_statuses["phase3"] == "skipped"


def test_backend_specific_runtime_ids_are_excluded_from_manifest_parity(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    tmp_path: Path,
) -> None:
    del tmp_dbt_project
    dagster_snapshot, temporal_snapshot, provider = _execute_pair(
        dagster_module,
        dagster_instance,
        stub_policy_path=stub_policy_path,
        tmp_path=tmp_path,
        cycle_id="cycle-runtime-id-parity",
        case=None,
    )

    assert provider.temporal_requests
    assert "temporal_workflow_id" in provider.temporal_requests[0].tags
    assert "temporal_run_id" in provider.temporal_requests[0].tags
    assert dagster_snapshot.manifest_fields == temporal_snapshot.manifest_fields
    for manifest in (
        dagster_snapshot.manifest_fields,
        temporal_snapshot.manifest_fields,
    ):
        assert "temporal_workflow_id" not in manifest
        assert "temporal_run_id" not in manifest


def _execute_pair(
    dagster: object,
    dagster_instance: object,
    *,
    stub_policy_path: str,
    tmp_path: Path,
    cycle_id: str,
    case: GateMatrixParityCase | None,
) -> tuple[CycleParitySnapshot, CycleParitySnapshot, object]:
    case_dir = tmp_path / ((case.case_id if case else "happy").replace(":", "-"))
    dagster_policy_path = parity_policy_path(
        case_dir / "policies" / "dagster",
        stub_policy_path,
        execution_backend="dagster_only",
    )
    temporal_policy_path = parity_policy_path(
        case_dir / "policies" / "temporal",
        stub_policy_path,
        execution_backend="dagster_plus_temporal",
    )
    provider = build_temporal_parity_fake_provider(
        dagster,
        request_root=case_dir / "requests",
        case=case,
    )
    module_factories = (provider,)

    dagster_snapshot = execute_dagster_only_snapshot(
        dagster=dagster,
        instance=dagster_instance,
        policy_path=dagster_policy_path,
        module_factories=module_factories,
        cycle_id=cycle_id,
    )
    temporal_snapshot = asyncio.run(
        execute_temporal_snapshot(
            policy_path=temporal_policy_path,
            module_factories=module_factories,
            cycle_id=cycle_id,
        ),
    )
    return dagster_snapshot, temporal_snapshot, provider


def _assert_snapshot_parity(
    dagster_snapshot: CycleParitySnapshot,
    temporal_snapshot: CycleParitySnapshot,
) -> None:
    assert dagster_snapshot.cycle_id == temporal_snapshot.cycle_id
    assert dagster_snapshot.phase_statuses == temporal_snapshot.phase_statuses
    assert dagster_snapshot.gate_decisions == temporal_snapshot.gate_decisions
    assert dagster_snapshot.manifest_fields == temporal_snapshot.manifest_fields
    assert dagster_snapshot.alert_payloads == temporal_snapshot.alert_payloads
    assert dagster_snapshot.diagnostic_fields == temporal_snapshot.diagnostic_fields
    assert (
        dagster_snapshot.rerun_request_statuses
        == temporal_snapshot.rerun_request_statuses
    )


def _assert_gate_decision(
    case: GateMatrixParityCase,
    snapshot: CycleParitySnapshot,
) -> None:
    assert len(snapshot.gate_decisions) == 1
    decision = snapshot.gate_decisions[0]

    assert decision.phase is case.phase
    assert decision.failure_class is case.failure_class
    assert decision.action is case.action
    assert decision.scenario_id == case.scenario_id
    assert decision.reason


def _assert_alert_payload(
    case: GateMatrixParityCase,
    snapshot: CycleParitySnapshot,
) -> None:
    assert len(snapshot.alert_payloads) == 1
    alert = snapshot.alert_payloads[0]

    assert alert["phase"] == case.phase.value
    assert alert["action"] == case.action.value
    assert alert["failure_class"] == case.failure_class.value
    assert alert["scenario_id"] == case.scenario_id
    assert alert["failed_node"] == case.failed_node
    assert alert["runbook_url"] == runbook_url_for(
        case.phase.value,
        case.failure_class.value,
        case.action.value,
        scenario_id=case.scenario_id,
    )
    dispatch_results = alert["dispatch_results"]
    assert dispatch_results
    assert all(
        {"channel", "delivered", "error"} <= set(result)
        for result in dispatch_results
    )
    assert any(result["channel"] == "ops" for result in dispatch_results)


def _assert_diagnostics(
    case: GateMatrixParityCase,
    snapshot: CycleParitySnapshot,
) -> None:
    diagnostics = snapshot.diagnostic_fields

    assert diagnostics["failed_node"] == case.failed_node
    assert diagnostics["runbook_url"] == runbook_url_for(
        case.phase.value,
        case.failure_class.value,
        case.action.value,
        scenario_id=case.scenario_id,
    )
    assert diagnostics["rerun_request_status"] in {
        "not_applicable",
        "present",
        "missing",
        "invalid",
    }

    if case.action in (GateAction.PARTIAL_RERUN, GateAction.REPAIR_MANIFEST):
        assert diagnostics["rerun_request_status"] == "present"
        assert snapshot.rerun_request_statuses[case.case_id] == "present"
    else:
        assert diagnostics["rerun_request_status"] == "not_applicable"

    if case.action is GateAction.REPAIR_MANIFEST:
        assert PHASE3_FORMAL_COMMIT_ASSET_KEY not in diagnostics["rerun_selection"]
        assert diagnostics["rerun_selection"] == ("repair_cycle_publish_manifest",)

    if case.action is GateAction.MARK_INCONCLUSIVE:
        assert snapshot.phase_statuses["phase2"] == "inconclusive"
        assert snapshot.phase_statuses["phase3"] == "succeeded"
        assert diagnostics["inconclusive"] == "present"

    if case.phase is PhaseEnum.PHASE1 and case.action is GateAction.FAIL_RUN:
        assert snapshot.phase_statuses["phase2"] == "skipped"
        assert snapshot.phase_statuses["phase3"] == "skipped"

    if case.scenario_id == "phase2_pool_failure_rate_exceeded":
        assert snapshot.phase_statuses["phase3"] == "skipped"
