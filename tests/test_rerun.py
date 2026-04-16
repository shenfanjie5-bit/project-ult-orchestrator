from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timezone
from inspect import getsource, signature

import pytest

from orchestrator.policy import FailureClass, GateAction, GatePolicyProfile, PhaseEnum
from orchestrator.policy.schema import PhaseMatrixEntry
from orchestrator.rerun import (
    PartialRerunNotAllowed,
    PartialRerunPlan,
    RunHistorySnapshot,
    UnknownFailedNode,
    compute_partial_rerun_plan,
    plan_partial_rerun,
)


def _policy(*entries: PhaseMatrixEntry) -> GatePolicyProfile:
    return GatePolicyProfile(
        policy_version="test",
        contract_version="stub-0.1",
        execution_backend="dagster_only",
        phase_matrix=list(entries),
        thresholds={},
        alert_channels=[],
        updated_at=datetime(2026, 4, 16, tzinfo=timezone.utc),
    )


def _entry(
    phase: PhaseEnum,
    failure_class: FailureClass,
    action: GateAction,
    *,
    allow_partial_rerun: bool,
    rerun_mode: str | None = None,
) -> PhaseMatrixEntry:
    return PhaseMatrixEntry(
        phase=phase,
        failure_class=failure_class,
        action=action,
        allow_partial_rerun=allow_partial_rerun,
        description="test policy row",
        rerun_mode=rerun_mode,
    )


def test_partial_rerun_plan_fields_match_runtime_model() -> None:
    assert [field.name for field in fields(PartialRerunPlan)] == [
        "run_id",
        "failed_node",
        "rerun_selection",
        "requires_manual_ack",
        "generated_at",
        "rerun_mode",
    ]


def test_plan_partial_rerun_signature_matches_contract() -> None:
    assert list(signature(plan_partial_rerun).parameters) == [
        "run_id",
        "failed_node",
    ]


def test_compute_partial_rerun_plan_signature_matches_pure_contract() -> None:
    assert list(signature(compute_partial_rerun_plan).parameters) == [
        "run_id",
        "failed_node",
        "run_history",
        "policy",
    ]


def test_plan_partial_rerun_facade_remains_importable_and_frozen() -> None:
    plan = plan_partial_rerun("run-1", "phase0_readiness_ping")

    assert plan.run_id == "run-1"
    assert plan.failed_node == "phase0_readiness_ping"
    assert plan.rerun_selection == ("phase0_readiness_ping",)
    assert plan.requires_manual_ack is False
    assert plan.rerun_mode == "asset_only"
    assert plan.generated_at.tzinfo is not None
    with pytest.raises(FrozenInstanceError):
        plan.failed_node = "other"


def test_asset_only_returns_failed_node_without_manual_ack() -> None:
    run_history = RunHistorySnapshot(
        run_id="run-asset",
        node_to_phase={"phase2_score_AAPL": PhaseEnum.PHASE2},
        node_dependencies={"phase2_score_AAPL": ()},
        failed_nodes=("phase2_score_AAPL",),
        repairable_nodes={},
        node_failure_classes={"phase2_score_AAPL": FailureClass.TASK_LEVEL},
    )
    policy = _policy(
        _entry(
            PhaseEnum.PHASE2,
            FailureClass.TASK_LEVEL,
            GateAction.PARTIAL_RERUN,
            allow_partial_rerun=True,
            rerun_mode="asset_only",
        )
    )

    plan = compute_partial_rerun_plan(
        "run-asset",
        "phase2_score_AAPL",
        run_history,
        policy,
    )

    assert plan.rerun_selection == ("phase2_score_AAPL",)
    assert plan.requires_manual_ack is False
    assert plan.rerun_mode == "asset_only"


def test_phase_only_returns_same_phase_downstream_closure() -> None:
    run_history = RunHistorySnapshot(
        run_id="run-phase",
        node_to_phase={
            "phase2_source": PhaseEnum.PHASE2,
            "phase2_transform": PhaseEnum.PHASE2,
            "phase2_score": PhaseEnum.PHASE2,
            "phase2_unrelated": PhaseEnum.PHASE2,
            "phase3_publish": PhaseEnum.PHASE3,
        },
        node_dependencies={
            "phase2_score": ("phase2_transform",),
            "phase3_publish": ("phase2_score",),
            "phase2_transform": ("phase2_source",),
            "phase2_unrelated": (),
        },
        failed_nodes=("phase2_transform",),
        repairable_nodes={},
        node_failure_classes={"phase2_transform": FailureClass.TASK_LEVEL},
    )
    policy = _policy(
        _entry(
            PhaseEnum.PHASE2,
            FailureClass.TASK_LEVEL,
            GateAction.PARTIAL_RERUN,
            allow_partial_rerun=True,
            rerun_mode="phase_only",
        )
    )

    plan = compute_partial_rerun_plan(
        "run-phase",
        "phase2_transform",
        run_history,
        policy,
    )

    assert plan.rerun_selection == ("phase2_transform", "phase2_score")
    assert plan.requires_manual_ack is True
    assert plan.rerun_mode == "phase_only"


def test_repair_only_returns_repair_node_without_upstream_business_assets() -> None:
    run_history = RunHistorySnapshot(
        run_id="run-repair",
        node_to_phase={
            "formal_table_commit": PhaseEnum.PHASE3,
            "publish_manifest": PhaseEnum.PHASE3,
            "phase2_score": PhaseEnum.PHASE2,
        },
        node_dependencies={
            "formal_table_commit": ("phase2_score",),
            "publish_manifest": ("formal_table_commit",),
        },
        failed_nodes=("publish_manifest",),
        repairable_nodes={"publish_manifest": "repair_publish_manifest"},
        node_failure_classes={"publish_manifest": FailureClass.INFRA},
    )
    policy = _policy(
        _entry(
            PhaseEnum.PHASE3,
            FailureClass.INFRA,
            GateAction.REPAIR_MANIFEST,
            allow_partial_rerun=True,
        )
    )

    plan = compute_partial_rerun_plan(
        "run-repair",
        "publish_manifest",
        run_history,
        policy,
    )

    assert plan.rerun_selection == ("repair_publish_manifest",)
    assert "formal_table_commit" not in plan.rerun_selection
    assert "phase2_score" not in plan.rerun_selection
    assert plan.rerun_mode == "repair_only"


def test_unknown_failed_node_mentions_run_id_and_node() -> None:
    run_history = RunHistorySnapshot(
        run_id="run-unknown",
        node_to_phase={},
        node_dependencies={},
        failed_nodes=("missing_node",),
        repairable_nodes={},
    )
    policy = _policy(
        _entry(
            PhaseEnum.PHASE2,
            FailureClass.TASK_LEVEL,
            GateAction.PARTIAL_RERUN,
            allow_partial_rerun=True,
        )
    )

    with pytest.raises(UnknownFailedNode) as exc_info:
        compute_partial_rerun_plan(
            "run-unknown",
            "missing_node",
            run_history,
            policy,
        )

    message = str(exc_info.value)
    assert "run-unknown" in message
    assert "missing_node" in message


def test_policy_not_allowing_partial_rerun_raises() -> None:
    run_history = RunHistorySnapshot(
        run_id="run-denied",
        node_to_phase={"phase0_market_data": PhaseEnum.PHASE0},
        node_dependencies={"phase0_market_data": ()},
        failed_nodes=("phase0_market_data",),
        repairable_nodes={},
        node_failure_classes={"phase0_market_data": FailureClass.DATA_QUALITY},
    )
    policy = _policy(
        _entry(
            PhaseEnum.PHASE0,
            FailureClass.DATA_QUALITY,
            GateAction.FAIL_RUN,
            allow_partial_rerun=False,
        )
    )

    with pytest.raises(PartialRerunNotAllowed, match="partial rerun is not allowed"):
        compute_partial_rerun_plan(
            "run-denied",
            "phase0_market_data",
            run_history,
            policy,
        )


def test_phase_only_selection_order_is_stable_and_unique() -> None:
    run_history = RunHistorySnapshot(
        run_id="run-stable",
        node_to_phase={
            "phase1_root": PhaseEnum.PHASE1,
            "phase1_mid_a": PhaseEnum.PHASE1,
            "phase1_mid_b": PhaseEnum.PHASE1,
            "phase1_leaf": PhaseEnum.PHASE1,
        },
        node_dependencies={
            "phase1_leaf": ("phase1_mid_b", "phase1_mid_a"),
            "phase1_mid_b": ("phase1_root",),
            "phase1_mid_a": ("phase1_root",),
        },
        failed_nodes=("phase1_root",),
        repairable_nodes={},
        node_failure_classes={"phase1_root": FailureClass.TASK_LEVEL},
    )
    policy = _policy(
        _entry(
            PhaseEnum.PHASE1,
            FailureClass.TASK_LEVEL,
            GateAction.PARTIAL_RERUN,
            allow_partial_rerun=True,
            rerun_mode="phase_only",
        )
    )

    first_plan = compute_partial_rerun_plan(
        "run-stable",
        "phase1_root",
        run_history,
        policy,
    )
    second_plan = compute_partial_rerun_plan(
        "run-stable",
        "phase1_root",
        run_history,
        policy,
    )

    assert first_plan.rerun_selection == (
        "phase1_root",
        "phase1_mid_b",
        "phase1_mid_a",
        "phase1_leaf",
    )
    assert first_plan.rerun_selection == second_plan.rerun_selection
    assert len(first_plan.rerun_selection) == len(set(first_plan.rerun_selection))


def test_compute_partial_rerun_plan_source_has_no_storage_or_process_calls() -> None:
    source = getsource(compute_partial_rerun_plan)

    assert "DagsterInstance" not in source
    assert "open(" not in source
    assert "requests" not in source
    assert "subprocess" not in source


def test_plan_partial_rerun_rejects_empty_contract_inputs() -> None:
    with pytest.raises(ValueError, match="run_id is required"):
        plan_partial_rerun("", "node")
    with pytest.raises(ValueError, match="failed_node is required"):
        plan_partial_rerun("run-1", "")
