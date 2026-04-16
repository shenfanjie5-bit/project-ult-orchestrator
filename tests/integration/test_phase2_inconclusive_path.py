from __future__ import annotations

from tests.integration.conftest import (
    asset_check_evaluations,
    asset_materialization_keys,
    metadata_value,
)


def test_phase2_single_stock_inconclusive_check_does_not_block_daily_cycle(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
) -> None:
    dagster = dagster_module

    from orchestrator.checks import (
        Phase2SingleStockFailureEvent,
        classify_phase2_single_stock_failure,
        inconclusive_metadata,
    )
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.jobs.phase0_constants import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GROUP_NAME,
        PHASE0_READINESS_ASSET_KEY,
    )
    from orchestrator.jobs.phase1 import (
        PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        PHASE1_GROUP_NAME,
    )
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME
    from orchestrator.policy import GateAction

    @dagster.asset(name=PHASE0_READINESS_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def phase0_readiness_ping() -> str:
        return "ready"

    @dagster.asset(name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def candidate_freeze() -> str:
        return "frozen"

    @dagster.asset(
        name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
        deps=[
            dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
            dagster.AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
        ],
    )
    def graph_snapshot() -> str:
        return "snapshot"

    @dagster.asset(
        name="phase2_stock_AAPL",
        group_name=PHASE2_GROUP_NAME,
        deps=[dagster.AssetKey([PHASE1_GRAPH_SNAPSHOT_ASSET_KEY])],
    )
    def phase2_stock_aapl() -> str:
        return "aapl placeholder"

    @dagster.asset(
        name="phase2_stock_MSFT",
        group_name=PHASE2_GROUP_NAME,
        deps=[dagster.AssetKey([PHASE1_GRAPH_SNAPSHOT_ASSET_KEY])],
    )
    def phase2_stock_msft() -> str:
        return "msft placeholder"

    @dagster.asset_check(
        asset=phase2_stock_aapl,
        name="phase2_aapl_single_stock_gate",
        blocking=False,
    )
    def phase2_aapl_single_stock_gate(gate_policy: GatePolicyResource) -> object:
        event = Phase2SingleStockFailureEvent(
            stock_id="AAPL",
            failed_node="phase2_stock_AAPL",
            failed_count=1,
            total_count=10,
            reason="fake LLM task failed",
        )
        decision = classify_phase2_single_stock_failure(event, gate_policy.policy)
        return dagster.AssetCheckResult(
            passed=decision.action is GateAction.CONTINUE,
            metadata=inconclusive_metadata(event, decision),
        )

    defs = dagster.Definitions(
        assets=[
            phase0_readiness_ping,
            candidate_freeze,
            graph_snapshot,
            phase2_stock_aapl,
            phase2_stock_msft,
        ],
        asset_checks=[phase2_aapl_single_stock_gate],
        jobs=[daily_cycle_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )
    dagster.Definitions.validate_loadable(defs)

    result = defs.get_job_def("daily_cycle_job").execute_in_process(
        instance=dagster_instance,
        tags={"cycle_id": "cycle-20260416"},
    )
    materialized_keys = asset_materialization_keys(result)
    evaluation = _single_evaluation(
        asset_check_evaluations(result),
        "phase2_aapl_single_stock_gate",
    )

    assert result.success is True
    assert dagster.AssetKey(["phase2_stock_AAPL"]) in materialized_keys
    assert dagster.AssetKey(["phase2_stock_MSFT"]) in materialized_keys
    assert getattr(evaluation, "passed", None) is False
    assert metadata_value(evaluation, "action") == "mark_inconclusive"
    assert (
        metadata_value(evaluation, "scenario_id")
        == "phase2_single_stock_task_failed"
    )
    assert metadata_value(evaluation, "stock_id") == "AAPL"
    assert metadata_value(evaluation, "failure_rate") == 0.1


def _single_evaluation(evaluations: list[object], check_name: str) -> object:
    matches = [
        evaluation
        for evaluation in evaluations
        if _check_name(evaluation) == check_name
    ]
    assert len(matches) == 1
    return matches[0]


def _check_name(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str):
        return check_name
    check_key = getattr(evaluation, "check_key", None)
    name = getattr(check_key, "name", None)
    return name if isinstance(name, str) else None
