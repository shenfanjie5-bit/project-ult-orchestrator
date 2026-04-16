from __future__ import annotations

from orchestrator.checks.dbt_events import plan_dbt_test_partial_rerun
from orchestrator.policy import load_gate_policy


def test_dbt_failure_matrix_plans_failed_asset_without_phase0_neighbors(
    stub_policy_path: str,
) -> None:
    policy = load_gate_policy(stub_policy_path)

    plan = plan_dbt_test_partial_rerun(
        "run-phase0-dbt",
        "dbt_phase0_assets",
        {"metadata": {"node_info": {"node_name": "not_null_heartbeat"}}},
        policy,
    )

    assert plan.rerun_selection == ("dbt_phase0_assets",)
    assert plan.requires_manual_ack is False
    assert "phase0_readiness_ping" not in plan.rerun_selection
    assert "candidate_freeze" not in plan.rerun_selection
