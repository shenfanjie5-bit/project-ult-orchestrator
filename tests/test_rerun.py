from dataclasses import FrozenInstanceError, fields
from inspect import signature

import pytest

from orchestrator.rerun import PartialRerunPlan, plan_partial_rerun


def test_partial_rerun_plan_fields_match_runtime_model() -> None:
    assert [field.name for field in fields(PartialRerunPlan)] == [
        "run_id",
        "failed_node",
        "rerun_selection",
        "requires_manual_ack",
        "generated_at",
    ]


def test_plan_partial_rerun_signature_matches_contract() -> None:
    assert list(signature(plan_partial_rerun).parameters) == [
        "run_id",
        "failed_node",
    ]


def test_plan_partial_rerun_returns_minimal_failed_node_selection() -> None:
    plan = plan_partial_rerun("run-1", "phase0_readiness_ping")

    assert plan.run_id == "run-1"
    assert plan.failed_node == "phase0_readiness_ping"
    assert plan.rerun_selection == ("phase0_readiness_ping",)
    assert plan.requires_manual_ack is True
    assert plan.generated_at.tzinfo is not None
    with pytest.raises(FrozenInstanceError):
        plan.failed_node = "other"


def test_plan_partial_rerun_rejects_empty_contract_inputs() -> None:
    with pytest.raises(ValueError, match="run_id is required"):
        plan_partial_rerun("", "node")
    with pytest.raises(ValueError, match="failed_node is required"):
        plan_partial_rerun("run-1", "")
