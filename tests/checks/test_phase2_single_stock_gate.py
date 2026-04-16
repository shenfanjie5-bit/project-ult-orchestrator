from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Any

import pytest

from orchestrator.checks import (
    Phase2SingleStockFailureEvent,
    classify_phase2_single_stock_failure,
    inconclusive_metadata,
    phase2_failure_rate,
)
from orchestrator.policy import FailureClass, GateAction, PhaseEnum, load_gate_policy


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


@pytest.fixture
def gate_policy() -> Any:
    return load_gate_policy(LITE_POLICY_PATH)


def test_phase2_single_stock_failure_event_fields_match_contract() -> None:
    assert [field.name for field in fields(Phase2SingleStockFailureEvent)] == [
        "failure_class",
        "stock_id",
        "failed_node",
        "failed_count",
        "total_count",
        "reason",
    ]


def test_phase2_failure_rate_calculates_share() -> None:
    assert phase2_failure_rate(1, 10) == 0.1


@pytest.mark.parametrize(
    ("failed_count", "total_count", "message"),
    [
        (1, 0, "total_count must be greater than 0"),
        (-1, 10, "failed_count must be greater than or equal to 0"),
        (11, 10, "failed_count must be less than or equal to total_count"),
    ],
)
def test_phase2_failure_rate_rejects_invalid_counts(
    failed_count: int,
    total_count: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        phase2_failure_rate(failed_count, total_count)


def test_classify_phase2_single_stock_failure_marks_inconclusive(
    gate_policy: Any,
) -> None:
    event = Phase2SingleStockFailureEvent(
        stock_id="AAPL",
        failed_node="phase2_llm_score_AAPL",
        failed_count=1,
        total_count=10,
        reason="llm timeout",
    )

    decision = classify_phase2_single_stock_failure(event, gate_policy)

    assert event.failure_class is FailureClass.TASK_LEVEL
    assert decision.phase is PhaseEnum.PHASE2
    assert decision.failure_class is FailureClass.TASK_LEVEL
    assert decision.action is GateAction.MARK_INCONCLUSIVE
    assert (
        decision.reason
        == "A single-stock LLM task failed within tolerance; continue the pool."
    )


def test_classify_phase2_single_stock_failure_allows_equal_tolerance(
    gate_policy: Any,
) -> None:
    event = Phase2SingleStockFailureEvent(
        stock_id="MSFT",
        failed_node="phase2_llm_score_MSFT",
        failed_count=5,
        total_count=10,
    )

    decision = classify_phase2_single_stock_failure(event, gate_policy)

    assert decision.action is GateAction.MARK_INCONCLUSIVE


def test_classify_phase2_single_stock_failure_requires_tolerance(
    gate_policy: Any,
) -> None:
    policy = gate_policy.model_copy(update={"thresholds": {}})
    event = Phase2SingleStockFailureEvent(
        stock_id="AAPL",
        failed_node="phase2_llm_score_AAPL",
        failed_count=1,
        total_count=10,
    )

    with pytest.raises(
        ValueError,
        match="missing thresholds.phase2_single_stock_tolerance",
    ):
        classify_phase2_single_stock_failure(event, policy)


@pytest.mark.parametrize("tolerance", [float("nan"), float("inf"), float("-inf")])
def test_classify_phase2_single_stock_failure_rejects_non_finite_tolerance(
    gate_policy: Any,
    tolerance: float,
) -> None:
    policy = gate_policy.model_copy(
        update={
            "thresholds": {
                **gate_policy.thresholds,
                "phase2_single_stock_tolerance": tolerance,
            },
        },
    )
    event = Phase2SingleStockFailureEvent(
        stock_id="AAPL",
        failed_node="phase2_llm_score_AAPL",
        failed_count=1,
        total_count=10,
    )

    with pytest.raises(
        ValueError,
        match=(
            "thresholds.phase2_single_stock_tolerance "
            "must be finite and between 0 and 1"
        ),
    ):
        classify_phase2_single_stock_failure(event, policy)


def test_classify_phase2_single_stock_failure_rejects_pool_threshold_case(
    gate_policy: Any,
) -> None:
    event = Phase2SingleStockFailureEvent(
        stock_id="AAPL",
        failed_node="phase2_llm_score_AAPL",
        failed_count=6,
        total_count=10,
    )

    with pytest.raises(ValueError, match="use the pool threshold gate"):
        classify_phase2_single_stock_failure(event, gate_policy)


def test_inconclusive_metadata_contains_asset_check_fields(gate_policy: Any) -> None:
    event = Phase2SingleStockFailureEvent(
        stock_id="AAPL",
        failed_node="phase2_llm_score_AAPL",
        failed_count=1,
        total_count=10,
        reason="llm timeout",
    )
    decision = classify_phase2_single_stock_failure(event, gate_policy)

    metadata = inconclusive_metadata(event, decision)

    assert metadata == {
        "phase": "phase2",
        "failure_class": "task_level",
        "action": "mark_inconclusive",
        "stock_id": "AAPL",
        "failed_node": "phase2_llm_score_AAPL",
        "failure_rate": 0.1,
        "reason": "llm timeout",
    }
