from __future__ import annotations

from inspect import signature
from pathlib import Path
from typing import Any

import pytest

from orchestrator.checks.dbt_events import (
    classify_dbt_test_failure,
    plan_dbt_test_partial_rerun,
)
from orchestrator.policy import FailureClass, GateAction, PhaseEnum, load_gate_policy
from orchestrator.rerun import PartialRerunNotAllowed


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


class FakeAssetKey:
    def __init__(self, path: tuple[str, ...]) -> None:
        self.path = path

    def to_user_string(self) -> str:
        return "/".join(self.path)


class MissingMetadataEvent:
    pass


@pytest.fixture
def gate_policy() -> Any:
    return load_gate_policy(LITE_POLICY_PATH)


def test_dbt_adapter_signatures_match_issue_contract() -> None:
    assert list(signature(classify_dbt_test_failure).parameters) == [
        "event",
        "policy",
    ]
    assert list(signature(plan_dbt_test_partial_rerun).parameters) == [
        "run_id",
        "failed_asset_key",
        "event",
        "policy",
    ]


def test_classify_dbt_test_failure_from_node_name(gate_policy: Any) -> None:
    event = {
        "metadata": {
            "node_info": {
                "node_name": "not_null_heartbeat_heartbeat",
            }
        }
    }

    decision = classify_dbt_test_failure(event, gate_policy)

    assert decision.phase is PhaseEnum.PHASE0
    assert decision.failure_class is FailureClass.TASK_LEVEL
    assert decision.action is GateAction.PARTIAL_RERUN
    assert (
        decision.reason
        == "dbt test failed; rerun the repaired asset group after the fix."
    )


def test_plan_dbt_test_partial_rerun_uses_validated_event_asset_key(
    gate_policy: Any,
) -> None:
    event = {
        "asset_key": FakeAssetKey(("dbt_phase0_assets",)),
        "metadata": {
            "node_info": {
                "node_name": "not_null_heartbeat_heartbeat",
            }
        },
    }

    plan = plan_dbt_test_partial_rerun(
        "run-dbt",
        FakeAssetKey(("dbt_phase0_assets",)),
        event,
        gate_policy,
    )

    assert plan.run_id == "run-dbt"
    assert plan.failed_node == "dbt_phase0_assets"
    assert plan.rerun_selection == ("dbt_phase0_assets",)
    assert plan.requires_manual_ack is False
    assert plan.rerun_mode == "asset_only"
    assert "phase0_readiness_ping" not in plan.rerun_selection
    assert "candidate_freeze" not in plan.rerun_selection


def test_plan_dbt_test_partial_rerun_rejects_asset_key_mismatch(
    gate_policy: Any,
) -> None:
    event = {
        "asset_key": FakeAssetKey(("other_dbt_asset",)),
        "metadata": {
            "node_info": {
                "node_name": "not_null_heartbeat_heartbeat",
            }
        },
    }

    with pytest.raises(ValueError, match="does not match failed_asset_key"):
        plan_dbt_test_partial_rerun(
            "run-dbt",
            FakeAssetKey(("dbt_phase0_assets",)),
            event,
            gate_policy,
        )


def test_dbt_test_failure_with_missing_metadata_still_maps_to_task_level(
    gate_policy: Any,
) -> None:
    decision = classify_dbt_test_failure(MissingMetadataEvent(), gate_policy)

    assert decision.phase is PhaseEnum.PHASE0
    assert decision.failure_class is FailureClass.TASK_LEVEL
    assert decision.action is GateAction.PARTIAL_RERUN


def test_plan_dbt_test_partial_rerun_rejects_dbt_group_without_event_asset_key(
    gate_policy: Any,
) -> None:
    with pytest.raises(ValueError, match="asset_key metadata is required"):
        plan_dbt_test_partial_rerun(
            "run-missing-metadata",
            "dbt_phase0_assets",
            MissingMetadataEvent(),
            gate_policy,
        )


def test_plan_dbt_test_partial_rerun_rejects_unvalidated_missing_asset_metadata(
    gate_policy: Any,
) -> None:
    with pytest.raises(ValueError, match="asset_key metadata is required"):
        plan_dbt_test_partial_rerun(
            "run-missing-metadata",
            "heartbeat",
            MissingMetadataEvent(),
            gate_policy,
        )


def test_dbt_partial_rerun_denied_policy_raises(gate_policy: Any) -> None:
    policy = _with_phase0_task_partial_allowed(gate_policy, allow_partial_rerun=False)

    with pytest.raises(PartialRerunNotAllowed, match="partial rerun is not allowed"):
        plan_dbt_test_partial_rerun(
            "run-denied",
            "dbt_phase0_assets",
            {"asset_key": "dbt_phase0_assets"},
            policy,
        )


def test_classify_dbt_test_failure_requires_event(gate_policy: Any) -> None:
    with pytest.raises(TypeError, match="dbt test failure event is required"):
        classify_dbt_test_failure(None, gate_policy)


def _with_phase0_task_partial_allowed(
    policy: Any,
    *,
    allow_partial_rerun: bool,
) -> Any:
    return policy.model_copy(
        update={
            "phase_matrix": [
                entry.model_copy(
                    update={"allow_partial_rerun": allow_partial_rerun},
                )
                if (
                    entry.phase is PhaseEnum.PHASE0
                    and entry.failure_class is FailureClass.TASK_LEVEL
                )
                else entry
                for entry in policy.phase_matrix
            ],
        },
    )
