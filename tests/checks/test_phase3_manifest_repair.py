from __future__ import annotations

import json
import logging
from dataclasses import fields
from pathlib import Path
from typing import Any

import pytest

from orchestrator.checks import (
    ManifestWriteFailureEvent,
    classify_manifest_write_failure,
    dispatch_gate_decision_alert,
    plan_manifest_repair_rerun,
)
from orchestrator.policy import FailureClass, GateAction, PhaseEnum, load_gate_policy
from orchestrator.rerun_request import request_from_plan


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"
PHASE3_FORMAL_COMMIT_ASSET_KEY = "formal_objects_commit"
PHASE3_MANIFEST_ASSET_KEY = "cycle_publish_manifest"
REPAIR_MANIFEST_ASSET_KEY = "repair_cycle_publish_manifest"


@pytest.fixture
def gate_policy() -> Any:
    return load_gate_policy(LITE_POLICY_PATH)


def test_manifest_write_failure_event_fields_match_contract() -> None:
    assert [field.name for field in fields(ManifestWriteFailureEvent)] == [
        "failure_class",
        "failed_node",
        "repair_node",
        "reason",
    ]


def test_classify_manifest_write_failure_repairs_manifest(
    gate_policy: Any,
) -> None:
    event = ManifestWriteFailureEvent(
        repair_node=REPAIR_MANIFEST_ASSET_KEY,
        reason="manifest write failed",
    )

    decision = classify_manifest_write_failure(event, gate_policy)

    assert event.failure_class is FailureClass.INFRA
    assert event.failed_node == PHASE3_MANIFEST_ASSET_KEY
    assert decision.phase is PhaseEnum.PHASE3
    assert decision.failure_class is FailureClass.INFRA
    assert decision.action is GateAction.REPAIR_MANIFEST
    assert (
        decision.reason
        == "Manifest write failed; alert and expose repair-only rerun."
    )


def test_plan_manifest_repair_rerun_selects_only_repair_asset(
    gate_policy: Any,
) -> None:
    event = ManifestWriteFailureEvent(
        repair_node=REPAIR_MANIFEST_ASSET_KEY,
        reason="manifest write failed",
    )

    plan = plan_manifest_repair_rerun("run-phase3", event, gate_policy)
    request = request_from_plan(plan)

    assert plan.failed_node == PHASE3_MANIFEST_ASSET_KEY
    assert plan.rerun_selection == (REPAIR_MANIFEST_ASSET_KEY,)
    assert plan.rerun_mode == "repair_only"
    assert plan.requires_manual_ack is True
    assert PHASE3_FORMAL_COMMIT_ASSET_KEY not in plan.rerun_selection
    assert PHASE3_MANIFEST_ASSET_KEY not in plan.rerun_selection
    assert request["rerun_selection"] == [REPAIR_MANIFEST_ASSET_KEY]
    assert request["rerun_mode"] == "repair_only"
    assert request["requires_manual_ack"] is True


def test_plan_manifest_repair_rerun_rejects_missing_repair_node(
    gate_policy: Any,
) -> None:
    event = ManifestWriteFailureEvent(repair_node=" ")

    with pytest.raises(ValueError, match="repair_node is required"):
        plan_manifest_repair_rerun("run-phase3", event, gate_policy)


def test_manifest_write_failure_alert_payload_contains_repair_action(
    gate_policy: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    event = ManifestWriteFailureEvent(
        repair_node=REPAIR_MANIFEST_ASSET_KEY,
        reason="manifest write failed",
    )
    decision = classify_manifest_write_failure(event, gate_policy)

    with caplog.at_level(logging.WARNING):
        dispatch_gate_decision_alert(
            decision,
            cycle_id="cycle-20260416",
            failed_node=event.failed_node,
            summary=event.reason or "manifest write failed",
            channels=("logging",),
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload["phase"] == "phase3"
    assert payload["failure_class"] == "infra"
    assert payload["action"] == "repair_manifest"
    assert payload["failed_node"] == PHASE3_MANIFEST_ASSET_KEY
