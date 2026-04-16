from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from inspect import signature
from pathlib import Path
from typing import Any

import pytest

from orchestrator.checks.dbt_events import (
    classify_dbt_test_failure,
    handle_dbt_test_failure,
    plan_dbt_test_partial_rerun,
    stream_dbt_build_events,
    write_dbt_partial_rerun_request,
)
from orchestrator.policy import FailureClass, GateAction, PhaseEnum, load_gate_policy
from orchestrator.rerun import PartialRerunNotAllowed, PartialRerunPlan


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


class FakeAssetKey:
    def __init__(self, path: tuple[str, ...]) -> None:
        self.path = path

    def to_user_string(self) -> str:
        return "/".join(self.path)


class MissingMetadataEvent:
    pass


class FakeDagsterContext:
    run_id = "run-dbt"
    run_tags = {"cycle_id": "cycle-dbt"}

    def __init__(self) -> None:
        self.logged_events: list[object] = []

    def log_event(self, event: object) -> None:
        self.logged_events.append(event)


class FailingDbtInvocation:
    def stream(self) -> Any:
        yield {"event": "dbt build started"}
        raise RuntimeError("dbt build failed")

    def get_artifact(self, artifact_name: str) -> object:
        if artifact_name == "run_results.json":
            return {
                "results": [
                    {
                        "status": "fail",
                        "unique_id": (
                            "test.orchestrator_stub."
                            "not_null_heartbeat_heartbeat"
                        ),
                        "message": "Got 1 result, configured to fail if != 0",
                    },
                ],
            }
        if artifact_name == "manifest.json":
            return _manifest_for_heartbeat_test()
        raise KeyError(artifact_name)


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


def test_handle_dbt_test_failure_dispatches_alert_and_writes_request(
    caplog: pytest.LogCaptureFixture,
    gate_policy: Any,
    tmp_path: Path,
) -> None:
    event = _heartbeat_failure_event()
    context = FakeDagsterContext()

    with caplog.at_level(logging.WARNING):
        result = handle_dbt_test_failure(
            context=context,
            event=event,
            policy=gate_policy,
            request_dir=tmp_path,
        )

    payload = _alert_payloads(caplog)[-1]
    request_payload = json.loads(result.rerun_request_path.read_text())

    assert result.decision.action is GateAction.PARTIAL_RERUN
    assert result.partial_rerun_plan is not None
    assert result.partial_rerun_plan.rerun_selection == ("heartbeat",)
    assert request_payload["run_id"] == "run-dbt"
    assert request_payload["failed_node"] == "heartbeat"
    assert request_payload["rerun_selection"] == ["heartbeat"]
    assert request_payload["requires_manual_ack"] is False
    assert request_payload["rerun_mode"] == "asset_only"
    assert payload["cycle_id"] == "cycle-dbt"
    assert payload["phase"] == "phase0"
    assert payload["failed_node"] == "heartbeat"
    assert payload["action"] == "partial_rerun"
    assert payload["failure_class"] == "task_level"


def test_stream_dbt_build_events_handles_failed_test_artifact(
    caplog: pytest.LogCaptureFixture,
    gate_policy: Any,
    tmp_path: Path,
) -> None:
    stream = stream_dbt_build_events(
        context=FakeDagsterContext(),
        dbt_invocation=FailingDbtInvocation(),
        policy=gate_policy,
        request_dir=tmp_path,
    )

    assert next(stream) == {"event": "dbt build started"}
    with caplog.at_level(logging.WARNING), pytest.raises(
        RuntimeError,
        match="dbt build failed",
    ):
        next(stream)

    payload = _alert_payloads(caplog)[-1]
    request_files = list(tmp_path.glob("*.json"))
    request_payload = json.loads(request_files[0].read_text())

    assert payload["cycle_id"] == "cycle-dbt"
    assert payload["failed_node"] == "heartbeat"
    assert payload["action"] == "partial_rerun"
    assert payload["failure_class"] == "task_level"
    assert request_payload["failed_node"] == "heartbeat"
    assert request_payload["rerun_selection"] == ["heartbeat"]


def test_write_dbt_partial_rerun_request_is_atomically_published(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import orchestrator.checks.dbt_events as dbt_events

    real_replace = os.replace
    sensor_visible_files_before_publish: list[Path] = []

    def replace_spy(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        temp_path = Path(src)
        final_path = Path(dst)

        assert temp_path.parent == tmp_path
        assert temp_path.suffix == ".tmp"
        assert final_path.suffix == ".json"

        sensor_visible_files_before_publish.extend(tmp_path.glob("*.json"))
        real_replace(src, dst)

    monkeypatch.setattr(dbt_events.os, "replace", replace_spy)

    request_path = write_dbt_partial_rerun_request(
        PartialRerunPlan(
            run_id="run-dbt",
            failed_node="heartbeat",
            rerun_selection=("heartbeat",),
            requires_manual_ack=False,
            generated_at=datetime(2026, 4, 16, tzinfo=timezone.utc),
            rerun_mode="asset_only",
        ),
        request_dir=tmp_path,
    )

    assert request_path == tmp_path / "run-dbt-heartbeat.json"
    assert not list(tmp_path.glob("*.tmp"))
    assert not (tmp_path / ".failed").exists()
    assert sensor_visible_files_before_publish == []

    payload = json.loads(request_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "run-dbt"
    assert payload["failed_node"] == "heartbeat"


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


def _heartbeat_failure_event() -> dict[str, object]:
    return {
        "asset_key": "heartbeat",
        "metadata": {
            "node_info": {
                "node_name": "not_null_heartbeat_heartbeat",
                "unique_id": "test.orchestrator_stub.not_null_heartbeat_heartbeat",
            },
            "dbt_message": "Got 1 result, configured to fail if != 0",
        },
    }


def _manifest_for_heartbeat_test() -> dict[str, object]:
    test_unique_id = "test.orchestrator_stub.not_null_heartbeat_heartbeat"
    model_unique_id = "model.orchestrator_stub.heartbeat"

    return {
        "nodes": {
            test_unique_id: {
                "resource_type": "test",
                "name": "not_null_heartbeat_heartbeat",
                "unique_id": test_unique_id,
                "depends_on": {"nodes": [model_unique_id]},
            },
            model_unique_id: {
                "resource_type": "model",
                "name": "heartbeat",
                "unique_id": model_unique_id,
            },
        },
    }


def _alert_payloads(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in caplog.records:
        if record.name != "orchestrator.alerting.dispatcher":
            continue
        payloads.append(json.loads(record.message))
    return payloads
