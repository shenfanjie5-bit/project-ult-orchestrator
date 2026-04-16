from __future__ import annotations

import json
import logging
from inspect import signature
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from orchestrator.checks.dbt_events import (
    classify_dbt_test_failure,
    handle_dbt_test_failures,
    plan_dbt_test_partial_rerun,
    stream_dbt_events_with_gate_handling,
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


class FakeDbtCheckEvent:
    def __init__(
        self,
        *,
        asset_key: object,
        check_name: str,
        unique_id: str | None = None,
    ) -> None:
        self.passed = False
        self.asset_key = asset_key
        self.check_name = check_name
        self.metadata = {}
        if unique_id is not None:
            self.metadata["unique_id"] = unique_id


class FakeDbtInvocation:
    def __init__(
        self,
        events: tuple[object, ...],
        *,
        artifacts: dict[str, object] | None = None,
        error: Exception | None = None,
    ) -> None:
        self._events = events
        self._artifacts = artifacts or {}
        self._error = error

    def stream(self) -> Any:
        yield from self._events
        if self._error is not None:
            raise self._error

    def get_artifact(self, name: str) -> object | None:
        return self._artifacts.get(name)


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


def test_stream_dbt_events_handles_failed_check_with_alert_and_request(
    gate_policy: Any,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    failed_check = FakeDbtCheckEvent(
        asset_key=FakeAssetKey(("heartbeat",)),
        check_name="not_null_heartbeat_heartbeat",
        unique_id="test.orchestrator_stub.not_null_heartbeat_heartbeat",
    )
    invocation = FakeDbtInvocation((failed_check,))
    context = _fake_context(run_id="run-dbt", cycle_id="cycle-dbt")

    with caplog.at_level(logging.WARNING):
        emitted_events = list(
            stream_dbt_events_with_gate_handling(
                context=context,
                dbt_invocation=invocation,
                policy=gate_policy,
                request_dir=tmp_path,
            ),
        )

    observation = emitted_events[-1]
    request = json.loads((tmp_path / "run-dbt-heartbeat.json").read_text())
    alert = _alert_payloads(caplog)[-1]

    assert emitted_events[0] is failed_check
    assert _observation_asset_key(observation) == "heartbeat"
    assert _observation_metadata_value(observation, "action") == "partial_rerun"
    assert _observation_metadata_value(observation, "failure_class") == "task_level"
    assert _observation_metadata_value(observation, "failed_node") == "heartbeat"
    assert request["run_id"] == "run-dbt"
    assert request["failed_node"] == "heartbeat"
    assert request["rerun_selection"] == ["heartbeat"]
    assert request["rerun_mode"] == "asset_only"
    assert alert["cycle_id"] == "cycle-dbt"
    assert alert["failed_node"] == "heartbeat"
    assert alert["action"] == "partial_rerun"
    assert alert["failure_class"] == "task_level"


def test_rerun_request_write_failure_still_alerts_and_observes(
    gate_policy: Any,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    request_dir = tmp_path / "not-a-directory"
    request_dir.write_text("already a file", encoding="utf-8")
    failed_check = FakeDbtCheckEvent(
        asset_key=FakeAssetKey(("heartbeat",)),
        check_name="not_null_heartbeat_heartbeat",
    )

    with caplog.at_level(logging.WARNING):
        emitted_events = list(
            stream_dbt_events_with_gate_handling(
                context=_fake_context(run_id="run-dbt", cycle_id="cycle-dbt"),
                dbt_invocation=FakeDbtInvocation((failed_check,)),
                policy=gate_policy,
                request_dir=request_dir,
            ),
        )

    observation = emitted_events[-1]
    alert = _alert_payloads(caplog)[-1]

    assert _observation_metadata_value(observation, "action") == "partial_rerun"
    assert "File exists" in str(
        _observation_metadata_value(observation, "rerun_request_write_error"),
    )
    assert "rerun request write failed" in str(alert["summary"])
    assert not (tmp_path / "not-a-directory-heartbeat.json").exists()


def test_handle_dbt_test_failures_covers_multiple_failed_assets(
    gate_policy: Any,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    first = FakeDbtCheckEvent(
        asset_key=FakeAssetKey(("heartbeat",)),
        check_name="not_null_heartbeat_heartbeat",
    )
    second = FakeDbtCheckEvent(
        asset_key=FakeAssetKey(("orders",)),
        check_name="not_null_orders_order_id",
    )

    with caplog.at_level(logging.WARNING):
        results = handle_dbt_test_failures(
            (first, second),
            run_id="run-dbt",
            cycle_id="cycle-dbt",
            policy=gate_policy,
            request_dir=tmp_path,
        )

    request_files = sorted(path.name for path in tmp_path.glob("*.json"))
    alerts = _alert_payloads(caplog)

    assert [result.event.asset_key for result in results] == ["heartbeat", "orders"]
    assert request_files == [
        "run-dbt-heartbeat.json",
        "run-dbt-orders.json",
    ]
    assert [alert["failed_node"] for alert in alerts[-2:]] == ["heartbeat", "orders"]


def test_stream_dbt_events_uses_failed_artifacts_before_reraising(
    gate_policy: Any,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    invocation = FakeDbtInvocation(
        (),
        artifacts=_failed_dbt_artifacts(),
        error=RuntimeError("dbt build failed"),
    )
    events = stream_dbt_events_with_gate_handling(
        context=_fake_context(run_id="run-dbt", cycle_id="cycle-dbt"),
        dbt_invocation=invocation,
        policy=gate_policy,
        request_dir=tmp_path,
    )

    with caplog.at_level(logging.WARNING):
        observation = next(events)
        with pytest.raises(RuntimeError, match="dbt build failed"):
            next(events)

    assert _observation_asset_key(observation) == "heartbeat"
    assert _observation_metadata_value(observation, "action") == "partial_rerun"
    assert (tmp_path / "run-dbt-heartbeat.json").exists()
    assert _alert_payloads(caplog)[-1]["failed_node"] == "heartbeat"


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


def _fake_context(run_id: str, cycle_id: str) -> object:
    return SimpleNamespace(
        run_id=run_id,
        run=SimpleNamespace(tags={"cycle_id": cycle_id}),
    )


def _failed_dbt_artifacts() -> dict[str, object]:
    test_unique_id = "test.orchestrator_stub.not_null_heartbeat_heartbeat"
    model_unique_id = "model.orchestrator_stub.heartbeat"
    return {
        "run_results.json": {
            "results": [
                {
                    "status": "fail",
                    "unique_id": test_unique_id,
                },
            ],
        },
        "manifest.json": {
            "nodes": {
                test_unique_id: {
                    "name": "not_null_heartbeat_heartbeat",
                    "resource_type": "test",
                    "depends_on": {"nodes": [model_unique_id]},
                },
                model_unique_id: {
                    "name": "heartbeat",
                    "resource_type": "model",
                },
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


def _observation_asset_key(observation: object) -> str:
    asset_key = getattr(observation, "asset_key")
    if isinstance(asset_key, str):
        return asset_key
    to_user_string = getattr(asset_key, "to_user_string", None)
    if callable(to_user_string):
        return to_user_string()
    path = getattr(asset_key, "path", None)
    if isinstance(path, (list, tuple)):
        return "/".join(str(part) for part in path)
    return str(asset_key)


def _observation_metadata_value(observation: object, key: str) -> object:
    metadata = getattr(observation, "metadata", {}) or {}
    value = metadata[key]
    return getattr(value, "value", getattr(value, "text", value))
