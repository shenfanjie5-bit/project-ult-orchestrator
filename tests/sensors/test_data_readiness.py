from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest

from orchestrator.checks import DataReadinessSignal
from orchestrator.policy import FailureClass, GateAction, PhaseEnum, load_gate_policy

REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


class FakeReadinessResource:
    def __init__(self, signal: DataReadinessSignal) -> None:
        self.signal = signal

    def get_data_readiness_signal(self) -> DataReadinessSignal:
        return self.signal


class FakeGatePolicyResource:
    def __init__(self) -> None:
        self.policy = load_gate_policy(LITE_POLICY_PATH)


def _alert_payloads(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in caplog.records:
        if record.name != "orchestrator.alerting.dispatcher":
            continue
        payloads.append(json.loads(record.message))
    return payloads


@pytest.fixture
def sensor_exports() -> dict[str, Any]:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.sensors import data_readiness_sensor

    return {
        "Definitions": dagster.Definitions,
        "RunRequest": dagster.RunRequest,
        "SkipReason": dagster.SkipReason,
        "build_sensor_context": dagster.build_sensor_context,
        "data_readiness_sensor": data_readiness_sensor,
    }


def test_data_readiness_sensor_ready_returns_run_request(
    sensor_exports: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    RunRequest = sensor_exports["RunRequest"]
    build_sensor_context = sensor_exports["build_sensor_context"]
    data_readiness_sensor = sensor_exports["data_readiness_sensor"]
    signal = DataReadinessSignal(ready=True, cycle_id="cycle-20260416")
    context = build_sensor_context(
        resources={
            "data_readiness": FakeReadinessResource(signal),
            "gate_policy": FakeGatePolicyResource(),
        },
    )

    with caplog.at_level(logging.WARNING):
        result = data_readiness_sensor.evaluation_fn(context)

    assert isinstance(result, RunRequest)
    assert result.run_key == "cycle-20260416"
    assert result.run_config == {}
    assert result.tags["cycle_id"] == "cycle-20260416"
    assert result.tags["phase"] == "phase0"
    assert _alert_payloads(caplog) == []


def test_data_readiness_sensor_not_ready_returns_skip_reason(
    sensor_exports: dict[str, Any],
) -> None:
    SkipReason = sensor_exports["SkipReason"]
    build_sensor_context = sensor_exports["build_sensor_context"]
    data_readiness_sensor = sensor_exports["data_readiness_sensor"]
    signal = DataReadinessSignal(
        ready=False,
        cycle_id="cycle-20260416",
        reason="market data delayed",
    )
    context = build_sensor_context(
        resources={
            "data_readiness": FakeReadinessResource(signal),
            "gate_policy": FakeGatePolicyResource(),
        },
    )

    result = data_readiness_sensor.evaluation_fn(context)

    assert isinstance(result, SkipReason)
    assert "market data delayed" in result.skip_message


def test_data_readiness_sensor_not_ready_dispatches_policy_alert(
    sensor_exports: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    build_sensor_context = sensor_exports["build_sensor_context"]
    data_readiness_sensor = sensor_exports["data_readiness_sensor"]
    gate_policy = FakeGatePolicyResource().policy
    first_policy_row = gate_policy.phase_matrix[0]
    signal = DataReadinessSignal(
        ready=False,
        cycle_id="cycle-20260416",
        reason="market data delayed",
    )
    context = build_sensor_context(
        resources={
            "data_readiness": FakeReadinessResource(signal),
            "gate_policy": FakeGatePolicyResource(),
        },
    )

    assert first_policy_row.phase is PhaseEnum.PHASE0
    assert first_policy_row.failure_class is FailureClass.DATA_QUALITY
    assert first_policy_row.action is GateAction.FAIL_RUN

    with caplog.at_level(logging.WARNING):
        data_readiness_sensor.evaluation_fn(context)

    payload = _alert_payloads(caplog)[-1]

    assert payload["cycle_id"] == "cycle-20260416"
    assert payload["phase"] == "phase0"
    assert payload["status"] == "failed"
    assert payload["failed_node"] == "data_readiness"
    assert payload["action"] == "fail_run"
    assert payload["failure_class"] == "data_quality"
    assert payload["summary"] == first_policy_row.description


@pytest.mark.parametrize(
    ("signal", "message"),
    [
        (
            DataReadinessSignal(
                ready="false",  # type: ignore[arg-type]
                cycle_id="cycle-20260416",
            ),
            "ready must be bool",
        ),
        (
            {"ready": True, "cycle_id": ""},
            "cycle_id must be a non-empty string",
        ),
        (
            {"ready": True},
            "cycle_id must be a non-empty string",
        ),
        (
            {
                "ready": True,
                "cycle_id": "cycle-20260416",
                "failed_node": "",
            },
            "failed_node must be a non-empty string",
        ),
        (
            {
                "ready": True,
                "cycle_id": "cycle-20260416",
                "reason": 404,
            },
            "reason must be a string or None",
        ),
    ],
)
def test_data_readiness_sensor_rejects_invalid_provider_signal(
    sensor_exports: dict[str, Any],
    signal: object,
    message: str,
) -> None:
    build_sensor_context = sensor_exports["build_sensor_context"]
    data_readiness_sensor = sensor_exports["data_readiness_sensor"]
    context = build_sensor_context(
        resources={
            "data_readiness": _RawReadinessResource(signal),
            "gate_policy": FakeGatePolicyResource(),
        },
    )

    with pytest.raises(TypeError, match=message):
        data_readiness_sensor.evaluation_fn(context)


def test_data_readiness_sensor_name(sensor_exports: dict[str, Any]) -> None:
    data_readiness_sensor = sensor_exports["data_readiness_sensor"]

    assert data_readiness_sensor.name == "data_readiness_sensor"


def test_data_readiness_sensor_declares_required_resources(
    sensor_exports: dict[str, Any],
) -> None:
    data_readiness_sensor = sensor_exports["data_readiness_sensor"]

    assert data_readiness_sensor.required_resource_keys == {
        "data_readiness",
        "gate_policy",
    }


def test_schedule_and_sensor_can_be_collected_together(
    sensor_exports: dict[str, Any],
) -> None:
    from orchestrator.schedules import daily_cycle_schedule

    Definitions = sensor_exports["Definitions"]
    data_readiness_sensor = sensor_exports["data_readiness_sensor"]

    defs = Definitions(
        schedules=[daily_cycle_schedule],
        sensors=[data_readiness_sensor],
    )

    assert daily_cycle_schedule in defs.schedules
    assert data_readiness_sensor in defs.sensors


class _RawReadinessResource:
    def __init__(self, signal: object) -> None:
        self.signal = signal

    def get_data_readiness_signal(self) -> object:
        return self.signal
