import dataclasses
import json
import logging

import pytest

from orchestrator.alerting import AlertPayload, dispatch_alert


def test_dispatch_alert_logging_channel_writes_json(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = AlertPayload(
        cycle_id="c1",
        phase="phase0",
        status="failed",
        failed_node="phase0_readiness_ping",
        action="fail_run",
        summary="test",
    )

    with caplog.at_level(logging.WARNING):
        dispatch_alert(payload)

    record = caplog.records[-1]
    logged_payload = json.loads(record.message)

    assert record.levelno == logging.WARNING
    assert logged_payload["cycle_id"] == "c1"
    assert logged_payload["action"] == "fail_run"


def test_dispatch_alert_unknown_channel_warns_without_raising(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = AlertPayload(
        cycle_id="c1",
        phase="phase0",
        status="failed",
        failed_node="phase0_readiness_ping",
        action="fail_run",
        summary="test",
    )

    with caplog.at_level(logging.WARNING):
        dispatch_alert(payload, channels=("unknown",))

    assert caplog.records[-1].message == "unknown alert channel: unknown"


def test_dispatch_alert_ops_channel_uses_logging_backend(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = AlertPayload(
        cycle_id="c1",
        phase="phase0",
        status="failed",
        failed_node="phase0_readiness_ping",
        action="fail_run",
        summary="test",
    )

    with caplog.at_level(logging.WARNING):
        dispatch_alert(payload, channels=("ops",))

    logged_payload = json.loads(caplog.records[-1].message)

    assert logged_payload["cycle_id"] == "c1"
    assert logged_payload["action"] == "fail_run"


def test_alert_payload_optional_fields_allow_none() -> None:
    payload = AlertPayload(
        cycle_id="c1",
        phase="phase0",
        status="failed",
        failed_node=None,
        action="fail_run",
        summary="test",
    )

    assert payload.failed_node is None
    assert payload.runbook_url is None


def test_alert_payload_field_set_matches_runbook_payload() -> None:
    field_names = {field.name for field in dataclasses.fields(AlertPayload)}

    assert field_names == {
        "cycle_id",
        "phase",
        "status",
        "failed_node",
        "action",
        "summary",
        "runbook_url",
    }


def test_alert_payload_required_fields_are_required() -> None:
    with pytest.raises(TypeError):
        AlertPayload(
            cycle_id="c1",
            phase="phase0",
            status="failed",
            failed_node=None,
            action="fail_run",
        )
