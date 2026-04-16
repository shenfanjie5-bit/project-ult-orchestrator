import dataclasses
import json
import logging
import urllib.error
import urllib.request

import pytest

import orchestrator.alerting.dispatcher as dispatcher
from orchestrator.alerting import AlertDispatchResult, AlertPayload, dispatch_alert


class _FakeResponse:
    def __init__(self) -> None:
        self.closed = False

    def read(self) -> bytes:
        return b"ok"

    def close(self) -> None:
        self.closed = True


class _RecordingOpener:
    def __init__(self, exception: Exception | None = None) -> None:
        self.exception = exception
        self.requests: list[tuple[urllib.request.Request, int]] = []

    def open(
        self,
        request: urllib.request.Request,
        *,
        timeout: int,
    ) -> _FakeResponse:
        self.requests.append((request, timeout))
        if self.exception is not None:
            raise self.exception
        return _FakeResponse()


@pytest.fixture
def payload() -> AlertPayload:
    return AlertPayload(
        cycle_id="c1",
        phase="phase0",
        status="failed",
        failed_node="phase0_readiness_ping",
        action="fail_run",
        summary="test",
        failure_class="infra",
        scenario_id="phase0_llm_health_check_failed",
        runbook_url="https://runbooks.example/c1",
    )


def test_dispatch_alert_logging_channel_writes_json(
    caplog: pytest.LogCaptureFixture,
    payload: AlertPayload,
) -> None:
    with caplog.at_level(logging.WARNING):
        results = dispatch_alert(payload)

    record = caplog.records[-1]
    logged_payload = json.loads(record.message)

    assert record.levelno == logging.WARNING
    assert logged_payload["cycle_id"] == "c1"
    assert logged_payload["action"] == "fail_run"
    assert results == (
        AlertDispatchResult(channel="logging", delivered=True),
    )


def test_dispatch_alert_unknown_channel_warns_without_raising(
    caplog: pytest.LogCaptureFixture,
    payload: AlertPayload,
) -> None:
    with caplog.at_level(logging.WARNING):
        results = dispatch_alert(payload, channels=("unknown",))

    assert caplog.records[-1].message == "unknown alert channel: unknown"
    assert results == (
        AlertDispatchResult(
            channel="unknown",
            delivered=False,
            error="unknown alert channel: unknown",
        ),
    )


def test_dispatch_alert_ops_channel_uses_logging_backend(
    caplog: pytest.LogCaptureFixture,
    payload: AlertPayload,
) -> None:
    with caplog.at_level(logging.WARNING):
        results = dispatch_alert(payload, channels=("ops",))

    logged_payload = json.loads(caplog.records[-1].message)

    assert logged_payload["cycle_id"] == "c1"
    assert logged_payload["action"] == "fail_run"
    assert results == (
        AlertDispatchResult(channel="ops", delivered=True),
    )


def test_dispatch_alert_slack_channel_posts_minimal_payload(
    monkeypatch: pytest.MonkeyPatch,
    payload: AlertPayload,
) -> None:
    opener = _RecordingOpener()
    monkeypatch.setattr(dispatcher, "_HTTP_OPENER", opener)
    monkeypatch.setenv(
        "ORCHESTRATOR_SLACK_WEBHOOK_URL",
        "https://hooks.slack.example/test",
    )

    results = dispatch_alert(payload, channels=("slack",))

    assert results == (
        AlertDispatchResult(channel="slack", delivered=True),
    )
    assert len(opener.requests) == 1
    request, timeout = opener.requests[0]
    body = json.loads(request.data.decode("utf-8"))

    assert request.full_url == "https://hooks.slack.example/test"
    assert request.get_method() == "POST"
    assert request.headers["Content-type"] == "application/json"
    assert timeout == 5
    assert body["cycle_id"] == payload.cycle_id
    assert body["phase"] == payload.phase
    assert body["action"] == payload.action
    assert body["failure_class"] == payload.failure_class
    assert body["scenario_id"] == payload.scenario_id
    assert body["summary"] == payload.summary
    assert body["runbook_url"] == payload.runbook_url


def test_dispatch_alert_webhook_channel_posts_alert_payload(
    monkeypatch: pytest.MonkeyPatch,
    payload: AlertPayload,
) -> None:
    opener = _RecordingOpener()
    monkeypatch.setattr(dispatcher, "_HTTP_OPENER", opener)
    monkeypatch.setenv(
        "ORCHESTRATOR_ALERT_WEBHOOK_URL",
        "https://alerts.example/gate",
    )

    results = dispatch_alert(payload, channels=("webhook",))

    assert results == (
        AlertDispatchResult(channel="webhook", delivered=True),
    )
    assert len(opener.requests) == 1
    request, _timeout = opener.requests[0]
    body = json.loads(request.data.decode("utf-8"))

    assert request.full_url == "https://alerts.example/gate"
    assert request.get_method() == "POST"
    assert request.headers["Content-type"] == "application/json"
    assert body == dataclasses.asdict(payload)


@pytest.mark.parametrize(
    ("channel", "env_var"),
    [
        ("slack", "ORCHESTRATOR_SLACK_WEBHOOK_URL"),
        ("webhook", "ORCHESTRATOR_ALERT_WEBHOOK_URL"),
    ],
)
def test_dispatch_alert_http_channel_missing_endpoint_warns_without_raising(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    payload: AlertPayload,
    channel: str,
    env_var: str,
) -> None:
    opener = _RecordingOpener()
    monkeypatch.setattr(dispatcher, "_HTTP_OPENER", opener)
    monkeypatch.delenv(env_var, raising=False)

    with caplog.at_level(logging.WARNING):
        results = dispatch_alert(payload, channels=(channel,))

    assert opener.requests == []
    assert results == (
        AlertDispatchResult(
            channel=channel,
            delivered=False,
            error=f"missing alert endpoint: {env_var}",
        ),
    )
    assert f"{channel} alert channel failed" in caplog.records[-1].message
    assert env_var in caplog.records[-1].message


@pytest.mark.parametrize(
    ("channel", "env_var"),
    [
        ("slack", "ORCHESTRATOR_SLACK_WEBHOOK_URL"),
        ("webhook", "ORCHESTRATOR_ALERT_WEBHOOK_URL"),
    ],
)
def test_dispatch_alert_http_error_warns_without_raising(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    payload: AlertPayload,
    channel: str,
    env_var: str,
) -> None:
    http_error = urllib.error.HTTPError(
        url="https://alerts.example/gate",
        code=500,
        msg="server error",
        hdrs=None,
        fp=None,
    )
    opener = _RecordingOpener(exception=http_error)
    monkeypatch.setattr(dispatcher, "_HTTP_OPENER", opener)
    monkeypatch.setenv(env_var, "https://alerts.example/gate")

    with caplog.at_level(logging.WARNING):
        results = dispatch_alert(payload, channels=(channel,))

    assert len(opener.requests) == 1
    assert results == (
        AlertDispatchResult(
            channel=channel,
            delivered=False,
            error="HTTP Error 500: server error",
        ),
    )
    assert f"{channel} alert channel failed" in caplog.records[-1].message
    assert "HTTP Error 500: server error" in caplog.records[-1].message


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
    assert payload.failure_class is None
    assert payload.scenario_id is None
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
        "failure_class",
        "scenario_id",
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
