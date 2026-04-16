import dataclasses
import json
import logging
import os
import urllib.request
from collections.abc import Callable, Iterable

from orchestrator.alerting.payload import AlertPayload

logger = logging.getLogger(__name__)
_LOGGING_CHANNELS = {"logging", "ops"}
_SLACK_WEBHOOK_ENV = "ORCHESTRATOR_SLACK_WEBHOOK_URL"
_ALERT_WEBHOOK_ENV = "ORCHESTRATOR_ALERT_WEBHOOK_URL"
_HTTP_TIMEOUT_SECONDS = 5
_HTTP_OPENER = urllib.request.build_opener()


@dataclasses.dataclass(frozen=True, slots=True)
class AlertDispatchResult:
    channel: str
    delivered: bool
    error: str | None = None


def dispatch_alert(
    payload: AlertPayload,
    channels: Iterable[str] = ("logging",),
) -> tuple[AlertDispatchResult, ...]:
    results: list[AlertDispatchResult] = []

    for channel in channels:
        if channel in _LOGGING_CHANNELS:
            logger.warning(json.dumps(dataclasses.asdict(payload)))
            results.append(AlertDispatchResult(channel=channel, delivered=True))
            continue

        if channel == "slack":
            results.append(
                _dispatch_configured_http_channel(
                    channel=channel,
                    payload=payload,
                    endpoint_env_var=_SLACK_WEBHOOK_ENV,
                    dispatcher=_dispatch_slack,
                ),
            )
            continue

        if channel == "webhook":
            results.append(
                _dispatch_configured_http_channel(
                    channel=channel,
                    payload=payload,
                    endpoint_env_var=_ALERT_WEBHOOK_ENV,
                    dispatcher=_dispatch_webhook,
                ),
            )
            continue

        error = f"unknown alert channel: {channel}"
        logger.warning(error)
        results.append(
            AlertDispatchResult(channel=channel, delivered=False, error=error),
        )

    return tuple(results)


def _dispatch_configured_http_channel(
    *,
    channel: str,
    payload: AlertPayload,
    endpoint_env_var: str,
    dispatcher: Callable[[AlertPayload, str], None],
) -> AlertDispatchResult:
    webhook_url = os.environ.get(endpoint_env_var)
    if not webhook_url or not webhook_url.strip():
        error = f"missing alert endpoint: {endpoint_env_var}"
        logger.warning("%s alert channel failed: %s", channel, error)
        return AlertDispatchResult(channel=channel, delivered=False, error=error)

    try:
        dispatcher(payload, webhook_url)
    except Exception as exc:
        error = str(exc) or exc.__class__.__name__
        logger.warning("%s alert channel failed: %s", channel, error)
        return AlertDispatchResult(channel=channel, delivered=False, error=error)

    return AlertDispatchResult(channel=channel, delivered=True)


def _dispatch_slack(payload: AlertPayload, webhook_url: str) -> None:
    _post_json(
        webhook_url,
        {
            "text": f"{payload.phase} {payload.action}: {payload.summary}",
            "cycle_id": payload.cycle_id,
            "phase": payload.phase,
            "action": payload.action,
            "failure_class": payload.failure_class,
            "summary": payload.summary,
            "runbook_url": payload.runbook_url,
        },
    )


def _dispatch_webhook(payload: AlertPayload, webhook_url: str) -> None:
    _post_json(webhook_url, dataclasses.asdict(payload))


def _post_json(webhook_url: str, payload: dict[str, object]) -> None:
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    response = _HTTP_OPENER.open(request, timeout=_HTTP_TIMEOUT_SECONDS)
    try:
        response.read()
    finally:
        response.close()


__all__ = [
    "AlertDispatchResult",
    "dispatch_alert",
]
