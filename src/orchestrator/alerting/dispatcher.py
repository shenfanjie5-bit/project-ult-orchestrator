import dataclasses
import json
import logging
from collections.abc import Iterable

from orchestrator.alerting.payload import AlertPayload

logger = logging.getLogger(__name__)
_LOGGING_CHANNELS = {"logging", "ops"}


def dispatch_alert(
    payload: AlertPayload,
    channels: Iterable[str] = ("logging",),
) -> None:
    for channel in channels:
        if channel in _LOGGING_CHANNELS:
            logger.warning(json.dumps(dataclasses.asdict(payload)))
            continue

        logger.warning("unknown alert channel: %s", channel)
