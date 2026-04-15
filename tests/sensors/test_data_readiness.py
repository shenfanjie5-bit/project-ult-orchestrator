from typing import Any

import pytest


@pytest.fixture
def sensor_exports() -> dict[str, Any]:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.sensors import data_readiness_sensor

    return {
        "SkipReason": dagster.SkipReason,
        "data_readiness_sensor": data_readiness_sensor,
    }


def test_data_readiness_sensor_skips_until_milestone_1(
    sensor_exports: dict[str, Any],
) -> None:
    data_readiness_sensor = sensor_exports["data_readiness_sensor"]
    SkipReason = sensor_exports["SkipReason"]

    result = data_readiness_sensor.evaluation_fn()

    assert isinstance(result, SkipReason)
    assert "milestone-1" in result.skip_message
