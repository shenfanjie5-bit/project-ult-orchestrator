from typing import Any

import pytest


@pytest.fixture
def sensor_exports() -> dict[str, Any]:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.sensors import data_readiness_sensor

    return {
        "Definitions": dagster.Definitions,
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


def test_data_readiness_sensor_name(sensor_exports: dict[str, Any]) -> None:
    data_readiness_sensor = sensor_exports["data_readiness_sensor"]

    assert data_readiness_sensor.name == "data_readiness_sensor"


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
