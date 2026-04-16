from typing import Any

import pytest


@pytest.fixture
def daily_cycle_schedule_definition() -> Any:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.schedules import daily_cycle_schedule

    return daily_cycle_schedule


def test_daily_cycle_schedule_cron(
    daily_cycle_schedule_definition: Any,
) -> None:
    assert daily_cycle_schedule_definition.cron_schedule == "0 17 * * 1-5"


def test_daily_cycle_schedule_timezone(
    daily_cycle_schedule_definition: Any,
) -> None:
    assert daily_cycle_schedule_definition.execution_timezone == "Asia/Shanghai"


def test_daily_cycle_schedule_name(
    daily_cycle_schedule_definition: Any,
) -> None:
    assert daily_cycle_schedule_definition.name == "daily_cycle_schedule"
