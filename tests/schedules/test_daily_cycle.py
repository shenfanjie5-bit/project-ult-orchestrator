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


def test_build_daily_cycle_schedule_targets_supplied_job() -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.jobs.cycle import daily_cycle_phase0_job
    from orchestrator.schedules import build_daily_cycle_schedule

    schedule = build_daily_cycle_schedule(
        daily_cycle_phase0_job,
        name="phase0_cycle_schedule",
    )

    assert schedule.name == "phase0_cycle_schedule"
    assert _target_name(schedule) == "daily_cycle_phase0_job"


def test_default_daily_cycle_schedule_targets_full_cycle_job(
    daily_cycle_schedule_definition: Any,
) -> None:
    assert _target_name(daily_cycle_schedule_definition) == "daily_cycle_job"


def _target_name(definition: Any) -> str | None:
    for attribute_name in ("job_name", "target_name"):
        value = getattr(definition, attribute_name, None)
        if isinstance(value, str):
            return value

    for attribute_name in ("job", "job_def", "_job", "_job_def"):
        target = getattr(definition, attribute_name, None)
        name = getattr(target, "name", None)
        if isinstance(name, str):
            return name

    targets = getattr(definition, "targets", None)
    if targets:
        first_target = next(iter(targets))
        for attribute_name in ("job_name", "target_name"):
            value = getattr(first_target, attribute_name, None)
            if isinstance(value, str):
                return value
    return None
