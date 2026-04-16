"""Daily cycle schedule definitions."""

from dagster import ScheduleDefinition

from orchestrator.jobs.cycle import daily_cycle_job


_DAILY_CYCLE_CRON = "0 17 * * 1-5"
_DAILY_CYCLE_TIMEZONE = "Asia/Shanghai"


def build_daily_cycle_schedule(
    job: object,
    *,
    name: str = "daily_cycle_schedule",
) -> ScheduleDefinition:
    """Build the daily cycle schedule for the selected automatic entrypoint."""

    return ScheduleDefinition(
        job=job,
        cron_schedule=_DAILY_CYCLE_CRON,
        execution_timezone=_DAILY_CYCLE_TIMEZONE,
        name=name,
    )


daily_cycle_schedule = build_daily_cycle_schedule(daily_cycle_job)


__all__ = ["build_daily_cycle_schedule", "daily_cycle_schedule"]
