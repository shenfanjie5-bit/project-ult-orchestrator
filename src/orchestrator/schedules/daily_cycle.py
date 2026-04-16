"""Daily cycle schedule definitions."""

from dagster import ScheduleDefinition

from orchestrator.jobs.cycle import daily_cycle_job


_DAILY_CYCLE_CRON = "0 17 * * 1-5"
_DAILY_CYCLE_TIMEZONE = "Asia/Shanghai"

daily_cycle_schedule = ScheduleDefinition(
    job=daily_cycle_job,
    cron_schedule=_DAILY_CYCLE_CRON,
    execution_timezone=_DAILY_CYCLE_TIMEZONE,
    name="daily_cycle_schedule",
)


__all__ = ["daily_cycle_schedule"]
