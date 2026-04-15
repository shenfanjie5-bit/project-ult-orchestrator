"""Daily cycle schedule definitions."""

from dagster import ScheduleDefinition

from orchestrator.jobs.cycle import daily_cycle_job


daily_cycle_schedule = ScheduleDefinition(
    job=daily_cycle_job,
    cron_schedule="0 17 * * 1-5",
    execution_timezone="Asia/Shanghai",
    name="daily_cycle_schedule",
)


__all__ = ["daily_cycle_schedule"]
