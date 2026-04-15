"""Dagster schedules for orchestrator jobs."""

from dagster import ScheduleDefinition

from orchestrator.jobs.cycle import daily_cycle_job


daily_cycle_schedule = ScheduleDefinition(
    job=daily_cycle_job,
    cron_schedule="0 9 * * 1-5",
    name="daily_cycle_schedule",
)


__all__ = ["daily_cycle_schedule"]
