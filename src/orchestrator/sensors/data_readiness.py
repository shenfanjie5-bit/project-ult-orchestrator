"""Data readiness sensor skeleton."""

from dagster import SkipReason, sensor

from orchestrator.jobs.cycle import daily_cycle_job


@sensor(job=daily_cycle_job, name="data_readiness_sensor")
def data_readiness_sensor() -> SkipReason:
    return SkipReason("readiness wiring deferred to milestone-1")


__all__ = ["data_readiness_sensor"]
