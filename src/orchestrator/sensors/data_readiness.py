"""Data readiness sensor skeleton."""

from dagster import SkipReason, sensor

from orchestrator.jobs.cycle import daily_cycle_job


_READINESS_DEFERRED_MESSAGE = "readiness wiring deferred to milestone-1"


@sensor(job=daily_cycle_job, name="data_readiness_sensor")
def data_readiness_sensor() -> SkipReason:
    return SkipReason(_READINESS_DEFERRED_MESSAGE)


__all__ = ["data_readiness_sensor"]
