"""Dagster sensors for orchestrator readiness checks."""

from dagster import SkipReason, sensor

from orchestrator.jobs.cycle import daily_cycle_job


@sensor(job=daily_cycle_job, name="data_readiness_sensor")
def data_readiness_sensor(_context: object) -> SkipReason:
    return SkipReason("P1a stub sensor: no external readiness source configured.")


__all__ = ["data_readiness_sensor"]
