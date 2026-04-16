"""Daily cycle Dagster job definitions."""

from collections.abc import Mapping
from typing import Any

from dagster import AssetSelection, define_asset_job


daily_cycle_job = define_asset_job(
    name="daily_cycle_job",
    selection=AssetSelection.groups("phase0"),
)


def build_daily_cycle_jobs(
    phase_config: Mapping[str, Any] | None,
) -> tuple[object, ...]:
    """Build P1a daily cycle jobs from the phase configuration facade."""

    return (daily_cycle_job,)


__all__ = ["build_daily_cycle_jobs", "daily_cycle_job"]
