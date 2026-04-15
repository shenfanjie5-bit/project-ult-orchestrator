"""Daily cycle Dagster job definitions."""

from dagster import AssetSelection, define_asset_job


daily_cycle_job = define_asset_job(
    name="daily_cycle_job",
    selection=AssetSelection.groups("phase0"),
)


__all__ = ["daily_cycle_job"]
