"""Daily cycle Dagster job definitions."""

from collections.abc import Mapping
from typing import Any

from dagster import AssetSelection, define_asset_job

from orchestrator.checks.phase1 import build_phase1_graph_failure_gate_hook
from orchestrator.jobs.phase0_constants import PHASE0_GROUP_NAME
from orchestrator.jobs.phase1 import PHASE1_GROUP_NAME
from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME
from orchestrator.jobs.phase3 import PHASE3_GROUP_NAME


daily_cycle_job = define_asset_job(
    name="daily_cycle_job",
    selection=AssetSelection.groups(
        PHASE0_GROUP_NAME,
        PHASE1_GROUP_NAME,
        PHASE2_GROUP_NAME,
        PHASE3_GROUP_NAME,
    ),
    hooks={build_phase1_graph_failure_gate_hook()},
)


def build_daily_cycle_jobs(
    phase_config: Mapping[str, Any] | None,
) -> tuple[object, ...]:
    """Build daily cycle jobs from the phase configuration facade."""

    return (daily_cycle_job,)


__all__ = ["build_daily_cycle_jobs", "daily_cycle_job"]
