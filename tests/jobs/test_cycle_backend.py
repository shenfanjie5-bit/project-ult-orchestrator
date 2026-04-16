from __future__ import annotations

from typing import Any

import pytest


@pytest.fixture
def cycle_exports() -> dict[str, Any]:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.jobs.cycle import (
        build_daily_cycle_jobs,
        daily_cycle_job,
        daily_cycle_phase0_job,
    )
    from orchestrator.jobs.phase0_constants import PHASE0_GROUP_NAME
    from orchestrator.jobs.phase1 import PHASE1_GROUP_NAME

    return {
        "build_daily_cycle_jobs": build_daily_cycle_jobs,
        "dagster": dagster,
        "daily_cycle_job": daily_cycle_job,
        "daily_cycle_phase0_job": daily_cycle_phase0_job,
        "PHASE0_GROUP_NAME": PHASE0_GROUP_NAME,
        "PHASE1_GROUP_NAME": PHASE1_GROUP_NAME,
    }


def test_build_daily_cycle_jobs_defaults_to_full_dagster_job(
    cycle_exports: dict[str, Any],
) -> None:
    build_daily_cycle_jobs = cycle_exports["build_daily_cycle_jobs"]
    daily_cycle_job = cycle_exports["daily_cycle_job"]

    assert build_daily_cycle_jobs(None) == (daily_cycle_job,)
    assert build_daily_cycle_jobs({"execution_backend": "dagster_only"}) == (
        daily_cycle_job,
    )
    assert build_daily_cycle_jobs({"enabled_phases": ["phase0"]}) == (
        daily_cycle_job,
    )


def test_build_daily_cycle_jobs_temporal_backend_returns_stable_targets(
    cycle_exports: dict[str, Any],
) -> None:
    build_daily_cycle_jobs = cycle_exports["build_daily_cycle_jobs"]
    daily_cycle_job = cycle_exports["daily_cycle_job"]
    daily_cycle_phase0_job = cycle_exports["daily_cycle_phase0_job"]

    jobs = build_daily_cycle_jobs({"execution_backend": "dagster_plus_temporal"})

    assert jobs == (daily_cycle_phase0_job, daily_cycle_job)
    assert [job.name for job in jobs] == [
        "daily_cycle_phase0_job",
        "daily_cycle_job",
    ]
    assert len({job.name for job in jobs}) == 2


def test_build_daily_cycle_jobs_rejects_unknown_backend(
    cycle_exports: dict[str, Any],
) -> None:
    build_daily_cycle_jobs = cycle_exports["build_daily_cycle_jobs"]

    with pytest.raises(ValueError, match="unsupported execution_backend"):
        build_daily_cycle_jobs({"execution_backend": "temporal"})


def test_daily_cycle_phase0_job_selects_only_phase0_group(
    cycle_exports: dict[str, Any],
) -> None:
    dagster = cycle_exports["dagster"]
    daily_cycle_phase0_job = cycle_exports["daily_cycle_phase0_job"]
    phase0_group_name = cycle_exports["PHASE0_GROUP_NAME"]
    phase1_group_name = cycle_exports["PHASE1_GROUP_NAME"]

    @dagster.asset(name="unit_phase0_asset", group_name=phase0_group_name)
    def unit_phase0_asset() -> str:
        return "ok"

    @dagster.asset(name="unit_phase1_asset", group_name=phase1_group_name)
    def unit_phase1_asset() -> str:
        return "ok"

    selected = daily_cycle_phase0_job.selection.resolve(
        [unit_phase0_asset, unit_phase1_asset],
    )

    assert selected == {dagster.AssetKey(["unit_phase0_asset"])}
