from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DBT_MANIFEST_PATH = _REPO_ROOT / "dbt_stub" / "target" / "manifest.json"


@pytest.fixture
def definitions_exports() -> dict[str, Any]:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")
    if not _DBT_MANIFEST_PATH.exists():
        pytest.skip("dbt manifest is not compiled; run make dbt-compile")

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.jobs.phase0 import dbt_phase0_assets, phase0_readiness_ping

    return {
        "AssetKey": dagster.AssetKey,
        "Definitions": dagster.Definitions,
        "build_definitions": build_definitions,
        "daily_cycle_job": daily_cycle_job,
        "dbt_phase0_assets": dbt_phase0_assets,
        "phase0_readiness_ping": phase0_readiness_ping,
    }


def test_build_definitions_collects_p1a_surface(
    definitions_exports: dict[str, Any],
) -> None:
    build_definitions = definitions_exports["build_definitions"]

    defs = build_definitions()

    assert len(defs.jobs) == 1
    assert len(defs.schedules) == 1
    assert len(defs.sensors) == 1
    assert "gate_policy" in defs.resources
    assert "orchestration_context_stub" in defs.resources


def test_build_definitions_is_loadable(
    definitions_exports: dict[str, Any],
) -> None:
    Definitions = definitions_exports["Definitions"]
    build_definitions = definitions_exports["build_definitions"]

    Definitions.validate_loadable(build_definitions())


def test_daily_cycle_job_selects_phase0_readiness_ping(
    definitions_exports: dict[str, Any],
) -> None:
    AssetKey = definitions_exports["AssetKey"]
    daily_cycle_job = definitions_exports["daily_cycle_job"]
    dbt_phase0_assets = definitions_exports["dbt_phase0_assets"]
    phase0_readiness_ping = definitions_exports["phase0_readiness_ping"]

    selected_keys = daily_cycle_job.selection.resolve(
        [phase0_readiness_ping, dbt_phase0_assets],
    )

    assert AssetKey(["phase0_readiness_ping"]) in selected_keys


def test_provider_cannot_override_reserved_resource_keys(
    definitions_exports: dict[str, Any],
) -> None:
    build_definitions = definitions_exports["build_definitions"]

    class DbtOverrideProvider:
        def get_assets(self) -> tuple[object, ...]:
            return ()

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {"dbt": object()}

    with pytest.raises(ValueError, match="duplicate resource key: dbt"):
        build_definitions(providers=[DbtOverrideProvider()])
