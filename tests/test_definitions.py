from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def definition_exports(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")
    monkeypatch.delenv("ORCHESTRATOR_POLICY_PATH", raising=False)

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.phase0 import phase0_readiness_ping

    return {
        "AssetKey": dagster.AssetKey,
        "Definitions": dagster.Definitions,
        "build_definitions": build_definitions,
        "phase0_readiness_ping": phase0_readiness_ping,
    }


def test_build_definitions_collects_p1a_members(
    definition_exports: dict[str, Any],
) -> None:
    build_definitions = definition_exports["build_definitions"]

    defs = build_definitions()

    assert len(defs.jobs) == 1
    assert len(defs.schedules) == 1
    assert len(defs.sensors) == 1
    assert "gate_policy" in defs.resources
    assert "orchestration_context_stub" in defs.resources


def test_build_definitions_default_policy_env_override(
    definition_exports: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    build_definitions = definition_exports["build_definitions"]
    policy_path = tmp_path / "gate_policy.yaml"
    monkeypatch.setenv("ORCHESTRATOR_POLICY_PATH", str(policy_path))

    defs = build_definitions()

    assert defs.resources["gate_policy"].policy_path == str(policy_path)


def test_daily_cycle_job_selects_phase0_readiness_ping(
    definition_exports: dict[str, Any],
) -> None:
    AssetKey = definition_exports["AssetKey"]
    build_definitions = definition_exports["build_definitions"]
    phase0_readiness_ping = definition_exports["phase0_readiness_ping"]
    asset_key = AssetKey(["phase0_readiness_ping"])

    defs = build_definitions()
    job = defs.jobs[0]

    assert job.name == "daily_cycle_job"
    assert phase0_readiness_ping.group_names_by_key[asset_key] == "phase0"
    assert "phase0" in repr(job.selection)


def test_definitions_are_loadable(definition_exports: dict[str, Any]) -> None:
    Definitions = definition_exports["Definitions"]
    build_definitions = definition_exports["build_definitions"]
    defs = build_definitions()

    Definitions.validate_loadable(defs)
