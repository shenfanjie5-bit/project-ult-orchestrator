from __future__ import annotations

import importlib
import json
import sys
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest


def test_production_daily_cycle_status_is_truthful_blocker() -> None:
    from orchestrator_adapters.production_daily_cycle import (
        PRODUCTION_DAILY_CYCLE_FACTORY,
        production_daily_cycle_provider,
        production_daily_cycle_status,
    )

    status = production_daily_cycle_status()
    provider = production_daily_cycle_provider()
    status_payload = json.dumps(asdict(status), sort_keys=True)

    assert status.blocked is True
    assert status.factory == PRODUCTION_DAILY_CYCLE_FACTORY
    assert status.current_cycle_binding == "dagster_run_tag:cycle_id"
    assert "phase2_current_cycle_tushare_inputs" in status.supported_surfaces
    assert "phase3_cycle_publish_manifest" in status.supported_surfaces
    assert "audit_eval_formal_audit_replay_persistence" in status.supported_surfaces
    assert "real_phase0_data_platform_candidate_freeze_asset" in status.missing_surfaces
    assert "real_phase1_graph_snapshot_asset" in status.missing_surfaces
    assert "real_audit_eval_retrospective_hook_asset" in status.missing_surfaces
    assert "not_fake_phase0_phase1_closure" in status.non_claims
    assert "CYCLE_20260415" not in status_payload
    assert provider.status() == status
    assert provider.p2_provider.require_cycle_tag is True


def test_production_p2_requires_cycle_id_tag_instead_of_fixed_cycle() -> None:
    from orchestrator_adapters.p2_dry_run import _cycle_id_from_context

    context_without_cycle = SimpleNamespace(tags={})
    context_with_cycle = SimpleNamespace(tags={"cycle_id": "CYCLE_20260427"})

    with pytest.raises(
        ValueError,
        match="requires Dagster run tag 'cycle_id'.*fixed current-cycle fallback",
    ):
        _cycle_id_from_context(context_without_cycle, require_tag=True)

    assert (
        _cycle_id_from_context(context_with_cycle, require_tag=True)
        == "CYCLE_20260427"
    )


def test_production_daily_cycle_factory_fails_closed_without_fake_phase0_phase1(
    dagster_module: object,
    monkeypatch: pytest.MonkeyPatch,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    policy_path = Path(stub_policy_path)
    assert policy_path.is_absolute()

    monkeypatch.setenv("ORCHESTRATOR_POLICY_PATH", str(policy_path))
    monkeypatch.setenv("ORCHESTRATOR_DEFINITIONS_PROFILE", "phase3")
    monkeypatch.setenv(
        "ORCHESTRATOR_MODULE_FACTORIES",
        "orchestrator_adapters.production_daily_cycle:production_daily_cycle_provider",
    )
    _clear_orchestrator_definition_imports()

    try:
        with pytest.raises(
            RuntimeError,
            match=(
                "candidate_freeze asset.*graph_status asset.*"
                "neo4j_graph_consistency_check.*data_readiness"
            ),
        ):
            importlib.import_module("orchestrator.definitions")
    finally:
        _clear_orchestrator_definition_imports()


def test_production_daily_cycle_provider_does_not_supply_fake_phase0_or_phase1(
    dagster_module: object,
) -> None:
    dagster = dagster_module
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    from orchestrator_adapters.production_daily_cycle import (
        production_daily_cycle_provider,
    )

    provider = production_daily_cycle_provider()
    asset_keys = {
        asset_key
        for asset_def in provider.get_assets()
        for asset_key in getattr(asset_def, "keys", ())
    }

    assert dagster.AssetKey(["candidate_freeze"]) not in asset_keys
    assert dagster.AssetKey(["graph_status"]) not in asset_keys
    assert dagster.AssetKey(["graph_promotion"]) not in asset_keys
    assert dagster.AssetKey(["graph_snapshot"]) not in asset_keys
    assert dagster.AssetKey(["l8"]) in asset_keys
    assert dagster.AssetKey(["cycle_publish_manifest"]) in asset_keys


def _clear_orchestrator_definition_imports() -> None:
    for module_name in tuple(sys.modules):
        if module_name == "orchestrator.definitions":
            sys.modules.pop(module_name, None)
        elif module_name == "orchestrator.jobs":
            sys.modules.pop(module_name, None)
        elif module_name.startswith("orchestrator.jobs."):
            sys.modules.pop(module_name, None)
        elif module_name == "orchestrator.schedules":
            sys.modules.pop(module_name, None)
        elif module_name.startswith("orchestrator.schedules."):
            sys.modules.pop(module_name, None)
        elif module_name == "orchestrator.sensors":
            sys.modules.pop(module_name, None)
        elif module_name.startswith("orchestrator.sensors."):
            sys.modules.pop(module_name, None)
