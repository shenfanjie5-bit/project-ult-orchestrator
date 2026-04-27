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
    assert "phase0_current_cycle_selection" in status.supported_surfaces
    assert "phase0_data_platform_candidate_freeze_asset" in status.supported_surfaces
    assert "phase0_graph_status_asset" in status.supported_surfaces
    assert "phase1_graph_promotion_asset" in status.supported_surfaces
    assert "phase1_graph_snapshot_asset" in status.supported_surfaces
    assert "phase2_current_cycle_tushare_inputs" in status.supported_surfaces
    assert "phase3_cycle_publish_manifest" in status.supported_surfaces
    assert "audit_eval_formal_audit_replay_persistence" in status.supported_surfaces
    assert "audit_eval_retrospective_hook_asset" in status.supported_surfaces
    assert status.missing_surfaces == ()
    assert "live_gds_zero_skip_proof" in status.runtime_blockers
    assert "configured_graph_phase1_runtime" in status.runtime_blockers
    assert "not_live_gds_zero_skip_proof" in status.non_claims
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


def test_production_daily_cycle_factory_assembles_real_provider_surface(
    dagster_module: object,
    monkeypatch: pytest.MonkeyPatch,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    policy_path = Path(stub_policy_path)
    assert policy_path.is_absolute()

    monkeypatch.setenv("ORCHESTRATOR_POLICY_PATH", str(policy_path))
    monkeypatch.setenv("ORCHESTRATOR_DEFINITIONS_PROFILE", "p5")
    monkeypatch.setenv(
        "ORCHESTRATOR_MODULE_FACTORIES",
        "orchestrator_adapters.production_daily_cycle:production_daily_cycle_provider",
    )
    _clear_orchestrator_definition_imports()

    try:
        definitions = importlib.import_module("orchestrator.definitions")
        defs = definitions.defs
        dagster.Definitions.validate_loadable(defs)
        asset_keys = {
            asset_key
            for asset_def in defs.assets or ()
            for asset_key in getattr(asset_def, "keys", ())
        }
        assert dagster.AssetKey(["candidate_freeze"]) in asset_keys
        assert dagster.AssetKey(["graph_status"]) in asset_keys
        assert dagster.AssetKey(["graph_promotion"]) in asset_keys
        assert dagster.AssetKey(["graph_snapshot"]) in asset_keys
        assert dagster.AssetKey(["l8"]) in asset_keys
        assert dagster.AssetKey(["cycle_publish_manifest"]) in asset_keys
        assert dagster.AssetKey(["retrospective_hook"]) in asset_keys
    finally:
        _clear_orchestrator_definition_imports()


def test_production_daily_cycle_provider_supplies_real_surface_asset_names(
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

    assert dagster.AssetKey(["candidate_freeze"]) in asset_keys
    assert dagster.AssetKey(["graph_status"]) in asset_keys
    assert dagster.AssetKey(["graph_promotion"]) in asset_keys
    assert dagster.AssetKey(["graph_snapshot"]) in asset_keys
    assert dagster.AssetKey(["l8"]) in asset_keys
    assert dagster.AssetKey(["cycle_publish_manifest"]) in asset_keys
    assert dagster.AssetKey(["retrospective_hook"]) in asset_keys


def test_production_daily_cycle_default_graph_runtime_fails_closed(
    dagster_module: object,
) -> None:
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    from orchestrator_adapters.production_daily_cycle import (
        GRAPH_STATUS_PROVIDER_RESOURCE_KEY,
        production_daily_cycle_provider,
    )

    provider = production_daily_cycle_provider()
    resources = provider.get_resources()
    graph_status_provider = resources[GRAPH_STATUS_PROVIDER_RESOURCE_KEY].resource_fn(None)

    with pytest.raises(RuntimeError, match="Graph Phase 0 status runtime"):
        graph_status_provider.get_graph_status(candidate_freeze={}, cycle_id="CYCLE_20260427")


def test_production_candidate_freeze_requires_cycle_tag_before_side_effect() -> None:
    from orchestrator_adapters.production_daily_cycle import _require_cycle_id_from_context

    with pytest.raises(ValueError, match="before any Phase 0 freeze side effect"):
        _require_cycle_id_from_context(SimpleNamespace(tags={}))

    assert (
        _require_cycle_id_from_context(
            SimpleNamespace(tags={"cycle_id": "CYCLE_20260427"}),
        )
        == "CYCLE_20260427"
    )


def test_production_phase2_pool_failure_rate_resource_fails_closed(
    dagster_module: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    from orchestrator.checks import PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY
    from orchestrator_adapters.production_daily_cycle import (
        PHASE2_POOL_FAILURE_RATE_EVENT_ENV,
        production_daily_cycle_provider,
    )

    provider = production_daily_cycle_provider()
    resources = provider.get_resources()
    pool_resource = resources[PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY].resource_fn(None)

    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_EVENT_ENV, raising=False)
    with pytest.raises(RuntimeError, match="runtime is not configured"):
        pool_resource.get_phase2_pool_failure_rate_event()

    monkeypatch.setenv(
        PHASE2_POOL_FAILURE_RATE_EVENT_ENV,
        json.dumps(
            {
                "failed_count": 1,
                "total_count": 4,
                "failed_nodes": ["l6:ENT_STOCK_600519.SH"],
                "reason": "current-cycle metric",
            },
        ),
    )

    event = pool_resource.get_phase2_pool_failure_rate_event()

    assert event.failed_count == 1
    assert event.total_count == 4
    assert event.failed_nodes == ("l6:ENT_STOCK_600519.SH",)
    assert event.reason == "current-cycle metric"


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
