from __future__ import annotations

import importlib
import json
import sys
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.integration.conftest import asset_check_evaluations, metadata_value


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
    assert "live_gds_zero_skip_proof" not in status.runtime_blockers
    assert "configured_data_platform_current_cycle_runtime" in status.runtime_blockers
    assert "configured_graph_phase1_runtime" in status.runtime_blockers
    assert "configured_reasoner_runtime" in status.runtime_blockers
    assert "not_live_gds_zero_skip_proof" not in status.non_claims
    assert "not_live_pg_current_cycle_freeze_proof" in status.non_claims
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


def test_production_phase2_pool_failure_rate_resource_derives_from_p2_output(
    dagster_module: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    from orchestrator.checks import PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY
    from orchestrator_adapters.production_daily_cycle import (
        PHASE2_POOL_FAILURE_RATE_EVENT_ENV,
        PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV,
        production_daily_cycle_provider,
    )

    provider = production_daily_cycle_provider()
    resources = provider.get_resources()
    pool_resource = resources[PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY].resource_fn(None)

    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_EVENT_ENV, raising=False)
    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV, raising=False)

    event = pool_resource.get_phase2_pool_failure_rate_event(
        current_cycle_p2_output=_current_cycle_p2_output(
            {
                "600519.SH": "ok",
                "000001.SZ": "inconclusive",
                "300750.SZ": "ok",
            },
        ),
    )

    assert event.failed_count == 1
    assert event.total_count == 3
    assert event.failed_nodes == ("l6:000001.SZ",)
    assert event.reason == "derived from current-cycle P2 outputs"


def test_production_phase2_pool_failure_rate_check_uses_l8_output(
    dagster_module: object,
    dagster_instance: object,
    monkeypatch: pytest.MonkeyPatch,
    stub_policy_path: str,
) -> None:
    dagster = dagster_module
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    from orchestrator.checks import (
        PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
        build_phase2_pool_failure_rate_check,
    )
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME
    from orchestrator_adapters.production_daily_cycle import (
        PHASE2_POOL_FAILURE_RATE_EVENT_ENV,
        PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV,
        production_daily_cycle_provider,
    )

    provider = production_daily_cycle_provider()
    pool_resource = provider.get_resources()[PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY].resource_fn(
        None,
    )
    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_EVENT_ENV, raising=False)
    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV, raising=False)

    @dagster.asset(name="l8", group_name=PHASE2_GROUP_NAME)
    def l8() -> object:
        return _current_cycle_p2_output({"600519.SH": "ok", "000001.SZ": "ok"})

    defs = dagster.Definitions(
        assets=[l8],
        asset_checks=[build_phase2_pool_failure_rate_check(dagster.AssetKey(["l8"]))],
        jobs=[dagster.define_asset_job("phase2_pool_metric_job")],
        resources={
            PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: dagster.ResourceDefinition.hardcoded_resource(
                pool_resource,
            ),
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )
    dagster.Definitions.validate_loadable(defs)

    result = defs.get_job_def("phase2_pool_metric_job").execute_in_process(
        instance=dagster_instance,
        tags={"cycle_id": "CYCLE_20260427"},
    )
    evaluation = _single_evaluation(
        asset_check_evaluations(result),
        "phase2_pool_failure_rate_gate",
    )

    assert result.success is True
    assert getattr(evaluation, "passed", None) is True
    assert metadata_value(evaluation, "failure_rate") == 0.0
    assert metadata_value(evaluation, "metric_cycle_id") == "CYCLE_20260427"


def test_production_phase2_pool_failure_rate_check_rejects_stale_l8_cycle(
    dagster_module: object,
    dagster_instance: object,
    monkeypatch: pytest.MonkeyPatch,
    stub_policy_path: str,
) -> None:
    dagster = dagster_module
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    from orchestrator.checks import (
        PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
        build_phase2_pool_failure_rate_check,
    )
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME
    from orchestrator_adapters.production_daily_cycle import (
        PHASE2_POOL_FAILURE_RATE_EVENT_ENV,
        PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV,
        production_daily_cycle_provider,
    )

    provider = production_daily_cycle_provider()
    pool_resource = provider.get_resources()[PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY].resource_fn(
        None,
    )
    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_EVENT_ENV, raising=False)
    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV, raising=False)

    @dagster.asset(name="l8", group_name=PHASE2_GROUP_NAME)
    def l8() -> object:
        return _current_cycle_p2_output({"600519.SH": "ok"})

    defs = dagster.Definitions(
        assets=[l8],
        asset_checks=[build_phase2_pool_failure_rate_check(dagster.AssetKey(["l8"]))],
        jobs=[dagster.define_asset_job("phase2_pool_metric_job")],
        resources={
            PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: dagster.ResourceDefinition.hardcoded_resource(
                pool_resource,
            ),
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )
    dagster.Definitions.validate_loadable(defs)

    with pytest.raises(Exception, match="metric cycle_id must match Dagster run tag"):
        defs.get_job_def("phase2_pool_metric_job").execute_in_process(
            instance=dagster_instance,
            tags={"cycle_id": "CYCLE_20260428"},
        )


def test_production_phase2_pool_failure_rate_resource_reads_metric_artifact(
    dagster_module: object,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    from orchestrator.checks import PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY
    from orchestrator_adapters.production_daily_cycle import (
        PHASE2_POOL_FAILURE_RATE_EVENT_ENV,
        PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV,
        production_daily_cycle_provider,
    )

    provider = production_daily_cycle_provider()
    resources = provider.get_resources()
    pool_resource = resources[PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY].resource_fn(None)
    artifact_path = tmp_path / "phase2_pool_failure_rate_metric.json"
    artifact_path.write_text(
        json.dumps(
            {
                "cycle_id": "CYCLE_20260427",
                "failed_count": 2,
                "total_count": 5,
                "failed_nodes": ["l6:600519.SH", "l6:000001.SZ"],
                "reason": "generated after P2 L8",
            },
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_EVENT_ENV, raising=False)
    monkeypatch.setenv(PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV, str(artifact_path))

    event = pool_resource.get_phase2_pool_failure_rate_event()

    assert event.failed_count == 2
    assert event.total_count == 5
    assert event.failed_nodes == ("l6:600519.SH", "l6:000001.SZ")
    assert event.reason == "persisted P2 metric artifact: generated after P2 L8"
    assert event.cycle_id == "CYCLE_20260427"


def test_production_phase2_pool_failure_rate_artifact_env_rejects_inline_json(
    dagster_module: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    from orchestrator.checks import PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY
    from orchestrator_adapters.production_daily_cycle import (
        PHASE2_POOL_FAILURE_RATE_EVENT_ENV,
        PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV,
        production_daily_cycle_provider,
    )

    provider = production_daily_cycle_provider()
    resources = provider.get_resources()
    pool_resource = resources[PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY].resource_fn(None)

    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_EVENT_ENV, raising=False)
    monkeypatch.setenv(
        PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV,
        json.dumps(
            {
                "cycle_id": "CYCLE_20260427",
                "failed_count": 0,
                "total_count": 2,
                "failed_nodes": [],
            },
        ),
    )

    with pytest.raises(RuntimeError, match="must be a filesystem path"):
        pool_resource.get_phase2_pool_failure_rate_event()


def test_production_phase2_pool_failure_rate_resource_fails_closed(
    dagster_module: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    from orchestrator.checks import PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY
    from orchestrator_adapters.production_daily_cycle import (
        PHASE2_POOL_FAILURE_RATE_EVENT_ENV,
        PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV,
        production_daily_cycle_provider,
    )

    provider = production_daily_cycle_provider()
    resources = provider.get_resources()
    pool_resource = resources[PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY].resource_fn(None)

    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_EVENT_ENV, raising=False)
    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV, raising=False)
    with pytest.raises(RuntimeError, match="has no current-cycle metric"):
        pool_resource.get_phase2_pool_failure_rate_event()


def test_production_phase2_pool_failure_rate_env_json_is_non_production_fallback(
    dagster_module: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    from orchestrator.checks import PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY
    from orchestrator_adapters.production_daily_cycle import (
        PHASE2_POOL_FAILURE_RATE_EVENT_ENV,
        PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV,
        production_daily_cycle_provider,
    )

    provider = production_daily_cycle_provider()
    resources = provider.get_resources()
    pool_resource = resources[PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY].resource_fn(None)

    monkeypatch.delenv(PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV, raising=False)
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
    assert event.reason == "non-production env JSON fallback: current-cycle metric"


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


def _current_cycle_p2_output(status_by_entity: dict[str, str]) -> SimpleNamespace:
    return SimpleNamespace(
        cycle_id="CYCLE_20260427",
        formal_objects={
            "official_alpha_pool": SimpleNamespace(
                selected_entities=tuple(status_by_entity),
            ),
            "alpha_result_snapshot": tuple(
                SimpleNamespace(
                    cycle_id="CYCLE_20260427",
                    entity_id=entity_id,
                    status=status,
                )
                for entity_id, status in status_by_entity.items()
            ),
        },
    )


def _single_evaluation(evaluations: list[object], check_name: str) -> object:
    matches = [
        evaluation
        for evaluation in evaluations
        if _check_name(evaluation) == check_name
    ]
    assert len(matches) == 1
    return matches[0]


def _check_name(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str):
        return check_name
    check_key = getattr(evaluation, "check_key", None)
    return getattr(check_key, "name", None)
