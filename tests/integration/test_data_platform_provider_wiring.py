from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest


def test_data_platform_provider_contributes_phase0_surface(
    dagster_module: object,
    dagster_dbt_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.jobs.phase0 import (
        PHASE0_GROUP_NAME,
        dbt_phase0_assets,
        phase0_readiness_ping,
    )
    from orchestrator.resources import ResourceBundle

    provider, fake_asset, _fake_check = _fake_data_platform_provider(dagster)
    defs = build_definitions(
        module_factories=[provider],
        policy_path=stub_policy_path,
    )

    dagster.Definitions.validate_loadable(defs)

    candidate_freeze_key = dagster.AssetKey(["candidate_freeze"])
    selected_keys = daily_cycle_job.selection.resolve(
        [phase0_readiness_ping, dbt_phase0_assets, fake_asset],
    )
    dbt_phase0_keys = set(dbt_phase0_assets.keys)

    assert candidate_freeze_key in _asset_keys(defs)
    assert fake_asset.group_names_by_key[candidate_freeze_key] == PHASE0_GROUP_NAME
    assert dagster.AssetKey(["phase0_readiness_ping"]) in selected_keys
    assert candidate_freeze_key in selected_keys
    assert dbt_phase0_keys
    assert dbt_phase0_keys <= selected_keys
    assert "fake_data_platform_phase0_check" in _check_names(defs)
    assert "fake_data_platform_resource" in defs.resources
    assert "llm_health_probe" in defs.resources

    bundle = defs.resources["resource_bundle"]
    assert isinstance(bundle, ResourceBundle)
    assert bundle.resource_keys == (
        "fake_data_platform_resource",
        "llm_health_probe",
    )
    assert bundle.source_modules == (__name__,)
    assert bundle.config_ref == stub_policy_path
    assert bundle.read_only is True

    result = dagster.materialize(
        [fake_asset],
        resources={
            "fake_data_platform_resource": defs.resources[
                "fake_data_platform_resource"
            ],
            "resource_bundle": defs.resources["resource_bundle"],
        },
        instance=dagster_instance,
    )

    assert result.success is True


def test_candidate_freeze_group_mismatch_is_rejected(
    dagster_module: object,
    dagster_dbt_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    provider, _fake_asset, _fake_check = _fake_data_platform_provider(
        dagster,
        candidate_group="not_phase0",
    )

    with pytest.raises(
        ValueError,
        match=(
            "candidate_freeze asset must declare "
            "group_name='phase0'; got 'not_phase0'"
        ),
    ):
        build_definitions(
            module_factories=[provider],
            policy_path=stub_policy_path,
        )


def _fake_data_platform_provider(
    dagster: Any,
    candidate_group: str = "phase0",
) -> tuple[object, object, object]:
    class FakeDataPlatformResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> dict[str, str]:
            return {"source": "fake-provider"}

    class FakeProviderHealthStatus:
        provider = "fake-llm"
        model = "critical-model"
        reachable = True
        latency_ms = 12.0
        quota_status = "available"
        error = None

    class FakeLLMHealthReport:
        provider_statuses = (FakeProviderHealthStatus(),)
        all_critical_targets_available = True
        summary = "provider ready"

    class FakeLLMHealthProbe:
        def check_health(self) -> FakeLLMHealthReport:
            return FakeLLMHealthReport()

    class FakeLLMHealthProbeResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> FakeLLMHealthProbe:
            return FakeLLMHealthProbe()

    @dagster.asset(
        name="candidate_freeze",
        group_name=candidate_group,
        required_resource_keys={
            "fake_data_platform_resource",
            "resource_bundle",
        },
    )
    def candidate_freeze(context: object) -> str:
        fake_resource = context.resources.fake_data_platform_resource
        bundle = context.resources.resource_bundle
        assert bundle.read_only is True
        return fake_resource["source"]

    @dagster.asset_check(
        asset=candidate_freeze,
        name="fake_data_platform_phase0_check",
    )
    def fake_data_platform_phase0_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    class FakeDataPlatformProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (candidate_freeze,)

        def get_checks(self) -> tuple[object, ...]:
            return (fake_data_platform_phase0_check,)

        def get_resources(self) -> dict[str, object]:
            return {
                "fake_data_platform_resource": cast(
                    object,
                    FakeDataPlatformResource(),
                ),
                "llm_health_probe": cast(
                    object,
                    FakeLLMHealthProbeResource(),
                ),
            }

    return (
        FakeDataPlatformProvider(),
        candidate_freeze,
        fake_data_platform_phase0_check,
    )


def _asset_keys(defs: Any) -> set[object]:
    return {
        asset_key
        for asset_def in defs.assets or ()
        for asset_key in getattr(asset_def, "keys", ())
    }


def _check_names(defs: Any) -> set[str]:
    names: set[str] = set()
    for check_def in defs.asset_checks or ():
        names.update(
            check_key.name
            for check_key in getattr(check_def, "check_keys", ())
        )
        names.update(spec.name for spec in getattr(check_def, "specs", ()))
        if name := getattr(check_def, "name", None):
            names.add(name)
    return names
