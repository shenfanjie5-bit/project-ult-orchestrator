from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest


def test_data_platform_provider_contributes_phase0_surface(
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")

    from orchestrator.definitions import build_definitions
    from orchestrator.resources import ResourceBundle

    provider, fake_asset, _fake_check = _fake_data_platform_provider(dagster)
    defs = build_definitions(
        module_factories=[provider],
        policy_path=stub_policy_path,
    )

    dagster.Definitions.validate_loadable(defs)

    assert dagster.AssetKey(["fake_data_platform_phase0_asset"]) in _asset_keys(defs)
    assert "fake_data_platform_phase0_check" in _check_names(defs)
    assert "fake_data_platform_resource" in defs.resources

    bundle = defs.resources["resource_bundle"]
    assert isinstance(bundle, ResourceBundle)
    assert bundle.resource_keys == ("fake_data_platform_resource",)
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


def _fake_data_platform_provider(dagster: Any) -> tuple[object, object, object]:
    class FakeDataPlatformResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> dict[str, str]:
            return {"source": "fake-provider"}

    @dagster.asset(
        name="fake_data_platform_phase0_asset",
        group_name="phase0",
        required_resource_keys={
            "fake_data_platform_resource",
            "resource_bundle",
        },
    )
    def fake_data_platform_phase0_asset(context: object) -> str:
        fake_resource = context.resources.fake_data_platform_resource
        bundle = context.resources.resource_bundle
        assert bundle.read_only is True
        return fake_resource["source"]

    @dagster.asset_check(
        asset=fake_data_platform_phase0_asset,
        name="fake_data_platform_phase0_check",
    )
    def fake_data_platform_phase0_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    class FakeDataPlatformProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (fake_data_platform_phase0_asset,)

        def get_checks(self) -> tuple[object, ...]:
            return (fake_data_platform_phase0_check,)

        def get_resources(self) -> dict[str, object]:
            return {
                "fake_data_platform_resource": cast(
                    object,
                    FakeDataPlatformResource(),
                )
            }

    return (
        FakeDataPlatformProvider(),
        fake_data_platform_phase0_asset,
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
