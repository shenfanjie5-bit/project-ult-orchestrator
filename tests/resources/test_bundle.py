from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timezone
from typing import Any

import pytest


@pytest.fixture
def resource_exports() -> dict[str, Any]:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.resources import (
        AssetFactoryProvider,
        ResourceBundle,
        build_resource_bundle,
    )
    from orchestrator.resources._stub_provider import StubProvider

    return {
        "AssetFactoryProvider": AssetFactoryProvider,
        "ResourceBundle": ResourceBundle,
        "StubProvider": StubProvider,
        "build_resource_bundle": build_resource_bundle,
    }


def _read_only_bundle(resource_bundle_type: Any) -> Any:
    return resource_bundle_type(
        resource_keys=("orchestration_context_stub",),
        source_modules=("stub",),
        config_ref="lite",
        injected_at=datetime(2026, 4, 16, tzinfo=timezone.utc),
        read_only=True,
    )


def test_dagster_resource_types_are_real_imports() -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    ConfigurableResource = dagster.ConfigurableResource
    ResourceDefinition = dagster.ResourceDefinition

    assert ConfigurableResource.__module__.startswith("dagster")
    assert ResourceDefinition.__module__.startswith("dagster")


def test_stub_provider_injects_bootstrap_resource(
    resource_exports: dict[str, Any],
) -> None:
    AssetFactoryProvider = resource_exports["AssetFactoryProvider"]
    StubProvider = resource_exports["StubProvider"]
    build_resource_bundle = resource_exports["build_resource_bundle"]
    provider = StubProvider()

    resources = build_resource_bundle("lite", [provider])

    assert isinstance(provider, AssetFactoryProvider)
    assert tuple(resources) == ("orchestration_context_stub",)
    assert resources["orchestration_context_stub"].create_resource(None) == {
        "mode": "stub"
    }


def test_duplicate_resource_key_raises_value_error(
    resource_exports: dict[str, Any],
) -> None:
    StubProvider = resource_exports["StubProvider"]
    build_resource_bundle = resource_exports["build_resource_bundle"]

    with pytest.raises(
        ValueError,
        match="duplicate resource key: orchestration_context_stub",
    ):
        build_resource_bundle("lite", [StubProvider(), StubProvider()])


def test_read_only_resource_bundle_rejects_field_mutation(
    resource_exports: dict[str, Any],
) -> None:
    ResourceBundle = resource_exports["ResourceBundle"]
    bundle = _read_only_bundle(ResourceBundle)

    assert [field.name for field in fields(ResourceBundle)] == [
        "resource_keys",
        "source_modules",
        "config_ref",
        "injected_at",
        "read_only",
    ]
    assert replace(bundle, config_ref="lite-replaced").config_ref == "lite-replaced"
    with pytest.raises(FrozenInstanceError):
        bundle.config_ref = "mutated"


def test_read_only_resource_bundle_has_no_dict_mutation_bypass(
    resource_exports: dict[str, Any],
) -> None:
    ResourceBundle = resource_exports["ResourceBundle"]
    bundle = _read_only_bundle(ResourceBundle)

    with pytest.raises(AttributeError):
        bundle.__dict__["config_ref"] = "mutated"
    assert bundle.config_ref == "lite"


def test_read_only_resource_bundle_rejects_metadata_shadowing_bypass(
    resource_exports: dict[str, Any],
) -> None:
    ResourceBundle = resource_exports["ResourceBundle"]
    bundle = _read_only_bundle(ResourceBundle)

    with pytest.raises(FrozenInstanceError):
        bundle.__dataclass_fields__ = {}
    with pytest.raises(FrozenInstanceError):
        bundle.extra = "mutated"
    with pytest.raises(FrozenInstanceError):
        bundle.config_ref = "mutated"
