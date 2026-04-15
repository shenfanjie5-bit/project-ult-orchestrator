from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timezone

import pytest

from orchestrator.resources import (
    AssetFactoryProvider,
    ResourceBundle,
    build_resource_bundle,
)
from orchestrator.resources._stub_provider import StubProvider


def test_stub_provider_injects_bootstrap_resource() -> None:
    provider = StubProvider()

    resources = build_resource_bundle("lite", [provider])

    assert isinstance(provider, AssetFactoryProvider)
    assert tuple(resources) == ("orchestration_context_stub",)
    assert resources["orchestration_context_stub"].create_resource(None) == {
        "mode": "stub"
    }


def test_duplicate_resource_key_raises_value_error() -> None:
    with pytest.raises(
        ValueError,
        match="duplicate resource key: orchestration_context_stub",
    ):
        build_resource_bundle("lite", [StubProvider(), StubProvider()])


def test_read_only_resource_bundle_rejects_field_mutation() -> None:
    bundle = ResourceBundle(
        resource_keys=("orchestration_context_stub",),
        source_modules=("stub",),
        config_ref="lite",
        injected_at=datetime(2026, 4, 16, tzinfo=timezone.utc),
        read_only=True,
    )

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
