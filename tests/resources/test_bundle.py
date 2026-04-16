import json
import logging
from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timezone
from inspect import signature
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


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
        resources=MappingProxyType({}),
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
    ResourceBundle = resource_exports["ResourceBundle"]
    StubProvider = resource_exports["StubProvider"]
    build_resource_bundle = resource_exports["build_resource_bundle"]
    provider = StubProvider()

    bundle = build_resource_bundle({"config_ref": "lite"}, [provider])

    assert isinstance(provider, AssetFactoryProvider)
    assert isinstance(bundle, ResourceBundle)
    assert bundle.resource_keys == ("orchestration_context_stub",)
    assert set(bundle.resources) == {"orchestration_context_stub"}
    assert bundle.source_modules == ("orchestrator.resources._stub_provider",)
    assert bundle.config_ref == "lite"
    assert bundle.read_only is True


def test_build_resource_bundle_signature_matches_contract(
    resource_exports: dict[str, Any],
) -> None:
    build_resource_bundle = resource_exports["build_resource_bundle"]

    assert list(signature(build_resource_bundle).parameters) == [
        "env_config",
        "module_factories",
    ]


def test_duplicate_resource_key_raises_value_error(
    resource_exports: dict[str, Any],
) -> None:
    StubProvider = resource_exports["StubProvider"]
    build_resource_bundle = resource_exports["build_resource_bundle"]

    with pytest.raises(
        ValueError,
        match="duplicate resource key: orchestration_context_stub",
    ):
        build_resource_bundle(
            {"config_ref": "lite"},
            [StubProvider(), StubProvider()],
        )


def test_read_only_resource_bundle_rejects_field_mutation(
    resource_exports: dict[str, Any],
) -> None:
    ResourceBundle = resource_exports["ResourceBundle"]
    bundle = _read_only_bundle(ResourceBundle)

    assert [field.name for field in fields(ResourceBundle)] == [
        "resource_keys",
        "resources",
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


def test_read_only_resource_bundle_rejects_resource_mapping_mutation(
    resource_exports: dict[str, Any],
) -> None:
    StubProvider = resource_exports["StubProvider"]
    build_resource_bundle = resource_exports["build_resource_bundle"]
    bundle = build_resource_bundle({"config_ref": "lite"}, [StubProvider()])

    with pytest.raises(TypeError):
        bundle.resources["extra"] = object()
    assert bundle.resource_keys == ("orchestration_context_stub",)


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


def test_build_resource_bundle_hard_stops_provider_resource_construction_failure(
    resource_exports: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    from orchestrator.resources import InfrastructureUnavailableError

    build_resource_bundle = resource_exports["build_resource_bundle"]

    class IcebergProvider:
        infrastructure_resource_key = "iceberg_catalog"

        def get_resources(self) -> dict[str, object]:
            raise RuntimeError("fake iceberg catalog unavailable")

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        with pytest.raises(InfrastructureUnavailableError) as error:
            build_resource_bundle(str(LITE_POLICY_PATH), [IcebergProvider()])

    payload = _single_alert_for_resource(caplog.records, "iceberg_catalog")

    assert error.value.event.resource_key == "iceberg_catalog"
    assert error.value.decision.action.value == "fail_run"
    assert payload["phase"] == "phase0"
    assert payload["failure_class"] == "infra"
    assert payload["action"] == "fail_run"
    assert payload["failed_node"] == "iceberg_catalog"
    assert "fake iceberg catalog unavailable" in payload["summary"]


def test_build_resource_bundle_hard_stops_generic_get_resources_failure(
    resource_exports: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    from orchestrator.resources import (
        InfrastructureUnavailableError,
        PROVIDER_RESOURCE_CONSTRUCTION_KEY,
    )

    build_resource_bundle = resource_exports["build_resource_bundle"]

    class BrokenProvider:
        def get_resources(self) -> dict[str, object]:
            raise RuntimeError("provider get_resources exploded")

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        with pytest.raises(InfrastructureUnavailableError) as error:
            build_resource_bundle(str(LITE_POLICY_PATH), [BrokenProvider()])

    payload = _single_alert_for_resource(
        caplog.records,
        PROVIDER_RESOURCE_CONSTRUCTION_KEY,
    )

    assert error.value.event.resource_key == PROVIDER_RESOURCE_CONSTRUCTION_KEY
    assert payload["phase"] == "phase0"
    assert payload["failure_class"] == "infra"
    assert payload["action"] == "fail_run"
    assert payload["failed_node"] == PROVIDER_RESOURCE_CONSTRUCTION_KEY
    assert "provider get_resources exploded" in payload["summary"]


def _single_alert_for_resource(
    records: list[logging.LogRecord],
    resource_key: str,
) -> dict[str, object]:
    payloads = [
        payload
        for payload in _alert_payloads(records)
        if payload.get("failed_node") == resource_key
    ]
    assert len(payloads) == 1
    return payloads[0]


def _alert_payloads(records: list[logging.LogRecord]) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in records:
        try:
            payload = json.loads(record.message)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads
