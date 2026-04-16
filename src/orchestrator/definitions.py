"""Dagster Definitions assembly entrypoint."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

from dagster import AssetKey, ConfigurableResource, Definitions
from dagster_dbt import DbtCliResource

from orchestrator.checks import (
    DataReadinessSignal,
    GatePolicyResource,
    llm_health_check,
    phase0_ping_check,
)
from orchestrator.jobs.cycle import daily_cycle_job
from orchestrator.jobs.phase0 import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
    PHASE0_GROUP_NAME,
    dbt_phase0_assets,
    phase0_readiness_ping,
)
from orchestrator.resources import AssetFactoryProvider, build_resource_bundle
from orchestrator.schedules import daily_cycle_schedule
from orchestrator.sensors.data_readiness import (
    DATA_READINESS_PROVIDER_RESOURCE_KEY,
    DATA_READINESS_RESOURCE_KEY,
    data_readiness_sensor,
)
from orchestrator.sensors.manual_rerun import manual_rerun_sensor

DEFAULT_POLICY_PATH = "config/policy/gate_policy.lite.yaml"
_RESERVED_RESOURCE_KEYS = ("gate_policy", "dbt", "resource_bundle")
_LLM_HEALTH_PROBE_RESOURCE_KEY = "llm_health_probe"
_MODULE_FACTORIES_ENV = "ORCHESTRATOR_MODULE_FACTORIES"
_DEFINITIONS_PROFILE_ENV = "ORCHESTRATOR_DEFINITIONS_PROFILE"
_PROFILE_ENV = "ORCHESTRATOR_PROFILE"
_REQUIRE_MILESTONE_SURFACE_ENV = "ORCHESTRATOR_REQUIRE_MILESTONE_SURFACE"
_MILESTONE_SURFACE_PROFILES = frozenset(
    {
        "milestone",
        "milestone-1",
        "p1b",
        "p1c",
        "phase0",
    }
)
_TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes", "on"})


class _MissingLLMHealthResult:
    healthy = False
    summary = "llm_health_probe resource is not configured"
    provider = "missing"


class _MissingLLMHealthProbe:
    provider = "missing"

    def check_health(self) -> _MissingLLMHealthResult:
        return _MissingLLMHealthResult()


class _FailClosedLLMHealthProbeResource(ConfigurableResource):
    """Default probe that keeps the LLM hard-stop gate fail-closed."""

    def create_resource(self, context: object) -> _MissingLLMHealthProbe:
        return _MissingLLMHealthProbe()


class _MissingDataReadinessProvider:
    def get_data_readiness_signal(self) -> DataReadinessSignal:
        return DataReadinessSignal(
            ready=False,
            cycle_id="missing-data-readiness",
            reason="data_readiness resource is not configured",
            failed_node=DATA_READINESS_RESOURCE_KEY,
        )


class _FailClosedDataReadinessResource(ConfigurableResource):
    """Default provider that lets the readiness gate fail through policy."""

    def create_resource(self, context: object) -> _MissingDataReadinessProvider:
        return _MissingDataReadinessProvider()


def build_definitions(
    module_factories: Iterable[AssetFactoryProvider] | None = None,
    policy_path: str | Path | None = None,
) -> Definitions:
    if policy_path is None:
        policy_path = os.environ.get(
            "ORCHESTRATOR_POLICY_PATH",
            DEFAULT_POLICY_PATH,
        )
    module_factory_list = _resolve_module_factories(module_factories)

    resource_bundle = build_resource_bundle(str(policy_path), module_factory_list)
    for reserved in _RESERVED_RESOURCE_KEYS:
        if reserved in resource_bundle.resource_keys:
            raise ValueError(f"duplicate resource key: {reserved}")

    provider_assets = [
        asset
        for module_factory in module_factory_list
        for asset in module_factory.get_assets()
    ]
    _validate_phase0_provider_assets(provider_assets)
    if _requires_milestone_surface():
        _validate_milestone_surface(provider_assets, resource_bundle.resources)
    provider_checks = [
        check
        for module_factory in module_factory_list
        for check in module_factory.get_checks()
    ]
    builtin_checks = [phase0_ping_check, llm_health_check]
    llm_health_probe_resource = resource_bundle.resources.get(
        _LLM_HEALTH_PROBE_RESOURCE_KEY,
        _FailClosedLLMHealthProbeResource(),
    )
    data_readiness_resource = _data_readiness_resource(resource_bundle.resources)

    return Definitions(
        assets=[
            phase0_readiness_ping,
            dbt_phase0_assets,
            *provider_assets,
        ],
        asset_checks=[
            *builtin_checks,
            *provider_checks,
        ],
        jobs=[daily_cycle_job],
        schedules=[daily_cycle_schedule],
        sensors=[data_readiness_sensor, manual_rerun_sensor],
        resources={
            "gate_policy": GatePolicyResource(policy_path=str(policy_path)),
            "dbt": DbtCliResource(
                project_dir=str(DBT_PROJECT_DIR),
                profiles_dir=str(DBT_PROFILES_DIR),
            ),
            _LLM_HEALTH_PROBE_RESOURCE_KEY: llm_health_probe_resource,
            DATA_READINESS_RESOURCE_KEY: data_readiness_resource,
            "resource_bundle": resource_bundle,
            **resource_bundle.resources,
        },
    )


def _resolve_module_factories(
    module_factories: Iterable[AssetFactoryProvider] | None,
) -> tuple[AssetFactoryProvider, ...]:
    if module_factories is not None:
        return tuple(module_factories)

    configured_factories = os.environ.get(_MODULE_FACTORIES_ENV, "")
    if not configured_factories.strip():
        return ()

    return tuple(
        _load_configured_module_factory(factory_path)
        for factory_path in _split_factory_paths(configured_factories)
    )


def _split_factory_paths(configured_factories: str) -> tuple[str, ...]:
    return tuple(
        factory_path.strip()
        for factory_path in configured_factories.replace("\n", ",").split(",")
        if factory_path.strip()
    )


def _load_configured_module_factory(factory_path: str) -> AssetFactoryProvider:
    module_name, separator, attribute_path = factory_path.partition(":")
    if not separator or not module_name or not attribute_path:
        raise RuntimeError(
            f"{_MODULE_FACTORIES_ENV} entries must use 'module:attribute' import "
            f"paths; got {factory_path!r}",
        )

    try:
        module = import_module(module_name)
    except Exception as exc:
        raise RuntimeError(
            f"failed to import module factory {factory_path!r} from "
            f"{_MODULE_FACTORIES_ENV}",
        ) from exc

    try:
        resolved: Any = module
        for attribute_name in attribute_path.split("."):
            resolved = getattr(resolved, attribute_name)
    except AttributeError as exc:
        raise RuntimeError(
            f"module factory {factory_path!r} could not be resolved from "
            f"{_MODULE_FACTORIES_ENV}",
        ) from exc

    if isinstance(resolved, type) or (
        callable(resolved) and not isinstance(resolved, AssetFactoryProvider)
    ):
        resolved = resolved()

    if not isinstance(resolved, AssetFactoryProvider):
        raise RuntimeError(
            f"module factory {factory_path!r} must resolve to an "
            "AssetFactoryProvider or a zero-argument factory returning one",
        )

    return resolved


def _validate_phase0_provider_assets(provider_assets: Iterable[object]) -> None:
    candidate_freeze_key = AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY])
    has_phase0_provider_asset = False
    has_candidate_freeze = False

    for asset_def in provider_assets:
        keys = tuple(getattr(asset_def, "keys", ()))
        group_names = getattr(asset_def, "group_names_by_key", {})
        if any(
            group_names.get(asset_key) == PHASE0_GROUP_NAME for asset_key in keys
        ):
            has_phase0_provider_asset = True

        if candidate_freeze_key not in keys:
            continue

        has_candidate_freeze = True
        group_name = group_names.get(candidate_freeze_key)
        if group_name != PHASE0_GROUP_NAME:
            raise ValueError(
                "candidate_freeze asset must declare "
                f"group_name={PHASE0_GROUP_NAME!r}; got {group_name!r}",
            )

    if has_phase0_provider_asset and not has_candidate_freeze:
        raise ValueError(
            "phase0 provider assets must include candidate_freeze",
        )


def _requires_milestone_surface() -> bool:
    forced = os.environ.get(_REQUIRE_MILESTONE_SURFACE_ENV)
    if forced is not None:
        return forced.strip().lower() in _TRUTHY_ENV_VALUES

    profile = os.environ.get(_DEFINITIONS_PROFILE_ENV) or os.environ.get(
        _PROFILE_ENV,
        "",
    )
    normalized_profile = profile.strip().lower().replace("_", "-")
    return normalized_profile in _MILESTONE_SURFACE_PROFILES


def _validate_milestone_surface(
    provider_assets: Iterable[object],
    provider_resources: Mapping[str, object],
) -> None:
    missing: list[str] = []
    candidate_freeze_key = AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY])
    asset_keys = {
        asset_key
        for asset_def in provider_assets
        for asset_key in getattr(asset_def, "keys", ())
    }
    if candidate_freeze_key not in asset_keys:
        missing.append("candidate_freeze asset")

    if not (
        DATA_READINESS_RESOURCE_KEY in provider_resources
        or DATA_READINESS_PROVIDER_RESOURCE_KEY in provider_resources
    ):
        missing.append("data_readiness or data_readiness_provider resource")

    if _LLM_HEALTH_PROBE_RESOURCE_KEY not in provider_resources:
        missing.append("llm_health_probe resource")

    if missing:
        missing_items = ", ".join(missing)
        raise RuntimeError(
            "milestone Definitions assembly requires upstream data-platform and "
            "reasoner-runtime module factories. Configure "
            f"{_MODULE_FACTORIES_ENV} with 'module:attribute' import paths, or "
            "pass module_factories explicitly to build_definitions(). "
            f"Missing: {missing_items}.",
        )


def _data_readiness_resource(resources: Mapping[str, object]) -> object:
    for resource_key in (
        DATA_READINESS_RESOURCE_KEY,
        DATA_READINESS_PROVIDER_RESOURCE_KEY,
    ):
        resource = resources.get(resource_key)
        if resource is not None:
            return resource

    return _FailClosedDataReadinessResource()


defs = build_definitions()


__all__ = ["build_definitions", "defs"]
