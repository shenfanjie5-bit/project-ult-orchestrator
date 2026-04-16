"""Dagster Definitions assembly entrypoint."""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path

from dagster import AssetKey, ConfigurableResource, Definitions
from dagster_dbt import DbtCliResource

from orchestrator.checks import (
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
from orchestrator.sensors import data_readiness_sensor, manual_rerun_sensor

DEFAULT_POLICY_PATH = "config/policy/gate_policy.lite.yaml"
_RESERVED_RESOURCE_KEYS = ("gate_policy", "dbt", "resource_bundle")
_LLM_HEALTH_PROBE_RESOURCE_KEY = "llm_health_probe"


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


def build_definitions(
    module_factories: Iterable[AssetFactoryProvider] = (),
    policy_path: str | Path | None = None,
) -> Definitions:
    if policy_path is None:
        policy_path = os.environ.get(
            "ORCHESTRATOR_POLICY_PATH",
            DEFAULT_POLICY_PATH,
        )
    module_factory_list = tuple(module_factories)

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
            "resource_bundle": resource_bundle,
            **resource_bundle.resources,
        },
    )


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


defs = build_definitions()


__all__ = ["build_definitions", "defs"]
