"""Dagster Definitions assembly entrypoint."""

from __future__ import annotations

import os
from collections.abc import Iterable

from dagster import Definitions
from dagster_dbt import DbtCliResource

from orchestrator.checks import GatePolicyResource, phase0_ping_check
from orchestrator.jobs.cycle import daily_cycle_job
from orchestrator.jobs.phase0 import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    dbt_phase0_assets,
    phase0_readiness_ping,
)
from orchestrator.resources import AssetFactoryProvider, build_resource_bundle
from orchestrator.resources._stub_provider import StubProvider
from orchestrator.schedules import daily_cycle_schedule
from orchestrator.sensors import data_readiness_sensor

DEFAULT_POLICY_PATH = "config/policy/gate_policy.lite.yaml"
_RESERVED_RESOURCE_KEYS = ("gate_policy", "dbt")


def build_definitions(
    policy_path: str | None = None,
    providers: Iterable[AssetFactoryProvider] | None = None,
) -> Definitions:
    if policy_path is None:
        policy_path = os.environ.get(
            "ORCHESTRATOR_POLICY_PATH",
            DEFAULT_POLICY_PATH,
        )
    provider_list = tuple(providers) if providers is not None else (StubProvider(),)

    provider_resources = build_resource_bundle(policy_path, provider_list)
    for reserved in _RESERVED_RESOURCE_KEYS:
        if reserved in provider_resources:
            raise ValueError(f"duplicate resource key: {reserved}")

    provider_assets = [
        asset
        for provider in provider_list
        for asset in provider.get_assets()
    ]
    provider_checks = [
        check
        for provider in provider_list
        for check in provider.get_checks()
    ]

    return Definitions(
        assets=[
            phase0_readiness_ping,
            dbt_phase0_assets,
            *provider_assets,
        ],
        asset_checks=[
            phase0_ping_check,
            *provider_checks,
        ],
        jobs=[daily_cycle_job],
        schedules=[daily_cycle_schedule],
        sensors=[data_readiness_sensor],
        resources={
            "gate_policy": GatePolicyResource(policy_path=policy_path),
            "dbt": DbtCliResource(
                project_dir=str(DBT_PROJECT_DIR),
                profiles_dir=str(DBT_PROFILES_DIR),
            ),
            **provider_resources,
        },
    )


defs = build_definitions()


__all__ = ["build_definitions", "defs"]
