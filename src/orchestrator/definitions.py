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
from orchestrator.checks.phase2 import (
    PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
    build_phase2_pool_failure_rate_check,
)
from orchestrator.jobs.audit import (
    AUDIT_EVAL_GROUP_NAME,
    RETROSPECTIVE_HOOK_ASSET_KEY,
)
from orchestrator.jobs.cycle import daily_cycle_job
from orchestrator.jobs.phase0 import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
    PHASE0_GROUP_NAME,
    PHASE0_READINESS_ASSET_KEY,
    dbt_phase0_assets,
    phase0_readiness_ping,
)
from orchestrator.jobs.phase1 import (
    PHASE1_GRAPH_PROMOTION_ASSET_KEY,
    PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
    PHASE1_GROUP_NAME,
)
from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME, PHASE2_STAGE_KEYS
from orchestrator.jobs.phase3 import (
    PHASE3_FORMAL_COMMIT_ASSET_KEY,
    PHASE3_GROUP_NAME,
    PHASE3_MANIFEST_ASSET_KEY,
)
from orchestrator.resources import (
    INFRASTRUCTURE_RESOURCE_PHASES,
    AssetFactoryProvider,
    build_resource_bundle,
    guard_infrastructure_resource,
)
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
        "milestone-2",
        "milestone-3",
        "milestone-4",
        "p1b",
        "p1c",
        "p2",
        "p3",
        "p5",
        "p5+",
        "phase0",
        "phase1",
        "phase2",
        "phase3",
    }
)
_PHASE1_SURFACE_PROFILES = frozenset(
    {
        "milestone-2",
        "milestone-3",
        "milestone-4",
        "p2",
        "p3",
        "p5",
        "p5+",
        "phase1",
        "phase2",
        "phase3",
    }
)
_PHASE2_SURFACE_PROFILES = frozenset(
    {
        "milestone-2",
        "milestone-3",
        "milestone-4",
        "p2",
        "p3",
        "p5",
        "p5+",
        "phase2",
        "phase3",
    }
)
_PHASE3_SURFACE_PROFILES = frozenset(
    {
        "milestone-2",
        "milestone-3",
        "milestone-4",
        "p2",
        "p3",
        "p5",
        "p5+",
        "phase3",
    }
)
_AUDIT_EVAL_SURFACE_PROFILES = frozenset(
    {
        "milestone-2",
        "milestone-3",
        "milestone-4",
        "p2",
        "p3",
        "p5",
        "p5+",
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

    provider_assets = _collect_provider_assets(module_factory_list)
    _validate_phase0_provider_assets(provider_assets)
    _validate_phase1_provider_assets(provider_assets)
    if _requires_milestone_surface():
        _validate_milestone_surface(provider_assets, resource_bundle.resources)
    if _requires_phase1_surface():
        _validate_phase1_surface(provider_assets)
    if _requires_phase2_surface():
        _validate_phase2_surface(provider_assets)
    _validate_phase3_provider_assets(
        provider_assets,
        require_surface=_requires_phase3_surface(),
    )
    _validate_audit_eval_provider_assets(
        provider_assets,
        require_surface=_requires_audit_eval_surface(),
    )
    provider_checks = _collect_provider_checks(module_factory_list)
    phase2_builtin_checks = _build_phase2_builtin_checks(
        provider_assets,
        resource_bundle.resources,
    )
    builtin_checks = [
        phase0_ping_check,
        llm_health_check,
        *phase2_builtin_checks,
    ]
    llm_health_probe_resource = resource_bundle.resources.get(
        _LLM_HEALTH_PROBE_RESOURCE_KEY,
        _FailClosedLLMHealthProbeResource(),
    )
    data_readiness_resource = _data_readiness_resource(resource_bundle.resources)
    resources = _guard_infrastructure_resources(
        {
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
        policy_path=str(policy_path),
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
        resources=resources,
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


def _collect_provider_assets(
    module_factories: Iterable[AssetFactoryProvider],
) -> tuple[object, ...]:
    return tuple(
        asset
        for module_factory in module_factories
        for asset in module_factory.get_assets()
    )


def _collect_provider_checks(
    module_factories: Iterable[AssetFactoryProvider],
) -> tuple[object, ...]:
    return tuple(
        check
        for module_factory in module_factories
        for check in module_factory.get_checks()
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


def _validate_phase1_provider_assets(provider_assets: Iterable[object]) -> None:
    graph_promotion_key = AssetKey([PHASE1_GRAPH_PROMOTION_ASSET_KEY])
    graph_snapshot_key = AssetKey([PHASE1_GRAPH_SNAPSHOT_ASSET_KEY])
    required_asset_keys = (graph_promotion_key, graph_snapshot_key)
    required_phase0_dependency_keys = frozenset(
        {
            AssetKey([PHASE0_READINESS_ASSET_KEY]),
            AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
        },
    )
    required_asset_key_names = {
        asset_key: asset_key.path[-1] for asset_key in required_asset_keys
    }
    phase1_asset_defs: dict[AssetKey, object] = {}

    for asset_def in provider_assets:
        keys = tuple(getattr(asset_def, "keys", ()))
        group_names = getattr(asset_def, "group_names_by_key", {})
        for asset_key in keys:
            if group_names.get(asset_key) == PHASE1_GROUP_NAME:
                phase1_asset_defs[asset_key] = asset_def
        for asset_key in required_asset_keys:
            if asset_key not in keys:
                continue
            group_name = group_names.get(asset_key)
            if group_name != PHASE1_GROUP_NAME:
                asset_key_name = required_asset_key_names[asset_key]
                raise ValueError(
                    f"{asset_key_name} asset must declare "
                    f"group_name={PHASE1_GROUP_NAME!r}; got {group_name!r}",
                )

    phase1_asset_keys = frozenset(phase1_asset_defs)
    for asset_key in phase1_asset_defs:
        if _has_required_dependency_ancestry(
            asset_key,
            phase1_asset_defs,
            required_phase0_dependency_keys,
            phase1_asset_keys,
            seen=frozenset(),
        ):
            continue

        if asset_key == graph_snapshot_key:
            raise ValueError(
                "phase1 graph_snapshot asset must depend on graph_promotion "
                "or directly on Phase 0 gate assets before it can be included "
                "in daily_cycle_job. Missing Phase 0 ancestry: "
                f"{_asset_key_names(required_phase0_dependency_keys)}.",
            )
        if asset_key == graph_promotion_key:
            raise ValueError(
                "phase1 graph_promotion asset must depend on Phase 0 gate "
                "assets before it can be included in daily_cycle_job. Missing: "
                f"{_asset_key_names(required_phase0_dependency_keys)}.",
            )
        raise ValueError(
            "phase1 provider asset must depend on Phase 0 gate assets directly "
            "or through another phase1 asset before it can be included in "
            f"daily_cycle_job. Asset: {asset_key.to_user_string()}.",
        )


def _requires_milestone_surface() -> bool:
    forced = os.environ.get(_REQUIRE_MILESTONE_SURFACE_ENV)
    if forced is not None:
        return _is_env_truthy(_REQUIRE_MILESTONE_SURFACE_ENV)
    return _is_milestone_surface_profile()


def _requires_phase1_surface() -> bool:
    return _normalized_definitions_profile() in _PHASE1_SURFACE_PROFILES


def _requires_phase2_surface() -> bool:
    return _normalized_definitions_profile() in _PHASE2_SURFACE_PROFILES


def _requires_phase3_surface() -> bool:
    return _normalized_definitions_profile() in _PHASE3_SURFACE_PROFILES


def _requires_audit_eval_surface() -> bool:
    return _normalized_definitions_profile() in _AUDIT_EVAL_SURFACE_PROFILES


def _is_milestone_surface_profile() -> bool:
    return _normalized_definitions_profile() in _MILESTONE_SURFACE_PROFILES


def _normalized_definitions_profile() -> str:
    profile = os.environ.get(_DEFINITIONS_PROFILE_ENV) or os.environ.get(
        _PROFILE_ENV,
        "",
    )
    return profile.strip().lower().replace("_", "-")


def _is_env_truthy(env_key: str) -> bool:
    return os.environ.get(env_key, "").strip().lower() in _TRUTHY_ENV_VALUES


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


def _validate_phase1_surface(provider_assets: Iterable[object]) -> None:
    required_asset_keys = {
        AssetKey([PHASE1_GRAPH_PROMOTION_ASSET_KEY]): "graph_promotion asset",
        AssetKey([PHASE1_GRAPH_SNAPSHOT_ASSET_KEY]): "graph_snapshot asset",
    }
    phase1_asset_keys = {
        asset_key
        for asset_def in provider_assets
        for asset_key in getattr(asset_def, "keys", ())
        if getattr(asset_def, "group_names_by_key", {}).get(asset_key)
        == PHASE1_GROUP_NAME
    }
    missing = [
        contract_name
        for asset_key, contract_name in required_asset_keys.items()
        if asset_key not in phase1_asset_keys
    ]

    if missing:
        missing_items = ", ".join(missing)
        raise ValueError(
            "phase1 milestone Definitions assembly requires graph provider "
            "assets that satisfy the graph promotion/snapshot contract. "
            f"Missing: {missing_items}.",
        )


def _validate_phase2_surface(provider_assets: Iterable[object]) -> None:
    phase2_asset_keys = _asset_keys_for_group(provider_assets, PHASE2_GROUP_NAME)
    if phase2_asset_keys:
        return

    raise ValueError(
        "phase2 milestone Definitions assembly requires provider assets "
        f"declaring group_name={PHASE2_GROUP_NAME!r}.",
    )


def _validate_phase3_provider_assets(
    provider_assets: Iterable[object],
    *,
    require_surface: bool,
) -> None:
    formal_commit_key = AssetKey([PHASE3_FORMAL_COMMIT_ASSET_KEY])
    manifest_key = AssetKey([PHASE3_MANIFEST_ASSET_KEY])
    required_asset_key_names = {
        formal_commit_key: PHASE3_FORMAL_COMMIT_ASSET_KEY,
        manifest_key: PHASE3_MANIFEST_ASSET_KEY,
    }
    phase3_asset_defs: dict[AssetKey, object] = {}

    for asset_def in provider_assets:
        keys = tuple(getattr(asset_def, "keys", ()))
        group_names = getattr(asset_def, "group_names_by_key", {})
        for asset_key in keys:
            if group_names.get(asset_key) == PHASE3_GROUP_NAME:
                phase3_asset_defs[asset_key] = asset_def
        for asset_key, asset_key_name in required_asset_key_names.items():
            if asset_key not in keys:
                continue
            group_name = group_names.get(asset_key)
            if group_name != PHASE3_GROUP_NAME:
                raise ValueError(
                    f"{asset_key_name} asset must declare "
                    f"group_name={PHASE3_GROUP_NAME!r}; got {group_name!r}",
                )

    if not phase3_asset_defs and not require_surface:
        return

    missing = [
        asset_key_name
        for asset_key, asset_key_name in required_asset_key_names.items()
        if asset_key not in phase3_asset_defs
    ]
    if missing:
        missing_items = ", ".join(missing)
        raise ValueError(
            "phase3 provider assets must include formal commit and publish "
            f"manifest assets. Missing: {missing_items}.",
        )

    if not _has_required_dependency_ancestry(
        manifest_key,
        phase3_asset_defs,
        frozenset({formal_commit_key}),
        frozenset(phase3_asset_defs),
        seen=frozenset(),
    ):
        raise ValueError(
            "phase3 cycle_publish_manifest asset must depend on "
            "formal_objects_commit before it can be included in daily_cycle_job.",
        )

    phase2_boundary_key = _phase2_pool_gate_asset_key(provider_assets)
    if phase2_boundary_key is None:
        raise ValueError(
            "phase3 formal_objects_commit asset requires a Phase 2 boundary "
            "asset before it can be included in daily_cycle_job.",
        )

    if _has_required_dependency_ancestry(
        formal_commit_key,
        phase3_asset_defs,
        frozenset({phase2_boundary_key}),
        frozenset(phase3_asset_defs),
        seen=frozenset(),
    ):
        return

    raise ValueError(
        "phase3 formal_objects_commit asset must depend on the Phase 2 "
        f"boundary asset {phase2_boundary_key.to_user_string()!r} before "
        "cycle_publish_manifest can be included in daily_cycle_job.",
    )


def _validate_audit_eval_provider_assets(
    provider_assets: Iterable[object],
    *,
    require_surface: bool = False,
) -> None:
    manifest_key = AssetKey([PHASE3_MANIFEST_ASSET_KEY])
    retrospective_hook_key = AssetKey([RETROSPECTIVE_HOOK_ASSET_KEY])
    all_provider_asset_defs: dict[AssetKey, object] = {}
    audit_asset_defs: dict[AssetKey, object] = {}

    for asset_def in provider_assets:
        keys = tuple(getattr(asset_def, "keys", ()))
        group_names = getattr(asset_def, "group_names_by_key", {})
        for asset_key in keys:
            all_provider_asset_defs[asset_key] = asset_def
            if group_names.get(asset_key) == AUDIT_EVAL_GROUP_NAME:
                audit_asset_defs[asset_key] = asset_def

    if not audit_asset_defs:
        if require_surface:
            raise ValueError(
                "audit_eval milestone Definitions assembly requires the "
                "retrospective_hook asset. Missing: retrospective_hook asset.",
            )
        return

    if retrospective_hook_key not in audit_asset_defs:
        raise ValueError(
            "audit_eval provider assets must include the retrospective_hook "
            "asset before they can be included in daily_cycle_job.",
        )

    provider_asset_keys = frozenset(all_provider_asset_defs)
    if manifest_key not in provider_asset_keys:
        raise ValueError(
            "audit_eval provider assets require the Phase 3 "
            "cycle_publish_manifest asset before they can be included in "
            "daily_cycle_job.",
        )

    for asset_key in sorted(
        audit_asset_defs,
        key=lambda key: key.to_user_string(),
    ):
        if _has_required_dependency_ancestry(
            asset_key,
            all_provider_asset_defs,
            frozenset({manifest_key}),
            provider_asset_keys,
            seen=frozenset(),
        ):
            continue

        raise ValueError(
            "audit_eval asset "
            f"{asset_key.to_user_string()!r} must depend on "
            "cycle_publish_manifest before it can be included in "
            "daily_cycle_job.",
        )


def _build_phase2_builtin_checks(
    provider_assets: Iterable[object],
    provider_resources: Mapping[str, object],
) -> tuple[object, ...]:
    phase2_asset_key = _phase2_pool_gate_asset_key(provider_assets)
    if phase2_asset_key is None:
        return ()

    if PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY not in provider_resources:
        raise ValueError(
            "phase2 provider assets require a "
            f"{PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY!r} resource so the "
            "production phase2_pool_failure_rate_gate can evaluate before "
            "daily_cycle_job advances downstream.",
        )

    return (build_phase2_pool_failure_rate_check(phase2_asset_key),)


def _phase2_pool_gate_asset_key(
    provider_assets: Iterable[object],
) -> AssetKey | None:
    phase2_asset_keys = _asset_keys_for_group(provider_assets, PHASE2_GROUP_NAME)
    if not phase2_asset_keys:
        return None

    final_stage_key = AssetKey([PHASE2_STAGE_KEYS[-1]])
    if final_stage_key in phase2_asset_keys:
        return final_stage_key

    if len(phase2_asset_keys) == 1:
        return next(iter(phase2_asset_keys))

    raise ValueError(
        "phase2 pool failure rate gate requires the final Phase 2 contract "
        f"asset {final_stage_key.to_user_string()!r}, or exactly one "
        f"group_name={PHASE2_GROUP_NAME!r} asset.",
    )


def _asset_keys_for_group(
    provider_assets: Iterable[object],
    group_name: str,
) -> frozenset[AssetKey]:
    return frozenset(
        asset_key
        for asset_def in provider_assets
        for asset_key in getattr(asset_def, "keys", ())
        if getattr(asset_def, "group_names_by_key", {}).get(asset_key) == group_name
    )


def _asset_dependency_keys(
    asset_def: object,
    asset_key: AssetKey,
) -> frozenset[AssetKey]:
    dependency_keys: set[AssetKey] = set()
    for attribute_name in (
        "asset_deps",
        "dependency_keys_by_key",
        "deps_by_key",
    ):
        dependency_mapping = getattr(asset_def, attribute_name, None)
        if isinstance(dependency_mapping, Mapping):
            dependency_keys.update(dependency_mapping.get(asset_key, ()))

    raw_dependency_keys = getattr(asset_def, "dependency_keys", ())
    if raw_dependency_keys:
        dependency_keys.update(raw_dependency_keys)

    return frozenset(dependency_keys)


def _has_required_dependency_ancestry(
    asset_key: AssetKey,
    phase1_asset_defs: Mapping[AssetKey, object],
    required_dependency_keys: frozenset[AssetKey],
    phase1_asset_keys: frozenset[AssetKey],
    *,
    seen: frozenset[AssetKey],
) -> bool:
    if asset_key in seen:
        return False

    asset_def = phase1_asset_defs[asset_key]
    dependency_keys = _asset_dependency_keys(asset_def, asset_key)
    if required_dependency_keys.issubset(dependency_keys):
        return True

    next_seen = seen | frozenset({asset_key})
    return any(
        _has_required_dependency_ancestry(
            dependency_key,
            phase1_asset_defs,
            required_dependency_keys,
            phase1_asset_keys,
            seen=next_seen,
        )
        for dependency_key in dependency_keys.intersection(phase1_asset_keys)
    )


def _asset_key_names(asset_keys: Iterable[AssetKey]) -> str:
    return ", ".join(sorted(asset_key.to_user_string() for asset_key in asset_keys))


def _data_readiness_resource(resources: Mapping[str, object]) -> object:
    for resource_key in (
        DATA_READINESS_RESOURCE_KEY,
        DATA_READINESS_PROVIDER_RESOURCE_KEY,
    ):
        resource = resources.get(resource_key)
        if resource is not None:
            return resource

    return _FailClosedDataReadinessResource()


def _guard_infrastructure_resources(
    resources: Mapping[str, object],
    policy_path: str,
) -> dict[str, object]:
    guarded_resources = dict(resources)
    for resource_key, phase in INFRASTRUCTURE_RESOURCE_PHASES.items():
        resource = guarded_resources.get(resource_key)
        if resource is None:
            continue
        guarded_resources[resource_key] = guard_infrastructure_resource(
            resource_key,
            resource,
            phase=phase,
            policy_path=policy_path,
        )
    return guarded_resources


defs = build_definitions()


__all__ = ["build_definitions", "defs"]
