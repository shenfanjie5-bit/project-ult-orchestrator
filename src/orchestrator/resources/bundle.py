"""Resource bundle assembly for Dagster definitions."""

from collections.abc import Iterable, Mapping
from dataclasses import FrozenInstanceError, dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any

from dagster import ResourceDefinition

from orchestrator.resources.infra import guard_provider_resource_construction
from orchestrator.resources.providers import AssetFactoryProvider


@dataclass(slots=True)
class ResourceBundle:
    resource_keys: tuple[str, ...]
    resources: Mapping[str, ResourceDefinition]
    source_modules: tuple[str, ...]
    config_ref: str
    injected_at: datetime
    read_only: bool

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "read_only", False):
            field_names = type(self).__dataclass_fields__
            if name in field_names:
                msg = f"cannot assign to field {name!r}: ResourceBundle is read-only"
            else:
                msg = f"cannot assign attribute {name!r}: ResourceBundle is read-only"
            raise FrozenInstanceError(msg)
        object.__setattr__(self, name, value)


def build_resource_bundle(
    env_config: Mapping[str, Any] | str | None,
    module_factories: Iterable[AssetFactoryProvider],
) -> ResourceBundle:
    module_factory_list = tuple(module_factories)
    resources = _collect_resource_definitions(module_factory_list, env_config)
    return _resource_bundle_from(
        env_config=env_config,
        module_factories=module_factory_list,
        resources=resources,
    )


def _collect_resource_definitions(
    module_factories: Iterable[AssetFactoryProvider],
    env_config: Mapping[str, Any] | str | None,
) -> dict[str, ResourceDefinition]:
    resources: dict[str, ResourceDefinition] = {}

    for module_factory in module_factories:
        provider_resources = guard_provider_resource_construction(
            module_factory,
            env_config=env_config,
        )
        for key, resource in provider_resources.items():
            if key in resources:
                raise ValueError(f"duplicate resource key: {key}")
            resources[key] = resource

    return resources


def _resource_bundle_from(
    env_config: Mapping[str, Any] | str | None,
    module_factories: Iterable[AssetFactoryProvider],
    resources: Mapping[str, ResourceDefinition],
) -> ResourceBundle:
    return ResourceBundle(
        resource_keys=tuple(resources),
        resources=MappingProxyType(dict(resources)),
        source_modules=_source_modules(module_factories),
        config_ref=_config_ref(env_config),
        injected_at=datetime.now(timezone.utc),
        read_only=True,
    )


def _source_modules(
    module_factories: Iterable[AssetFactoryProvider],
) -> tuple[str, ...]:
    modules: dict[str, None] = {}
    for module_factory in module_factories:
        modules[module_factory.__class__.__module__] = None
    return tuple(modules)


def _config_ref(env_config: Mapping[str, Any] | str | None) -> str:
    if env_config is None:
        return "default"
    if isinstance(env_config, str):
        return env_config
    for key in ("config_ref", "environment", "env", "name"):
        if key in env_config:
            return str(env_config[key])
    return "inline-env-config"
