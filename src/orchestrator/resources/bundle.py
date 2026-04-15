"""Resource bundle assembly for Dagster definitions."""

from collections.abc import Iterable
from dataclasses import FrozenInstanceError, dataclass
from datetime import datetime

from orchestrator.resources.providers import AssetFactoryProvider, ResourceDefinition


@dataclass
class ResourceBundle:
    resource_keys: tuple[str, ...]
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
        super().__setattr__(name, value)


def build_resource_bundle(
    config_ref: str,
    providers: Iterable[AssetFactoryProvider],
) -> dict[str, ResourceDefinition]:
    resources: dict[str, ResourceDefinition] = {}

    for provider in providers:
        for key, resource in provider.get_resources().items():
            if key in resources:
                raise ValueError(f"duplicate resource key: {key}")
            resources[key] = resource

    return resources
