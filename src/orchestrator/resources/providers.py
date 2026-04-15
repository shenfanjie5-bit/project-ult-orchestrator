"""Provider protocols for orchestrator asset factories and resources."""

from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable

try:
    from dagster import ResourceDefinition
except ModuleNotFoundError as exc:  # pragma: no cover - exercised when Dagster is absent
    if exc.name != "dagster":
        raise

    class ResourceDefinition:
        """Fallback marker used only when Dagster is not installed."""


@runtime_checkable
class AssetFactoryProvider(Protocol):
    """Structural provider interface consumed by orchestrator assembly."""

    def get_assets(self) -> Sequence[object]:
        """Return Dagster assets exposed by the upstream module."""
        ...

    def get_checks(self) -> Sequence[object]:
        """Return Dagster checks exposed by the upstream module."""
        ...

    def get_resources(self) -> Mapping[str, ResourceDefinition]:
        """Return Dagster resources keyed by injection name."""
        ...
