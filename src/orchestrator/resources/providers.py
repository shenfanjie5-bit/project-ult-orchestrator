"""Provider protocols for orchestrator asset factories and resources."""

from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable

from dagster import ResourceDefinition


@runtime_checkable
class PureCheckProvider(Protocol):
    """Provider interface for upstream pure Dagster AssetCheck wrappers."""

    def get_checks(self) -> Sequence[object]:
        """Return pure check wrappers with no orchestrator-side business IO."""
        ...


@runtime_checkable
class AssetFactoryProvider(PureCheckProvider, Protocol):
    """Structural provider interface consumed by orchestrator assembly."""

    def get_assets(self) -> Sequence[object]:
        """Return Dagster assets exposed by the upstream module."""
        ...

    def get_resources(self) -> Mapping[str, ResourceDefinition]:
        """Return Dagster resources keyed by injection name."""
        ...
