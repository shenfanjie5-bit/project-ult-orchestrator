"""P1a stub provider for the minimal Dagster skeleton."""

from collections.abc import Mapping, Sequence
from typing import cast

from orchestrator.resources.providers import ResourceDefinition

try:
    from dagster import ConfigurableResource
except ModuleNotFoundError as exc:  # pragma: no cover - exercised when Dagster is absent
    if exc.name != "dagster":
        raise

    class ConfigurableResource(ResourceDefinition):
        """Fallback base used only when Dagster is not installed."""


class OrchestrationContextStubResource(ConfigurableResource):
    """Return a constant context payload for P1a bootstrap."""

    def create_resource(self, context: object) -> dict[str, str]:
        return {"mode": "stub"}


class StubProvider:
    """# only for P1a bootstrap; replaced by data-platform providers in milestone-1"""

    def get_assets(self) -> Sequence[object]:
        return ()

    def get_checks(self) -> Sequence[object]:
        return ()

    def get_resources(self) -> Mapping[str, ResourceDefinition]:
        return {
            "orchestration_context_stub": cast(
                ResourceDefinition,
                OrchestrationContextStubResource(),
            )
        }
