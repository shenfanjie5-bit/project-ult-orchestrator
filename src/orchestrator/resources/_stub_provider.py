"""P1a stub provider for the minimal Dagster skeleton."""

from collections.abc import Mapping, Sequence
from typing import cast

from dagster import ConfigurableResource, ResourceDefinition


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
