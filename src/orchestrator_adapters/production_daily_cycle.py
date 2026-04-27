"""Production daily-cycle provider entrypoint and blocker evidence.

This module is intentionally conservative: it exposes the checked-in real
P2/Phase 3/audit-persistence provider slice that exists today, and records the
missing real provider surfaces that prevent a full production Phase 0-3
``daily_cycle_job`` proof.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from orchestrator_adapters.p2_dry_run import P2DryRunAssetFactoryProvider

PRODUCTION_DAILY_CYCLE_FACTORY: Final[str] = (
    "orchestrator_adapters.production_daily_cycle:production_daily_cycle_provider"
)
BLOCKER_CODE: Final[str] = "ORCH_PRODUCTION_DAILY_CYCLE_PROVIDER_BLOCKED"
SUPPORTED_SURFACES: Final[tuple[str, ...]] = (
    "phase2_current_cycle_tushare_inputs",
    "phase2_main_core_l1_l8",
    "phase3_formal_objects_commit",
    "phase3_cycle_publish_manifest",
    "audit_eval_formal_audit_replay_persistence",
)
MISSING_SURFACES: Final[tuple[str, ...]] = (
    "real_phase0_data_platform_candidate_freeze_asset",
    "real_phase0_graph_status_asset_and_neo4j_consistency_check",
    "real_phase0_data_readiness_resource",
    "real_phase1_graph_promotion_asset",
    "real_phase1_graph_snapshot_asset",
    "real_audit_eval_retrospective_hook_asset",
)
CURRENT_CYCLE_BINDING: Final[str] = "dagster_run_tag:cycle_id"


@dataclass(frozen=True, slots=True)
class ProductionDailyCycleProviderStatus:
    """Evidence-friendly status for the production provider set."""

    blocker_code: str
    factory: str
    blocked: bool
    supported_surfaces: tuple[str, ...]
    missing_surfaces: tuple[str, ...]
    current_cycle_binding: str
    non_claims: tuple[str, ...]


class ProductionDailyCycleProvider:
    """Composite provider for the real production slice currently available."""

    def __init__(
        self,
        p2_provider: P2DryRunAssetFactoryProvider | None = None,
    ) -> None:
        self.p2_provider = p2_provider or P2DryRunAssetFactoryProvider(
            require_cycle_tag=True,
        )

    def get_assets(self) -> tuple[object, ...]:
        return self.p2_provider.get_assets()

    def get_checks(self) -> tuple[object, ...]:
        return self.p2_provider.get_checks()

    def get_resources(self) -> dict[str, object]:
        return self.p2_provider.get_resources()

    def status(self) -> ProductionDailyCycleProviderStatus:
        return production_daily_cycle_status()


def production_daily_cycle_provider() -> ProductionDailyCycleProvider:
    """Factory usable from ``ORCHESTRATOR_MODULE_FACTORIES``.

    The returned provider deliberately does not synthesize Phase 0, Phase 1, or
    audit-eval hook assets. With a production/milestone definitions profile,
    ``orchestrator.definitions`` therefore fails closed until those upstream
    real provider surfaces are checked in.
    """

    return ProductionDailyCycleProvider()


def production_daily_cycle_status() -> ProductionDailyCycleProviderStatus:
    """Return the truthful provider-set status without importing Dagster assets."""

    return ProductionDailyCycleProviderStatus(
        blocker_code=BLOCKER_CODE,
        factory=PRODUCTION_DAILY_CYCLE_FACTORY,
        blocked=True,
        supported_surfaces=SUPPORTED_SURFACES,
        missing_surfaces=MISSING_SURFACES,
        current_cycle_binding=CURRENT_CYCLE_BINDING,
        non_claims=(
            "not_full_production_daily_cycle_proof",
            "not_fake_phase0_phase1_closure",
            "not_fixed_cycle_20260415_replay",
        ),
    )


__all__ = [
    "BLOCKER_CODE",
    "CURRENT_CYCLE_BINDING",
    "MISSING_SURFACES",
    "PRODUCTION_DAILY_CYCLE_FACTORY",
    "SUPPORTED_SURFACES",
    "ProductionDailyCycleProvider",
    "ProductionDailyCycleProviderStatus",
    "production_daily_cycle_provider",
    "production_daily_cycle_status",
]
