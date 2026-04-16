"""Default Phase 1-3 Temporal execution specs."""

from __future__ import annotations

from orchestrator.jobs.phase1 import (
    PHASE1_GRAPH_PROMOTION_ASSET_KEY,
    PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
)
from orchestrator.jobs.phase2 import PHASE2_STAGE_KEYS
from orchestrator.jobs.phase3 import (
    PHASE3_FORMAL_COMMIT_ASSET_KEY,
    PHASE3_MANIFEST_ASSET_KEY,
)
from orchestrator.policy import PhaseEnum
from orchestrator.temporal.models import TemporalPhaseSpec


def default_phase1_3_specs() -> tuple[TemporalPhaseSpec, ...]:
    """Return the canonical Phase 1 -> Phase 2 -> Phase 3 execution specs."""

    return (
        TemporalPhaseSpec(
            phase=PhaseEnum.PHASE1,
            asset_selection=(
                PHASE1_GRAPH_PROMOTION_ASSET_KEY,
                PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
            ),
        ),
        TemporalPhaseSpec(
            phase=PhaseEnum.PHASE2,
            asset_selection=PHASE2_STAGE_KEYS,
        ),
        TemporalPhaseSpec(
            phase=PhaseEnum.PHASE3,
            asset_selection=(
                PHASE3_FORMAL_COMMIT_ASSET_KEY,
                PHASE3_MANIFEST_ASSET_KEY,
            ),
        ),
    )


__all__ = ["default_phase1_3_specs"]
