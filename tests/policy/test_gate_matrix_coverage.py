from pathlib import Path

from orchestrator.policy import (
    PhaseEnum,
    REQUIRED_GATE_MATRIX_SCENARIOS,
    load_gate_policy,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


def test_lite_gate_matrix_covers_required_scenarios() -> None:
    profile = load_gate_policy(LITE_POLICY_PATH)

    assert {entry.scenario_id for entry in profile.phase_matrix} == set(
        REQUIRED_GATE_MATRIX_SCENARIOS,
    )


def test_infra_hard_stop_applies_to_all_phases() -> None:
    profile = load_gate_policy(LITE_POLICY_PATH)
    infra_entry = next(
        entry
        for entry in profile.phase_matrix
        if entry.scenario_id == "infra_unavailable_hard_stop"
    )

    assert infra_entry.applies_to_phases == (
        PhaseEnum.PHASE0,
        PhaseEnum.PHASE1,
        PhaseEnum.PHASE2,
        PhaseEnum.PHASE3,
    )
