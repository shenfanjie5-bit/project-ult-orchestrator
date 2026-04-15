from orchestrator.policy import CONTRACTS_VERSION, FailureClass, GateAction, PhaseEnum
from orchestrator.policy.contracts_adapter import (
    ALL_ACTIONS,
    ALL_FAILURE_CLASSES,
    ALL_PHASE_NAMES,
)


def test_contracts_version_is_stub() -> None:
    assert CONTRACTS_VERSION == "stub-0.1"


def test_phase_enum_matches_contract_table() -> None:
    assert len(PhaseEnum) == 4
    assert tuple(phase.name for phase in PhaseEnum) == (
        "PHASE0",
        "PHASE1",
        "PHASE2",
        "PHASE3",
    )
    assert ALL_PHASE_NAMES == ("phase0", "phase1", "phase2", "phase3")
    assert tuple(phase.value for phase in PhaseEnum) == ALL_PHASE_NAMES


def test_failure_class_matches_contract_table() -> None:
    assert len(FailureClass) == 4
    assert tuple(failure_class.name for failure_class in FailureClass) == (
        "INFRA",
        "DATA_QUALITY",
        "TASK_LEVEL",
        "PUBLISH",
    )
    assert ALL_FAILURE_CLASSES == (
        "infra",
        "data_quality",
        "task_level",
        "publish",
    )
    assert (
        tuple(failure_class.value for failure_class in FailureClass)
        == ALL_FAILURE_CLASSES
    )


def test_gate_action_matches_contract_table() -> None:
    assert len(GateAction) == 5
    assert tuple(action.name for action in GateAction) == (
        "CONTINUE",
        "FAIL_RUN",
        "PARTIAL_RERUN",
        "MARK_INCONCLUSIVE",
        "REPAIR_MANIFEST",
    )
    assert ALL_ACTIONS == (
        "continue",
        "fail_run",
        "partial_rerun",
        "mark_inconclusive",
        "repair_manifest",
    )
    assert tuple(action.value for action in GateAction) == ALL_ACTIONS


def test_acceptance_values_are_addressable() -> None:
    assert PhaseEnum.PHASE0.value == "phase0"
    assert GateAction.MARK_INCONCLUSIVE.value == "mark_inconclusive"
