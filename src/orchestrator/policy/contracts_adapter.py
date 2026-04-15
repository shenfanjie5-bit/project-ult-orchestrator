"""Adapter for gate-related enum types owned by the contracts package."""

# TODO(contracts): replace local Enum with `from contracts.gate import ...` once contracts package is published.

from enum import Enum


CONTRACTS_VERSION: str = "stub-0.1"


class PhaseEnum(Enum):
    """Execution phases exposed by the contracts boundary."""

    PHASE0 = "phase0"
    PHASE1 = "phase1"
    PHASE2 = "phase2"
    PHASE3 = "phase3"


class FailureClass(Enum):
    """Gate failure classes exposed by the contracts boundary."""

    INFRA = "infra"
    DATA_QUALITY = "data_quality"
    TASK_LEVEL = "task_level"
    PUBLISH = "publish"


class GateAction(Enum):
    """Gate actions exposed by the contracts boundary."""

    CONTINUE = "continue"
    FAIL_RUN = "fail_run"
    PARTIAL_RERUN = "partial_rerun"
    MARK_INCONCLUSIVE = "mark_inconclusive"
    REPAIR_MANIFEST = "repair_manifest"


ALL_PHASE_NAMES: tuple[str, ...] = tuple(phase.value for phase in PhaseEnum)
ALL_FAILURE_CLASSES: tuple[str, ...] = tuple(
    failure_class.value for failure_class in FailureClass
)
ALL_ACTIONS: tuple[str, ...] = tuple(action.value for action in GateAction)


__all__ = [
    "ALL_ACTIONS",
    "ALL_FAILURE_CLASSES",
    "ALL_PHASE_NAMES",
    "CONTRACTS_VERSION",
    "FailureClass",
    "GateAction",
    "PhaseEnum",
]
