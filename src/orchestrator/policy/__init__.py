"""Policy contract adapter and loader exports."""

from orchestrator.policy.contracts_adapter import (
    CONTRACTS_VERSION,
    FailureClass,
    GateAction,
    PhaseEnum,
)
from orchestrator.policy.loader import load_gate_policy
from orchestrator.policy.schema import (
    REQUIRED_GATE_MATRIX_SCENARIOS,
    GatePolicyProfile,
)

__all__ = [
    "CONTRACTS_VERSION",
    "FailureClass",
    "GatePolicyProfile",
    "GateAction",
    "PhaseEnum",
    "REQUIRED_GATE_MATRIX_SCENARIOS",
    "load_gate_policy",
]
