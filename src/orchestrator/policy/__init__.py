"""Policy contract adapter and loader exports."""

from orchestrator.policy.contracts_adapter import (
    CONTRACTS_VERSION,
    FailureClass,
    GateAction,
    PhaseEnum,
)
from orchestrator.policy.loader import load_gate_policy
from orchestrator.policy.schema import GatePolicyProfile


def __getattr__(name: str) -> object:
    if name == "GatePolicyResource":
        from orchestrator.policy.resource import GatePolicyResource

        return GatePolicyResource
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CONTRACTS_VERSION",
    "FailureClass",
    "GatePolicyResource",
    "GatePolicyProfile",
    "GateAction",
    "PhaseEnum",
    "load_gate_policy",
]
