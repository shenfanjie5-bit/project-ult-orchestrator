"""Gate check classifier and Dagster check exports."""

from orchestrator.checks.classifier import UnknownGateFailure, classify_gate_result
from orchestrator.checks.decision_handler import dispatch_gate_decision_alert
from orchestrator.checks.dbt_events import (
    classify_dbt_test_failure,
    plan_dbt_test_partial_rerun,
)
from orchestrator.checks.models import (
    DataReadinessSignal,
    GateDecision,
    LLMHealthProbe,
    LLMHealthResult,
)
from orchestrator.policy import FailureClass, GateAction, PhaseEnum


def __getattr__(name: str) -> object:
    if name == "GatePolicyResource":
        from orchestrator.checks.resources import GatePolicyResource

        return GatePolicyResource
    if name == "phase0_ping_check":
        from orchestrator.checks.asset_checks import phase0_ping_check

        return phase0_ping_check
    if name == "llm_health_check":
        from orchestrator.checks.asset_checks import llm_health_check

        return llm_health_check
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "FailureClass",
    "GateAction",
    "DataReadinessSignal",
    "GateDecision",
    "GatePolicyResource",
    "LLMHealthProbe",
    "LLMHealthResult",
    "PhaseEnum",
    "UnknownGateFailure",
    "classify_gate_result",
    "classify_dbt_test_failure",
    "dispatch_gate_decision_alert",
    "llm_health_check",
    "phase0_ping_check",
    "plan_dbt_test_partial_rerun",
]
