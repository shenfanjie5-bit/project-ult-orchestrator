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
    HealthCheckReport,
    LLMHealthProbe,
    ProviderHealthStatus,
)
from orchestrator.checks.phase1 import (
    GraphPromotionFailureEvent,
    classify_graph_promotion_failure,
    should_advance_ready_graph,
)
from orchestrator.checks.phase2 import (
    PHASE2_POOL_FAILURE_RATE_CHECK_NAME,
    PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
    Phase2PoolFailureRateEvent,
    Phase2PoolFailureRateProvider,
    Phase2SingleStockFailureEvent,
    build_phase2_pool_failure_rate_check,
    classify_phase2_pool_failure_rate,
    classify_phase2_single_stock_failure,
    dispatch_phase2_pool_failure_alert,
    inconclusive_metadata,
    phase2_failure_rate,
)
from orchestrator.checks.phase3 import (
    FormalCommitFailureEvent,
    ManifestWriteFailureEvent,
    classify_formal_commit_failure,
    classify_manifest_write_failure,
    plan_manifest_repair_rerun,
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
    "FormalCommitFailureEvent",
    "GateAction",
    "DataReadinessSignal",
    "GateDecision",
    "GatePolicyResource",
    "GraphPromotionFailureEvent",
    "HealthCheckReport",
    "LLMHealthProbe",
    "ManifestWriteFailureEvent",
    "PHASE2_POOL_FAILURE_RATE_CHECK_NAME",
    "PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY",
    "Phase2PoolFailureRateEvent",
    "Phase2PoolFailureRateProvider",
    "Phase2SingleStockFailureEvent",
    "ProviderHealthStatus",
    "PhaseEnum",
    "UnknownGateFailure",
    "build_phase2_pool_failure_rate_check",
    "classify_gate_result",
    "classify_dbt_test_failure",
    "classify_formal_commit_failure",
    "classify_graph_promotion_failure",
    "classify_manifest_write_failure",
    "classify_phase2_pool_failure_rate",
    "classify_phase2_single_stock_failure",
    "dispatch_gate_decision_alert",
    "dispatch_phase2_pool_failure_alert",
    "inconclusive_metadata",
    "llm_health_check",
    "phase0_ping_check",
    "phase2_failure_rate",
    "plan_dbt_test_partial_rerun",
    "plan_manifest_repair_rerun",
    "should_advance_ready_graph",
]
