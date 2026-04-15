"""Dagster AssetCheck wiring for Phase 0."""

from __future__ import annotations

from dagster import AssetCheckResult, asset_check

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.resources import GatePolicyResource
from orchestrator.policy import GateAction, PhaseEnum


@asset_check(asset="phase0_readiness_ping")
def phase0_ping_check(gate_policy: GatePolicyResource) -> AssetCheckResult:
    """Minimal policy-backed check node for the Phase 0 skeleton."""

    decision = classify_gate_result(PhaseEnum.PHASE0, None, gate_policy.policy)
    return AssetCheckResult(passed=decision.action is GateAction.CONTINUE)


__all__ = ["phase0_ping_check"]
