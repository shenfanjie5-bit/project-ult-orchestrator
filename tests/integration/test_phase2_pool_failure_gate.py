from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from typing import Any

import pytest

from tests.integration.conftest import (
    asset_check_evaluations,
    asset_materialization_keys,
    metadata_value,
)


def test_phase2_pool_failure_rate_gate_blocks_phase3_and_alerts(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dagster = dagster_module
    defs = _build_defs(
        dagster,
        stub_policy_path=stub_policy_path,
        failed_count=4,
        total_count=10,
        failed_nodes=("phase2_stock_AAPL", "phase2_stock_MSFT"),
    )
    dagster.Definitions.validate_loadable(defs)

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        result = defs.get_job_def("phase2_pool_gate_job").execute_in_process(
            instance=dagster_instance,
            raise_on_error=False,
            tags={"cycle_id": "cycle-20260416"},
        )

    materialized_keys = asset_materialization_keys(result)
    evaluation = _single_evaluation(
        asset_check_evaluations(result),
        "phase2_pool_failure_rate_gate",
    )
    payloads = _alert_payloads(caplog.records)

    assert result.success is False
    assert dagster.AssetKey(["phase2_pool"]) in materialized_keys
    assert dagster.AssetKey(["phase3_publish"]) not in materialized_keys
    assert getattr(evaluation, "passed", None) is False
    assert metadata_value(evaluation, "action") == "fail_run"
    assert metadata_value(evaluation, "failure_rate") == 0.4
    assert payloads == [
        {
            "cycle_id": "cycle-20260416",
            "phase": "phase2",
            "status": "failed",
            "failed_node": "phase2_stock_AAPL, phase2_stock_MSFT",
            "action": "fail_run",
            "summary": "Phase 2 pool failure rate 0.4 (4/10): fake pool failures",
            "failure_class": "data_quality",
            "runbook_url": None,
        },
    ]


def test_phase2_pool_failure_rate_gate_allows_phase3_without_alert(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dagster = dagster_module
    defs = _build_defs(
        dagster,
        stub_policy_path=stub_policy_path,
        failed_count=3,
        total_count=10,
        failed_nodes=("phase2_stock_AAPL",),
    )
    dagster.Definitions.validate_loadable(defs)

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        result = defs.get_job_def("phase2_pool_gate_job").execute_in_process(
            instance=dagster_instance,
            raise_on_error=False,
            tags={"cycle_id": "cycle-20260416"},
        )

    materialized_keys = asset_materialization_keys(result)
    evaluation = _single_evaluation(
        asset_check_evaluations(result),
        "phase2_pool_failure_rate_gate",
    )

    assert result.success is True
    assert dagster.AssetKey(["phase2_pool"]) in materialized_keys
    assert dagster.AssetKey(["phase3_publish"]) in materialized_keys
    assert getattr(evaluation, "passed", None) is True
    assert metadata_value(evaluation, "action") == "continue"
    assert _alert_payloads(caplog.records) == []


def _build_defs(
    dagster: Any,
    *,
    stub_policy_path: str,
    failed_count: int,
    total_count: int,
    failed_nodes: tuple[str, ...],
) -> object:
    from orchestrator.checks import (
        Phase2PoolFailureRateEvent,
        classify_phase2_pool_failure_rate,
        dispatch_phase2_pool_failure_alert,
        phase2_failure_rate,
    )
    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.phase0_constants import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GROUP_NAME,
        PHASE0_READINESS_ASSET_KEY,
    )
    from orchestrator.jobs.phase1 import (
        PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        PHASE1_GROUP_NAME,
    )
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME
    from orchestrator.policy import GateAction

    @dagster.asset(name=PHASE0_READINESS_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def phase0_readiness_ping() -> str:
        return "ready"

    @dagster.asset(
        name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        group_name=PHASE0_GROUP_NAME,
    )
    def candidate_freeze() -> str:
        return "frozen"

    @dagster.asset(
        name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
        deps=[
            dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
            dagster.AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
        ],
    )
    def graph_snapshot() -> str:
        return "snapshot"

    @dagster.asset(
        name="phase2_pool",
        group_name=PHASE2_GROUP_NAME,
        deps=[dagster.AssetKey([PHASE1_GRAPH_SNAPSHOT_ASSET_KEY])],
    )
    def phase2_pool() -> str:
        return "pool placeholder"

    @dagster.asset(
        name="phase3_publish",
        group_name="phase3",
        deps=[dagster.AssetKey(["phase2_pool"])],
    )
    def phase3_publish() -> str:
        return "published"

    @dagster.asset_check(
        asset=phase2_pool,
        name="phase2_pool_failure_rate_gate",
        blocking=True,
    )
    def phase2_pool_failure_rate_gate(
        context: object,
        gate_policy: GatePolicyResource,
    ) -> object:
        event = Phase2PoolFailureRateEvent(
            failed_count=failed_count,
            total_count=total_count,
            failed_nodes=failed_nodes,
            reason="fake pool failures",
        )
        decision = classify_phase2_pool_failure_rate(event, gate_policy.policy)
        dispatch_phase2_pool_failure_alert(
            decision,
            event,
            cycle_id=_cycle_id_from_context(context),
            channels=gate_policy.policy.alert_channels,
        )

        return dagster.AssetCheckResult(
            passed=decision.action is GateAction.CONTINUE,
            metadata={
                "phase": decision.phase.value,
                "failure_class": (
                    decision.failure_class.value if decision.failure_class else ""
                ),
                "action": decision.action.value,
                "failed_count": event.failed_count,
                "total_count": event.total_count,
                "failure_rate": phase2_failure_rate(
                    event.failed_count,
                    event.total_count,
                ),
                "failed_nodes": ", ".join(event.failed_nodes),
            },
        )

    phase2_pool_gate_job = dagster.define_asset_job(
        name="phase2_pool_gate_job",
        selection=dagster.AssetSelection.all(),
    )

    return dagster.Definitions(
        assets=[
            phase0_readiness_ping,
            candidate_freeze,
            graph_snapshot,
            phase2_pool,
            phase3_publish,
        ],
        asset_checks=[phase2_pool_failure_rate_gate],
        jobs=[phase2_pool_gate_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )


def _cycle_id_from_context(context: object) -> str:
    dagster_run = getattr(context, "dagster_run", None)
    if dagster_run is None:
        dagster_run = getattr(context, "run", None)
    tags = getattr(dagster_run, "tags", {}) or {}
    cycle_id = tags.get("cycle_id")
    return cycle_id if isinstance(cycle_id, str) else "unknown"


def _single_evaluation(evaluations: list[object], check_name: str) -> object:
    matches = [
        evaluation
        for evaluation in evaluations
        if _check_name(evaluation) == check_name
    ]
    assert len(matches) == 1
    return matches[0]


def _check_name(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str):
        return check_name
    check_key = getattr(evaluation, "check_key", None)
    name = getattr(check_key, "name", None)
    return name if isinstance(name, str) else None


def _alert_payloads(records: Iterable[logging.LogRecord]) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in records:
        try:
            payload = json.loads(record.message)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads
