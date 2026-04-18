from __future__ import annotations

import json
import logging

import pytest

from tests.integration.conftest import asset_materialization_keys


def test_phase1_snapshot_failure_alerts_and_does_not_advance_ready_graph(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dagster = dagster_module

    from orchestrator.checks.resources import GatePolicyResource
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.jobs.phase0_constants import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GRAPH_STATUS_ASSET_KEY,
        PHASE0_GROUP_NAME,
        PHASE0_READINESS_ASSET_KEY,
    )
    from orchestrator.jobs.phase1 import (
        PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        PHASE1_GROUP_NAME,
    )

    ready_marker_calls: list[str] = []

    @dagster.asset(name=PHASE0_READINESS_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def phase0_readiness_ping() -> str:
        return "ready"

    @dagster.asset(name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def candidate_freeze() -> str:
        return "frozen"

    @dagster.asset(name=PHASE0_GRAPH_STATUS_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def graph_status(candidate_freeze: str) -> str:
        return f"{candidate_freeze}:ready"

    @dagster.asset(
        name=PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
        deps=[
            dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
            dagster.AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
            dagster.AssetKey([PHASE0_GRAPH_STATUS_ASSET_KEY]),
        ],
    )
    def graph_promotion() -> str:
        return "promoted"

    @dagster.asset(
        name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
    )
    def graph_snapshot(graph_promotion: str) -> str:
        raise RuntimeError(f"snapshot writer failed after {graph_promotion}")

    @dagster.asset(name="ready_graph_marker", group_name=PHASE1_GROUP_NAME)
    def ready_graph_marker(graph_snapshot: str) -> str:
        ready_marker_calls.append(graph_snapshot)
        return graph_snapshot

    defs = dagster.Definitions(
        assets=[
            phase0_readiness_ping,
            candidate_freeze,
            graph_status,
            graph_promotion,
            graph_snapshot,
            ready_graph_marker,
        ],
        jobs=[daily_cycle_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )
    dagster.Definitions.validate_loadable(defs)

    cycle_id = "cycle-20260416"
    with caplog.at_level(logging.WARNING):
        result = defs.get_job_def("daily_cycle_job").execute_in_process(
            instance=dagster_instance,
            raise_on_error=False,
            tags={
                "cycle_id": cycle_id,
                "snapshot_id": "snapshot-20260416",
            },
        )

    materialized_keys = asset_materialization_keys(result)
    alert = _alert_payloads(caplog)[-1]

    assert result.success is False
    assert dagster.AssetKey([PHASE1_GRAPH_PROMOTION_ASSET_KEY]) in materialized_keys
    assert dagster.AssetKey([PHASE1_GRAPH_SNAPSHOT_ASSET_KEY]) not in materialized_keys
    assert dagster.AssetKey(["ready_graph_marker"]) not in materialized_keys
    assert ready_marker_calls == []
    assert alert["cycle_id"] == cycle_id
    assert alert["phase"] == "phase1"
    assert alert["failed_node"] == "graph_snapshot"
    assert alert["action"] == "fail_run"
    assert alert["failure_class"] == "publish"
    assert alert["scenario_id"] == "phase1_graph_promotion_snapshot_failed"
    assert "snapshot writer failed after promoted" in str(alert["summary"])


def _alert_payloads(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in caplog.records:
        if record.name != "orchestrator.alerting.dispatcher":
            continue
        payloads.append(json.loads(record.message))
    return payloads
