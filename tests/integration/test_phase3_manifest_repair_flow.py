from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from tests.integration.conftest import asset_materialization_keys


def test_daily_cycle_manifest_failure_hook_writes_repair_only_request(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME, PHASE2_STAGE_KEYS
    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_GROUP_NAME,
        PHASE3_MANIFEST_ASSET_KEY,
    )
    from orchestrator.sensors.manual_rerun import evaluate_manual_rerun_requests

    request_dir = tmp_path / "rerun_requests"
    repair_node = "repair_cycle_publish_manifest"
    cycle_id = "cycle-20260416"
    calls: list[str] = []
    monkeypatch.setenv("ORCHESTRATOR_RERUN_REQUEST_DIR", str(request_dir))
    monkeypatch.setenv("ORCHESTRATOR_MANIFEST_REPAIR_ASSET_KEY", repair_node)

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
        return f"{graph_promotion}:snapshot"

    @dagster.asset(name=PHASE2_STAGE_KEYS[-1], group_name=PHASE2_GROUP_NAME)
    def phase2_l8(graph_snapshot: str) -> str:
        return f"{graph_snapshot}:l8"

    @dagster.asset(
        name=PHASE3_FORMAL_COMMIT_ASSET_KEY,
        group_name=PHASE3_GROUP_NAME,
    )
    def formal_objects_commit(l8: str) -> str:
        assert l8
        calls.append(PHASE3_FORMAL_COMMIT_ASSET_KEY)
        return "formal-commit-ok"

    @dagster.asset(
        name=PHASE3_MANIFEST_ASSET_KEY,
        group_name=PHASE3_GROUP_NAME,
    )
    def cycle_publish_manifest(formal_objects_commit: str) -> str:
        assert formal_objects_commit == "formal-commit-ok"
        calls.append(PHASE3_MANIFEST_ASSET_KEY)
        raise RuntimeError("fake manifest write failed")

    defs = dagster.Definitions(
        assets=[
            phase0_readiness_ping,
            candidate_freeze,
            graph_status,
            graph_promotion,
            graph_snapshot,
            phase2_l8,
            formal_objects_commit,
            cycle_publish_manifest,
        ],
        jobs=[daily_cycle_job],
        resources={
            "gate_policy": GatePolicyResource(policy_path=stub_policy_path),
        },
    )
    dagster.Definitions.validate_loadable(defs)

    with caplog.at_level(logging.WARNING):
        result = defs.get_job_def("daily_cycle_job").execute_in_process(
            instance=dagster_instance,
            raise_on_error=False,
            tags={"cycle_id": cycle_id},
        )

    materialized_keys = asset_materialization_keys(result)
    request_files = list(request_dir.glob("*.json"))
    alert = _alert_payloads(caplog)[-1]

    assert result.success is False
    assert dagster.AssetKey([PHASE3_FORMAL_COMMIT_ASSET_KEY]) in materialized_keys
    assert dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY]) not in materialized_keys
    assert calls == [
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
    ]
    assert len(request_files) == 1

    request = json.loads(request_files[0].read_text(encoding="utf-8"))
    assert request["run_id"] == result.run_id
    assert request["failed_node"] == PHASE3_MANIFEST_ASSET_KEY
    assert request["rerun_selection"] == [repair_node]
    assert request["rerun_mode"] == "repair_only"
    assert request["requires_manual_ack"] is True
    assert request["scenario_id"] == "phase3_manifest_write_failed"

    assert alert["cycle_id"] == cycle_id
    assert alert["phase"] == "phase3"
    assert alert["failed_node"] == PHASE3_MANIFEST_ASSET_KEY
    assert alert["action"] == "repair_manifest"
    assert alert["failure_class"] == "infra"
    assert alert["scenario_id"] == "phase3_manifest_write_failed"
    assert str(request_files[0]) in str(alert["summary"])
    assert "fake manifest write failed" in str(alert["summary"])

    run_request = evaluate_manual_rerun_requests(request_dir)

    assert isinstance(run_request, dagster.RunRequest)
    assert run_request.job_name == "daily_cycle_job"
    assert run_request.tags == {
        "rerun_of": result.run_id,
        "failed_node": PHASE3_MANIFEST_ASSET_KEY,
        "rerun_mode": "repair_only",
        "scenario_id": "phase3_manifest_write_failed",
    }
    assert _asset_selection_strings(run_request.asset_selection) == [repair_node]


def test_phase3_manifest_failure_emits_repair_only_request_without_rollback(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_path: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.checks import (
        ManifestWriteFailureEvent,
        plan_manifest_repair_rerun,
    )
    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
    )
    from orchestrator.policy import load_gate_policy
    from orchestrator.rerun_request import request_from_plan
    from orchestrator.sensors.manual_rerun import evaluate_manual_rerun_requests

    repair_node = "repair_cycle_publish_manifest"
    phase3_calls: list[str] = []

    @dagster.asset(name=PHASE3_FORMAL_COMMIT_ASSET_KEY)
    def formal_objects_commit() -> str:
        phase3_calls.append(PHASE3_FORMAL_COMMIT_ASSET_KEY)
        return "formal-commit-ok"

    @dagster.asset(name=PHASE3_MANIFEST_ASSET_KEY)
    def cycle_publish_manifest(formal_objects_commit: str) -> str:
        assert formal_objects_commit == "formal-commit-ok"
        phase3_calls.append(PHASE3_MANIFEST_ASSET_KEY)
        raise RuntimeError("fake manifest write failed")

    @dagster.asset(name=repair_node)
    def repair_cycle_publish_manifest() -> str:
        phase3_calls.append(repair_node)
        return "repair-ok"

    failed_result = dagster.materialize(
        [formal_objects_commit, cycle_publish_manifest],
        instance=dagster_instance,
        raise_on_error=False,
    )
    failed_materialized_keys = asset_materialization_keys(failed_result)

    assert failed_result.success is False
    assert (
        dagster.AssetKey([PHASE3_FORMAL_COMMIT_ASSET_KEY])
        in failed_materialized_keys
    )
    assert (
        dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY])
        not in failed_materialized_keys
    )
    assert phase3_calls == [
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
    ]

    event = ManifestWriteFailureEvent(
        repair_node=repair_node,
        reason="fake manifest write failed",
    )
    plan = plan_manifest_repair_rerun(
        "run-phase3-manifest",
        event,
        load_gate_policy(stub_policy_path),
    )
    request_path = tmp_path / "repair.json"
    request_path.write_text(json.dumps(request_from_plan(plan)), encoding="utf-8")

    run_request = evaluate_manual_rerun_requests(tmp_path)

    assert isinstance(run_request, dagster.RunRequest)
    assert run_request.job_name == "daily_cycle_job"
    assert run_request.run_key.startswith(
        "manual-rerun:run-phase3-manifest:cycle_publish_manifest:"
    )
    assert run_request.tags == {
        "rerun_of": "run-phase3-manifest",
        "failed_node": PHASE3_MANIFEST_ASSET_KEY,
        "rerun_mode": "repair_only",
        "scenario_id": "phase3_manifest_write_failed",
    }
    assert _asset_selection_strings(run_request.asset_selection) == [repair_node]
    assert PHASE3_FORMAL_COMMIT_ASSET_KEY not in _asset_selection_strings(
        run_request.asset_selection,
    )

    repair_result = dagster.materialize(
        [repair_cycle_publish_manifest],
        instance=dagster_instance,
    )
    repair_materialized_keys = asset_materialization_keys(repair_result)

    assert repair_result.success is True
    assert dagster.AssetKey([repair_node]) in repair_materialized_keys
    assert phase3_calls == [
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
        repair_node,
    ]


def test_manual_repair_only_request_rejects_formal_commit_selection(
    dagster_module: object,
    tmp_path: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
    )
    from orchestrator.sensors.manual_rerun import evaluate_manual_rerun_requests

    request_path = tmp_path / "bad-repair.json"
    request_path.write_text(
        json.dumps(
            {
                "run_id": "run-phase3-manifest",
                "failed_node": PHASE3_MANIFEST_ASSET_KEY,
                "rerun_selection": [PHASE3_FORMAL_COMMIT_ASSET_KEY],
                "requires_manual_ack": True,
                "rerun_mode": "repair_only",
                "generated_at": "2026-04-16T00:00:00+00:00",
            },
        ),
        encoding="utf-8",
    )

    result = evaluate_manual_rerun_requests(tmp_path)

    assert isinstance(result, dagster.SkipReason)
    assert "formal_objects_commit" in result.skip_message
    assert not request_path.exists()
    assert (tmp_path / ".failed" / "bad-repair.json").exists()


def _asset_selection_strings(asset_selection: object) -> list[str]:
    output: list[str] = []
    for asset_key in asset_selection or ():
        if isinstance(asset_key, str):
            output.append(asset_key)
            continue
        if hasattr(asset_key, "to_user_string"):
            output.append(asset_key.to_user_string())
            continue
        path = getattr(asset_key, "path", None)
        if isinstance(path, list):
            output.append("/".join(path))
            continue
        output.append(str(asset_key))
    return output


def _alert_payloads(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in caplog.records:
        if record.name != "orchestrator.alerting.dispatcher":
            continue
        payloads.append(json.loads(record.message))
    return payloads
