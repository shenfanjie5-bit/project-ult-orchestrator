from __future__ import annotations

import json
from pathlib import Path

from tests.integration.conftest import asset_materialization_keys


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
