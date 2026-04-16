from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from orchestrator.diagnostics import collect_run_diagnostic, run_diagnostic_to_dict


class _FakeDagsterLikeInstance:
    def __init__(self, run_record: object, records: list[object]) -> None:
        self._run_record = run_record
        self._records = records

    def get_run_records(self, filters: object | None = None) -> list[object]:
        return [self._run_record]

    def get_records_for_run(self, run_id: str, ascending: bool = True) -> object:
        return SimpleNamespace(records=self._records)


def test_diag_run_collects_event_log_and_manual_request_json(
    tmp_path: Path,
) -> None:
    cycle_id = "cycle-20260416"
    run = SimpleNamespace(
        run_id="run-1",
        job_name="daily_cycle_job",
        status=SimpleNamespace(name="FAILURE"),
        tags={"cycle_id": cycle_id},
    )
    evaluation = SimpleNamespace(
        check_name="phase2_single_stock_tolerance",
        metadata={
            "phase": "phase2",
            "failure_class": "task_level",
            "action": "mark_inconclusive",
            "scenario_id": "phase2_single_stock_task_failed",
            "failed_node": "phase2_llm_score_AAPL",
            "reason": "single-stock failure remains within tolerance",
        },
    )
    event_log_entry = SimpleNamespace(
        run_id="run-1",
        dagster_event=SimpleNamespace(
            event_type_value="ASSET_CHECK_EVALUATION",
            event_specific_data=SimpleNamespace(asset_check_evaluation=evaluation),
        ),
        message="asset check evaluated",
    )
    instance = _FakeDagsterLikeInstance(
        SimpleNamespace(dagster_run=run),
        [SimpleNamespace(event_log_entry=event_log_entry)],
    )
    request_path = tmp_path / "run-1-dbt.json"
    request_path.write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "failed_node": "dbt.phase0.heartbeat",
                "rerun_selection": ["dbt.phase0.heartbeat"],
            "requires_manual_ack": False,
            "rerun_mode": "asset_only",
            "generated_at": "2026-04-16T00:00:00+00:00",
            "scenario_id": "phase0_dbt_test_failed",
        },
        ),
        encoding="utf-8",
    )

    summary = collect_run_diagnostic(
        cycle_id,
        instance=instance,
        request_dir=tmp_path,
    )
    payload = run_diagnostic_to_dict(summary)

    assert payload["cycle_id"] == cycle_id
    assert payload["runs"][0]["run_id"] == "run-1"
    assert payload["gate_decisions"] == [
        {
            "run_id": "run-1",
            "phase": "phase2",
            "failure_class": "task_level",
            "action": "mark_inconclusive",
            "scenario_id": "phase2_single_stock_task_failed",
            "failed_node": "phase2_llm_score_AAPL",
            "summary": "single-stock failure remains within tolerance",
            "runbook_url": "docs/RUNBOOK_P5.md#phase2-task_level-mark_inconclusive",
        },
    ]
    assert payload["rerun_plans"][0]["rerun_mode"] == "asset_only"
    assert payload["rerun_plans"][0]["rerun_selection"] == ["dbt.phase0.heartbeat"]
    assert payload["rerun_plans"][0]["scenario_id"] == "phase0_dbt_test_failed"
