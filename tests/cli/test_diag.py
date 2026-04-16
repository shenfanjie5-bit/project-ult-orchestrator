from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestrator.diagnostics import (
    RunDiagnosticNotFound,
    RunDiagnosticSummary,
    collect_run_diagnostic,
    run_diagnostic_to_dict,
)


class _FakeInstance:
    def __init__(self, runs: list[object], logs: dict[str, list[object]]) -> None:
        self._runs = runs
        self._logs = logs

    def get_runs(self, filters: object | None = None) -> list[object]:
        return self._runs

    def all_logs(self, run_id: str) -> list[object]:
        return self._logs.get(run_id, [])


class _MetadataText:
    def __init__(self, text: str) -> None:
        self.text = text


def test_collect_run_diagnostic_serializes_gate_metadata_and_rerun_request(
    tmp_path: Path,
) -> None:
    cycle_id = "cycle-20260416"
    run = _fake_run("run-1", cycle_id)
    observation = SimpleNamespace(
        asset_key="heartbeat",
        description="dbt test failure gate decision",
        metadata={
            "phase": "phase0",
            "action": "partial_rerun",
            "failure_class": "task_level",
            "scenario_id": "phase0_dbt_test_failed",
            "failed_node": "heartbeat",
            "reason": _MetadataText("dbt test failed"),
        },
    )
    evaluation = SimpleNamespace(
        check_name="llm_health_check",
        metadata={
            "action": "fail_run",
            "failure_class": "infra",
            "scenario_id": "phase0_llm_health_check_failed",
            "summary": "provider unavailable",
        },
    )
    instance = _FakeInstance(
        [run],
        {
            "run-1": [
                _fake_observation_record("run-1", observation),
                _fake_check_record("run-1", evaluation),
            ],
        },
    )
    _write_request(
        tmp_path / "dbt.json",
        run_id="run-1",
        failed_node="heartbeat",
        rerun_selection=["heartbeat"],
        rerun_mode="asset_only",
        requires_manual_ack=False,
        scenario_id="phase0_dbt_test_failed",
    )
    _write_request(
        tmp_path / "other-run.json",
        run_id="run-other",
        failed_node="heartbeat",
        rerun_selection=["heartbeat"],
        rerun_mode="asset_only",
        requires_manual_ack=False,
    )

    summary = collect_run_diagnostic(
        cycle_id,
        instance=instance,
        request_dir=tmp_path,
    )
    payload = run_diagnostic_to_dict(summary)

    assert set(payload) == {
        "cycle_id",
        "runs",
        "gate_decisions",
        "rerun_plans",
        "generated_at",
    }
    assert payload["cycle_id"] == cycle_id
    assert payload["runs"] == [
        {
            "run_id": "run-1",
            "job_name": "daily_cycle_job",
            "status": "FAILURE",
            "tags": {"cycle_id": cycle_id},
        },
    ]
    assert payload["gate_decisions"] == [
        {
            "run_id": "run-1",
            "phase": "phase0",
            "failure_class": "task_level",
            "action": "partial_rerun",
            "scenario_id": "phase0_dbt_test_failed",
            "failed_node": "heartbeat",
            "summary": "dbt test failed",
            "runbook_url": "docs/RUNBOOK_P5.md#phase0-task_level-partial_rerun",
        },
        {
            "run_id": "run-1",
            "phase": "phase0",
            "failure_class": "infra",
            "action": "fail_run",
            "scenario_id": "phase0_llm_health_check_failed",
            "failed_node": "llm_health_check",
            "summary": "provider unavailable",
            "runbook_url": "docs/RUNBOOK_P5.md#phase0-infra-fail_run",
        },
    ]
    assert payload["rerun_plans"] == [
        {
            "run_id": "run-1",
            "failed_node": "heartbeat",
            "rerun_selection": ["heartbeat"],
            "requires_manual_ack": False,
            "rerun_mode": "asset_only",
            "generated_at": "2026-04-16T00:00:00+00:00",
            "scenario_id": "phase0_dbt_test_failed",
        },
    ]


def test_collect_run_diagnostic_keeps_repair_request_manual_ack(
    tmp_path: Path,
) -> None:
    instance = _FakeInstance([_fake_run("run-1", "cycle-20260416")], {"run-1": []})
    _write_request(
        tmp_path / "repair.json",
        run_id="run-1",
        failed_node="cycle_publish_manifest",
        rerun_selection=["repair_cycle_publish_manifest"],
        rerun_mode="repair_only",
        requires_manual_ack=True,
    )

    payload = run_diagnostic_to_dict(
        collect_run_diagnostic(
            "cycle-20260416",
            instance=instance,
            request_dir=tmp_path,
        ),
    )

    assert payload["rerun_plans"] == [
        {
            "run_id": "run-1",
            "failed_node": "cycle_publish_manifest",
            "rerun_selection": ["repair_cycle_publish_manifest"],
            "requires_manual_ack": True,
            "rerun_mode": "repair_only",
            "generated_at": "2026-04-16T00:00:00+00:00",
            "scenario_id": None,
        },
    ]
    assert "formal_objects_commit" not in payload["rerun_plans"][0]["rerun_selection"]


def test_collect_run_diagnostic_raises_when_cycle_has_no_matching_run() -> None:
    instance = _FakeInstance([_fake_run("run-1", "cycle-other")], {"run-1": []})

    with pytest.raises(RunDiagnosticNotFound, match="cycle-20260416"):
        collect_run_diagnostic("cycle-20260416", instance=instance)


def test_diag_cli_run_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from orchestrator.cli import diag

    summary = RunDiagnosticSummary(
        cycle_id="cycle-20260416",
        runs=(),
        gate_decisions=(),
        rerun_plans=(),
        generated_at="2026-04-16T00:00:00+00:00",
    )
    monkeypatch.setattr(
        diag,
        "collect_run_diagnostic",
        lambda cycle_id, *, request_dir: summary,
    )

    exit_code = diag.main(
        [
            "run",
            "cycle-20260416",
            "--json",
            "--request-dir",
            str(tmp_path),
        ],
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["cycle_id"] == "cycle-20260416"
    assert payload["runs"] == []
    assert payload["gate_decisions"] == []
    assert payload["rerun_plans"] == []
    assert payload["generated_at"] == "2026-04-16T00:00:00+00:00"


def test_diag_cli_no_matching_run_returns_three(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from orchestrator.cli import diag

    def _raise_not_found(cycle_id: str, *, request_dir: object) -> object:
        raise RunDiagnosticNotFound(f"no run for {cycle_id}")

    monkeypatch.setattr(diag, "collect_run_diagnostic", _raise_not_found)

    exit_code = diag.main(["run", "cycle-20260416", "--json"])

    captured = capsys.readouterr()
    assert exit_code == 3
    assert "cycle-20260416" in captured.err


def test_top_level_cli_dispatches_diag(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from orchestrator.cli import diag
    from orchestrator.cli import main as cli_main

    summary = RunDiagnosticSummary(
        cycle_id="cycle-20260416",
        runs=(),
        gate_decisions=(),
        rerun_plans=(),
        generated_at="2026-04-16T00:00:00+00:00",
    )
    monkeypatch.setattr(
        diag,
        "collect_run_diagnostic",
        lambda cycle_id, *, request_dir: summary,
    )

    exit_code = cli_main.main(["diag", "run", "cycle-20260416", "--json"])

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["cycle_id"] == "cycle-20260416"


def _fake_run(run_id: str, cycle_id: str) -> object:
    return SimpleNamespace(
        run_id=run_id,
        job_name="daily_cycle_job",
        status=SimpleNamespace(value="FAILURE"),
        tags={"cycle_id": cycle_id},
    )


def _fake_observation_record(run_id: str, observation: object) -> object:
    return SimpleNamespace(
        run_id=run_id,
        dagster_event=SimpleNamespace(
            event_type_value="ASSET_OBSERVATION",
            event_specific_data=SimpleNamespace(asset_observation=observation),
        ),
        message="fallback observation message",
    )


def _fake_check_record(run_id: str, evaluation: object) -> object:
    return SimpleNamespace(
        run_id=run_id,
        dagster_event=SimpleNamespace(
            event_type_value="ASSET_CHECK_EVALUATION",
            event_specific_data=SimpleNamespace(asset_check_evaluation=evaluation),
        ),
        message="fallback check message",
    )


def _write_request(
    path: Path,
    *,
    run_id: str,
    failed_node: str,
    rerun_selection: list[str],
    rerun_mode: str,
    requires_manual_ack: bool,
    scenario_id: str | None = None,
) -> None:
    payload = {
        "run_id": run_id,
        "failed_node": failed_node,
        "rerun_selection": rerun_selection,
        "requires_manual_ack": requires_manual_ack,
        "rerun_mode": rerun_mode,
        "generated_at": "2026-04-16T00:00:00+00:00",
    }
    if scenario_id is not None:
        payload["scenario_id"] = scenario_id
    path.write_text(json.dumps(payload), encoding="utf-8")
