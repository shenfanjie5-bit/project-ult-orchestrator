import json
from pathlib import Path
from typing import Any

import pytest

from orchestrator.cli.rerun import main


def test_rerun_cli_dry_run_outputs_plan_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(
        [
            "--run-id",
            "r1",
            "--failed-node",
            "dbt.phase0.heartbeat",
            "--dry-run",
        ],
    )

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["run_id"] == "r1"
    assert output["failed_node"] == "dbt.phase0.heartbeat"
    assert output["rerun_selection"] == ["dbt.phase0.heartbeat"]
    assert output["requires_manual_ack"] is False
    assert output["rerun_mode"] == "asset_only"
    assert output["generated_at"]


def test_rerun_cli_writes_request_json(tmp_path: Path) -> None:
    exit_code = main(
        [
            "--run-id",
            "r1",
            "--failed-node",
            "dbt.phase0.heartbeat",
            "--request-dir",
            str(tmp_path),
        ],
    )

    request_path = tmp_path / "r1-dbt.phase0.heartbeat.json"
    assert exit_code == 0
    assert request_path.exists()
    request = json.loads(request_path.read_text(encoding="utf-8"))
    assert request["run_id"] == "r1"
    assert request["failed_node"] == "dbt.phase0.heartbeat"
    assert request["rerun_selection"] == ["dbt.phase0.heartbeat"]
    assert request["requires_manual_ack"] is False
    assert request["rerun_mode"] == "asset_only"
    assert request["generated_at"]


@pytest.fixture
def sensor_exports() -> dict[str, Any]:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.sensors.manual_rerun import evaluate_manual_rerun_requests

    return {
        "RunRequest": dagster.RunRequest,
        "SkipReason": dagster.SkipReason,
        "evaluate_manual_rerun_requests": evaluate_manual_rerun_requests,
    }


def test_manual_rerun_sensor_emits_run_request(
    tmp_path: Path,
    sensor_exports: dict[str, Any],
) -> None:
    evaluate_manual_rerun_requests = sensor_exports["evaluate_manual_rerun_requests"]
    RunRequest = sensor_exports["RunRequest"]
    _write_request(tmp_path / "request.json")

    result = evaluate_manual_rerun_requests(tmp_path)

    assert isinstance(result, RunRequest)
    assert result.job_name == "daily_cycle_job"
    assert _asset_selection_strings(result.asset_selection) == ["phase0_readiness_ping"]
    assert result.tags == {
        "rerun_of": "r1",
        "failed_node": "phase0_readiness_ping",
        "rerun_mode": "asset_only",
    }
    assert result.run_key == (
        "manual-rerun:r1:phase0_readiness_ping:2026-04-16T00:00:00+00:00"
    )
    assert (tmp_path / "request.json").exists()
    assert not (tmp_path / ".processed" / "request.json").exists()


def test_pending_manual_rerun_request_uses_stable_run_key(
    tmp_path: Path,
    sensor_exports: dict[str, Any],
) -> None:
    evaluate_manual_rerun_requests = sensor_exports["evaluate_manual_rerun_requests"]
    _write_request(tmp_path / "request.json")

    first_result = evaluate_manual_rerun_requests(tmp_path)
    second_result = evaluate_manual_rerun_requests(tmp_path)

    assert first_result.__class__.__name__ == "RunRequest"
    assert second_result.__class__.__name__ == "RunRequest"
    assert first_result.run_key == second_result.run_key
    assert (tmp_path / "request.json").exists()


def test_manual_rerun_sensor_invalid_json_returns_skip_reason(
    tmp_path: Path,
    sensor_exports: dict[str, Any],
) -> None:
    evaluate_manual_rerun_requests = sensor_exports["evaluate_manual_rerun_requests"]
    SkipReason = sensor_exports["SkipReason"]
    (tmp_path / "request.json").write_text("{not-json", encoding="utf-8")

    result = evaluate_manual_rerun_requests(tmp_path)

    assert isinstance(result, SkipReason)
    assert "invalid manual rerun request JSON" in result.skip_message
    assert not (tmp_path / "request.json").exists()
    assert (tmp_path / ".failed" / "request.json").exists()
    error = (tmp_path / ".failed" / "request.json.error.txt").read_text(encoding="utf-8")
    assert "invalid manual rerun request JSON" in error


def test_manual_rerun_sensor_unknown_rerun_mode_returns_skip_reason(
    tmp_path: Path,
    sensor_exports: dict[str, Any],
) -> None:
    evaluate_manual_rerun_requests = sensor_exports["evaluate_manual_rerun_requests"]
    SkipReason = sensor_exports["SkipReason"]
    _write_request(tmp_path / "request.json", rerun_mode="whole_cycle")

    result = evaluate_manual_rerun_requests(tmp_path)

    assert isinstance(result, SkipReason)
    assert "unknown rerun_mode" in result.skip_message
    assert not (tmp_path / "request.json").exists()
    assert (tmp_path / ".failed" / "request.json").exists()


def test_manual_rerun_sensor_invalid_request_does_not_starve_valid_later_request(
    tmp_path: Path,
    sensor_exports: dict[str, Any],
) -> None:
    evaluate_manual_rerun_requests = sensor_exports["evaluate_manual_rerun_requests"]
    RunRequest = sensor_exports["RunRequest"]
    (tmp_path / "00-invalid.json").write_text("{not-json", encoding="utf-8")
    _write_request(tmp_path / "01-valid.json")

    result = evaluate_manual_rerun_requests(tmp_path)

    assert isinstance(result, RunRequest)
    assert result.run_key == (
        "manual-rerun:r1:phase0_readiness_ping:2026-04-16T00:00:00+00:00"
    )
    assert not (tmp_path / "00-invalid.json").exists()
    assert (tmp_path / ".failed" / "00-invalid.json").exists()
    assert (tmp_path / "01-valid.json").exists()


def _write_request(path: Path, *, rerun_mode: str = "asset_only") -> None:
    path.write_text(
        json.dumps(
            {
                "run_id": "r1",
                "failed_node": "phase0_readiness_ping",
                "rerun_selection": ["phase0_readiness_ping"],
                "requires_manual_ack": False,
                "rerun_mode": rerun_mode,
                "generated_at": "2026-04-16T00:00:00+00:00",
            },
        ),
        encoding="utf-8",
    )


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
