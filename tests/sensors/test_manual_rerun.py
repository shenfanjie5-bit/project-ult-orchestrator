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
    assert not (tmp_path / "request.json").exists()
    assert (tmp_path / ".processed" / "request.json").exists()


def test_processed_manual_rerun_request_is_not_repeated(
    tmp_path: Path,
    sensor_exports: dict[str, Any],
) -> None:
    evaluate_manual_rerun_requests = sensor_exports["evaluate_manual_rerun_requests"]
    SkipReason = sensor_exports["SkipReason"]
    _write_request(tmp_path / "request.json")

    first_result = evaluate_manual_rerun_requests(tmp_path)
    second_result = evaluate_manual_rerun_requests(tmp_path)

    assert first_result.__class__.__name__ == "RunRequest"
    assert isinstance(second_result, SkipReason)
    assert "no manual rerun requests" in second_result.skip_message


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

