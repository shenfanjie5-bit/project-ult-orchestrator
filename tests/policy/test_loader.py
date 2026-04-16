import logging
from inspect import signature
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from orchestrator.policy import (
    FailureClass,
    GateAction,
    GatePolicyProfile,
    PhaseEnum,
    REQUIRED_GATE_MATRIX_SCENARIOS,
    load_gate_policy,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


def _load_lite_policy_data() -> dict[str, Any]:
    return yaml.safe_load(LITE_POLICY_PATH.read_text(encoding="utf-8"))


def _write_policy(tmp_path: Path, policy_data: dict[str, Any]) -> Path:
    policy_path = tmp_path / "gate_policy.yaml"
    policy_path.write_text(yaml.safe_dump(policy_data), encoding="utf-8")
    return policy_path


def test_load_gate_policy_signature_matches_contract() -> None:
    assert list(signature(load_gate_policy).parameters) == ["policy_path"]


def test_load_gate_policy_lite_success() -> None:
    profile = load_gate_policy(LITE_POLICY_PATH)

    assert isinstance(profile, GatePolicyProfile)
    assert profile.policy_version == "lite-0.1"
    assert profile.execution_backend == "dagster_only"
    assert len(profile.phase_matrix) == 9
    assert profile.thresholds == {
        "phase2_pool_failure_rate": 0.30,
        "phase2_single_stock_tolerance": 0.50,
    }
    assert {
        entry.scenario_id for entry in profile.phase_matrix
    } == set(REQUIRED_GATE_MATRIX_SCENARIOS)
    assert profile.phase_matrix[0].phase is PhaseEnum.PHASE0
    assert profile.phase_matrix[0].failure_class is FailureClass.DATA_QUALITY
    assert profile.phase_matrix[0].action is GateAction.FAIL_RUN
    assert profile.phase_matrix[0].scenario_id == "phase0_data_readiness_delayed"


def test_load_gate_policy_missing_file_raises_file_not_found(
    tmp_path: Path,
) -> None:
    with pytest.raises(FileNotFoundError):
        load_gate_policy(tmp_path / "missing.yaml")


def test_load_gate_policy_missing_required_field_raises_validation_error(
    tmp_path: Path,
) -> None:
    policy_data = _load_lite_policy_data()
    policy_data.pop("execution_backend")

    with pytest.raises(ValidationError):
        load_gate_policy(_write_policy(tmp_path, policy_data))


def test_load_gate_policy_invalid_action_raises_validation_error(
    tmp_path: Path,
) -> None:
    policy_data = _load_lite_policy_data()
    policy_data["phase_matrix"][0]["action"] = "ignore"

    with pytest.raises(ValidationError):
        load_gate_policy(_write_policy(tmp_path, policy_data))


def test_load_gate_policy_invalid_execution_backend_raises_validation_error(
    tmp_path: Path,
) -> None:
    policy_data = _load_lite_policy_data()
    policy_data["execution_backend"] = "temporal"

    with pytest.raises(ValidationError):
        load_gate_policy(_write_policy(tmp_path, policy_data))


def test_load_gate_policy_missing_matrix_scenario_raises_validation_error(
    tmp_path: Path,
) -> None:
    policy_data = _load_lite_policy_data()
    policy_data["phase_matrix"] = [
        entry
        for entry in policy_data["phase_matrix"]
        if entry["scenario_id"] != "phase0_data_readiness_delayed"
    ]

    with pytest.raises(ValidationError, match="phase0_data_readiness_delayed"):
        load_gate_policy(_write_policy(tmp_path, policy_data))


def test_load_gate_policy_duplicate_scenario_id_raises_validation_error(
    tmp_path: Path,
) -> None:
    policy_data = _load_lite_policy_data()
    duplicate_scenario_id = policy_data["phase_matrix"][0]["scenario_id"]
    policy_data["phase_matrix"][1]["scenario_id"] = duplicate_scenario_id

    with pytest.raises(ValidationError, match=duplicate_scenario_id):
        load_gate_policy(_write_policy(tmp_path, policy_data))


@pytest.mark.parametrize(
    "threshold_key",
    ["phase2_pool_failure_rate", "phase2_single_stock_tolerance"],
)
def test_load_gate_policy_missing_required_threshold_raises_validation_error(
    tmp_path: Path,
    threshold_key: str,
) -> None:
    policy_data = _load_lite_policy_data()
    policy_data["thresholds"].pop(threshold_key)

    with pytest.raises(ValidationError, match=f"thresholds.{threshold_key}"):
        load_gate_policy(_write_policy(tmp_path, policy_data))


@pytest.mark.parametrize(
    "threshold_key",
    ["phase2_pool_failure_rate", "phase2_single_stock_tolerance"],
)
@pytest.mark.parametrize("threshold", ["0.5", float("nan"), -0.01, 1.01])
def test_load_gate_policy_invalid_threshold_raises_validation_error(
    tmp_path: Path,
    threshold_key: str,
    threshold: object,
) -> None:
    policy_data = _load_lite_policy_data()
    policy_data["thresholds"][threshold_key] = threshold

    with pytest.raises(ValidationError, match=f"thresholds.{threshold_key}"):
        load_gate_policy(_write_policy(tmp_path, policy_data))


@pytest.mark.parametrize("alert_channels", [[], ["ops", " "]])
def test_load_gate_policy_invalid_alert_channels_raise_validation_error(
    tmp_path: Path,
    alert_channels: list[str],
) -> None:
    policy_data = _load_lite_policy_data()
    policy_data["alert_channels"] = alert_channels

    with pytest.raises(ValidationError, match="alert_channels"):
        load_gate_policy(_write_policy(tmp_path, policy_data))


def test_load_gate_policy_rejects_applies_to_phases_without_entry_phase(
    tmp_path: Path,
) -> None:
    policy_data = _load_lite_policy_data()
    infra_entry = next(
        entry
        for entry in policy_data["phase_matrix"]
        if entry["scenario_id"] == "infra_unavailable_hard_stop"
    )
    infra_entry["applies_to_phases"] = ["phase0", "phase1"]

    with pytest.raises(ValidationError, match="infra_unavailable_hard_stop"):
        load_gate_policy(_write_policy(tmp_path, policy_data))


def test_load_gate_policy_contract_version_mismatch_warns(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    policy_data = _load_lite_policy_data()
    policy_data["contract_version"] = "stub-mismatch"
    caplog.set_level(logging.WARNING, logger="orchestrator.policy.loader")

    profile = load_gate_policy(_write_policy(tmp_path, policy_data))

    assert profile.contract_version == "stub-mismatch"
    assert "does not match contracts adapter stub-0.1" in caplog.text
