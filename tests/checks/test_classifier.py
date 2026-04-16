from __future__ import annotations

from dataclasses import dataclass, fields
from inspect import signature
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from orchestrator.checks import GateDecision, UnknownGateFailure, classify_gate_result
from orchestrator.policy import FailureClass, GateAction, PhaseEnum, load_gate_policy


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


@dataclass(frozen=True, slots=True)
class GateEvent:
    failure_class: FailureClass | str | None


@pytest.fixture
def gate_policy() -> Any:
    return load_gate_policy(LITE_POLICY_PATH)


def _load_lite_policy_data() -> dict[str, Any]:
    return yaml.safe_load(LITE_POLICY_PATH.read_text(encoding="utf-8"))


def _write_policy(tmp_path: Path, policy_data: dict[str, Any]) -> Path:
    policy_path = tmp_path / "gate_policy.yaml"
    policy_path.write_text(yaml.safe_dump(policy_data), encoding="utf-8")
    return policy_path


def test_gate_decision_fields_match_runtime_model() -> None:
    assert [field.name for field in fields(GateDecision)] == [
        "phase",
        "failure_class",
        "action",
        "reason",
    ]


def test_classify_gate_result_signature_matches_contract() -> None:
    assert list(signature(classify_gate_result).parameters) == [
        "phase",
        "event",
        "policy",
    ]


def test_classify_phase0_without_failure_continues(gate_policy: Any) -> None:
    decision = classify_gate_result(PhaseEnum.PHASE0, None, gate_policy)

    assert decision == GateDecision(
        phase=PhaseEnum.PHASE0,
        failure_class=None,
        action=GateAction.CONTINUE,
    )


def test_classify_phase0_infra_fails_run(gate_policy: Any) -> None:
    decision = classify_gate_result(
        PhaseEnum.PHASE0,
        GateEvent(failure_class=FailureClass.INFRA),
        gate_policy,
    )

    assert decision.phase is PhaseEnum.PHASE0
    assert decision.failure_class is FailureClass.INFRA
    assert decision.action is GateAction.FAIL_RUN
    assert decision.reason == "LLM health check failed; stop before Phase 1."


def test_classify_phase2_task_level_marks_inconclusive(gate_policy: Any) -> None:
    decision = classify_gate_result(
        PhaseEnum.PHASE2,
        {"failure_class": FailureClass.TASK_LEVEL},
        gate_policy,
    )

    assert decision.phase is PhaseEnum.PHASE2
    assert decision.failure_class is FailureClass.TASK_LEVEL
    assert decision.action is GateAction.MARK_INCONCLUSIVE
    assert (
        decision.reason
        == "A single-stock LLM task failed within tolerance; continue the pool."
    )


def test_classify_phase3_infra_repairs_manifest(gate_policy: Any) -> None:
    decision = classify_gate_result(
        PhaseEnum.PHASE3,
        GateEvent(failure_class="infra"),
        gate_policy,
    )

    assert decision.action is GateAction.REPAIR_MANIFEST


def test_classify_scenario_uses_applies_to_phase(gate_policy: Any) -> None:
    decision = classify_gate_result(
        PhaseEnum.PHASE1,
        {
            "failure_class": FailureClass.INFRA,
            "scenario_id": "infra_unavailable_hard_stop",
        },
        gate_policy,
    )

    assert decision.phase is PhaseEnum.PHASE1
    assert decision.action is GateAction.FAIL_RUN


def test_unknown_policy_combination_raises(gate_policy: Any) -> None:
    with pytest.raises(
        UnknownGateFailure,
        match="phase=phase1 failure_class=infra",
    ):
        classify_gate_result(
            PhaseEnum.PHASE1,
            GateEvent(failure_class=FailureClass.INFRA),
            gate_policy,
        )


def test_classify_rejects_non_event_failure_class(gate_policy: Any) -> None:
    with pytest.raises(TypeError, match="gate event must expose failure_class"):
        classify_gate_result(PhaseEnum.PHASE0, FailureClass.INFRA, gate_policy)


def test_classify_rejects_mapping_event_with_null_failure_class(
    gate_policy: Any,
) -> None:
    with pytest.raises(ValueError, match="failure_class is required"):
        classify_gate_result(PhaseEnum.PHASE0, {"failure_class": None}, gate_policy)


def test_classify_rejects_object_event_with_null_failure_class(
    gate_policy: Any,
) -> None:
    with pytest.raises(ValueError, match="failure_class is required"):
        classify_gate_result(
            PhaseEnum.PHASE0,
            GateEvent(failure_class=None),
            gate_policy,
        )


def test_duplicate_phase_matrix_entries_are_rejected(tmp_path: Path) -> None:
    policy_data = _load_lite_policy_data()
    policy_data["phase_matrix"].append(dict(policy_data["phase_matrix"][0]))

    with pytest.raises(
        ValidationError,
        match="duplicate phase_matrix entry for phase=phase0",
    ):
        load_gate_policy(_write_policy(tmp_path, policy_data))


def test_phase0_ping_check_is_dagster_asset_check_definition() -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.checks import phase0_ping_check

    definitions = dagster.Definitions(asset_checks=[phase0_ping_check])

    assert definitions is not None
