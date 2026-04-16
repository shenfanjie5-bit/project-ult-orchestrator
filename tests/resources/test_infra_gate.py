from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from orchestrator.policy import GateAction, PhaseEnum, load_gate_policy

pytest.importorskip("dagster", reason="dagster is not installed")

from orchestrator.resources import (  # noqa: E402
    INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID,
    InfrastructureUnavailableEvent,
    classify_infrastructure_failure,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


@pytest.fixture
def gate_policy() -> Any:
    return load_gate_policy(LITE_POLICY_PATH)


def _load_lite_policy_data() -> dict[str, Any]:
    return yaml.safe_load(LITE_POLICY_PATH.read_text(encoding="utf-8"))


def _write_policy(tmp_path: Path, policy_data: dict[str, Any]) -> Path:
    policy_path = tmp_path / "gate_policy.yaml"
    policy_path.write_text(yaml.safe_dump(policy_data), encoding="utf-8")
    return policy_path


@pytest.mark.parametrize(
    "phase",
    [
        PhaseEnum.PHASE0,
        PhaseEnum.PHASE1,
        PhaseEnum.PHASE2,
        PhaseEnum.PHASE3,
    ],
)
def test_classify_infrastructure_failure_hard_stops_each_phase(
    gate_policy: Any,
    phase: PhaseEnum,
) -> None:
    event = InfrastructureUnavailableEvent(
        resource_key="dbt",
        phase=phase,
        reason="postgres unavailable",
    )

    decision = classify_infrastructure_failure(phase, event, gate_policy)

    assert decision.phase is phase
    assert decision.action is GateAction.FAIL_RUN
    assert decision.failure_class.value == "infra"
    assert decision.reason == (
        "Core storage or graph infrastructure is unavailable; hard stop."
    )


def test_classify_infrastructure_failure_rejects_policy_misconfiguration(
    tmp_path: Path,
) -> None:
    policy_data = _load_lite_policy_data()
    for entry in policy_data["phase_matrix"]:
        if entry["scenario_id"] == INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID:
            entry["action"] = "repair_manifest"
            break

    policy = load_gate_policy(_write_policy(tmp_path, policy_data))
    event = InfrastructureUnavailableEvent(
        resource_key="dbt",
        phase=PhaseEnum.PHASE1,
        reason="neo4j unavailable",
    )

    with pytest.raises(ValueError, match="action=fail_run"):
        classify_infrastructure_failure(PhaseEnum.PHASE1, event, policy)


def test_classify_infrastructure_failure_does_not_steal_manifest_repair(
    gate_policy: Any,
) -> None:
    from orchestrator.checks.phase3 import (
        ManifestWriteFailureEvent,
        classify_manifest_write_failure,
    )

    decision = classify_manifest_write_failure(
        ManifestWriteFailureEvent(
            repair_node="repair_cycle_publish_manifest",
            reason="manifest sink timed out",
        ),
        gate_policy,
    )

    assert decision.action is GateAction.REPAIR_MANIFEST
