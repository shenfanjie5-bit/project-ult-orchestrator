from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from orchestrator.policy import GateAction, PhaseEnum, load_gate_policy

_DAGSTER_IMPORT_ERROR: str | None = None
try:
    import dagster as _dagster  # noqa: F401
except Exception as exc:
    _DAGSTER_IMPORT_ERROR = f"dagster is not importable: {exc}"

pytestmark = pytest.mark.skipif(
    _DAGSTER_IMPORT_ERROR is not None,
    reason=_DAGSTER_IMPORT_ERROR or "",
)

from orchestrator.checks import DataReadinessSignal


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
    infra = _infra_module()
    event = infra.InfrastructureUnavailableEvent(
        resource_key="dbt",
        phase=phase,
        reason="postgres unavailable",
    )

    decision = infra.classify_infrastructure_failure(phase, event, gate_policy)

    assert decision.phase is phase
    assert decision.action is GateAction.FAIL_RUN
    assert decision.failure_class.value == "infra"
    assert decision.reason == (
        "Core storage or graph infrastructure is unavailable; hard stop."
    )


def test_classify_infrastructure_failure_rejects_policy_misconfiguration(
    tmp_path: Path,
) -> None:
    infra = _infra_module()
    policy_data = _load_lite_policy_data()
    for entry in policy_data["phase_matrix"]:
        if entry["scenario_id"] == infra.INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID:
            entry["action"] = "repair_manifest"
            break

    policy = load_gate_policy(_write_policy(tmp_path, policy_data))
    event = infra.InfrastructureUnavailableEvent(
        resource_key="dbt",
        phase=PhaseEnum.PHASE1,
        reason="neo4j unavailable",
    )

    with pytest.raises(ValueError, match="action=fail_run"):
        infra.classify_infrastructure_failure(PhaseEnum.PHASE1, event, policy)


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


@pytest.mark.parametrize(
    "surface_name",
    [
        "get_readiness_signal",
        "get_data_readiness",
        "readiness_signal",
    ],
)
def test_guarded_data_readiness_accepts_supported_signal_surfaces(
    surface_name: str,
) -> None:
    signal = DataReadinessSignal(ready=True, cycle_id="cycle-20260416")
    provider = _readiness_provider(surface_name, signal)
    guarded = _infra_module()._GuardedInfrastructureValue(
        resource_key="data_readiness",
        value=provider,
        phase=PhaseEnum.PHASE0,
        policy_path=str(LITE_POLICY_PATH),
        context=object(),
    )

    if callable(getattr(provider, surface_name, None)):
        observed = getattr(guarded, surface_name)()
    else:
        observed = getattr(guarded, surface_name)

    assert observed is signal
    assert getattr(guarded, "missing_optional_signal", "fallback") == "fallback"
    assert not hasattr(guarded, "missing_optional_signal")


def _infra_module() -> Any:
    import orchestrator.resources.infra as infra

    return infra


def _readiness_provider(
    surface_name: str,
    signal: DataReadinessSignal,
) -> object:
    if surface_name == "get_readiness_signal":
        class GetReadinessSignalProvider:
            def get_readiness_signal(self) -> DataReadinessSignal:
                return signal

        return GetReadinessSignalProvider()

    if surface_name == "get_data_readiness":
        class GetDataReadinessProvider:
            def get_data_readiness(self) -> DataReadinessSignal:
                return signal

        return GetDataReadinessProvider()

    if surface_name == "readiness_signal":
        class ReadinessSignalProvider:
            readiness_signal = signal

        return ReadinessSignalProvider()

    raise AssertionError(f"unsupported readiness surface: {surface_name}")
