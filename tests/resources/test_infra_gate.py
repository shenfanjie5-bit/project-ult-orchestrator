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
    assert decision.scenario_id == infra.INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID
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


def test_infrastructure_registry_includes_core_storage_and_graph_backends() -> None:
    infra = _infra_module()

    assert {
        "postgres_engine",
        "iceberg_catalog",
        "neo4j_driver",
    } <= infra.CORE_INFRASTRUCTURE_RESOURCE_KEYS
    assert infra.INFRASTRUCTURE_RESOURCE_PHASES["postgres_engine"] is PhaseEnum.PHASE0
    assert infra.INFRASTRUCTURE_RESOURCE_PHASES["iceberg_catalog"] is PhaseEnum.PHASE0
    assert infra.INFRASTRUCTURE_RESOURCE_PHASES["neo4j_driver"] is PhaseEnum.PHASE0
    assert set(infra.INFRASTRUCTURE_RESOURCE_REGISTRY) == set(
        infra.CORE_INFRASTRUCTURE_RESOURCE_KEYS,
    )


def test_guarded_graph_backend_method_failure_is_classified() -> None:
    infra = _infra_module()

    class LazyFailingNeo4jDriver:
        def execute_query(self) -> object:
            raise RuntimeError("connection reset")

    guarded = infra._GuardedInfrastructureValue(
        resource_key="neo4j_driver",
        value=LazyFailingNeo4jDriver(),
        phase=PhaseEnum.PHASE0,
        policy_path=str(LITE_POLICY_PATH),
        context=object(),
    )

    with pytest.raises(infra.InfrastructureUnavailableError) as exc_info:
        guarded.execute_query()

    assert exc_info.value.event.resource_key == "neo4j_driver"
    assert exc_info.value.event.phase is PhaseEnum.PHASE0
    assert exc_info.value.decision.action is GateAction.FAIL_RUN
    assert exc_info.value.decision.scenario_id == (
        infra.INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID
    )


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


def test_guarded_no_arg_resource_definition_initializes_normally() -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    infra = _infra_module()

    @dagster.resource
    def no_arg_resource() -> str:
        return "ok"

    value, finalizers = infra._initialize_resource(no_arg_resource, object())

    assert value == "ok"
    assert finalizers == ()


def test_guarded_resource_definition_preserves_dagster_metadata() -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    infra = _infra_module()

    @dagster.resource(
        required_resource_keys={"postgres_engine"},
        config_schema={"catalog_name": str},
    )
    def iceberg_catalog(context: object) -> str:
        return str(context.resource_config["catalog_name"])

    guarded = infra.guard_infrastructure_resource(
        "iceberg_catalog",
        iceberg_catalog,
        phase=PhaseEnum.PHASE0,
        policy_path=str(LITE_POLICY_PATH),
    )

    assert guarded.required_resource_keys == {"postgres_engine"}
    assert guarded.config_schema is iceberg_catalog.config_schema


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
