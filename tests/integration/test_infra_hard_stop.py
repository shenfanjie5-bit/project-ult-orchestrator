from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from tests.integration.conftest import asset_materialization_keys

_DOWNSTREAM_PUBLISH_ASSET_KEY = "phase2_downstream_publish"


@pytest.mark.parametrize(
    ("failing_resource_key", "blocked_asset_key"),
    [
        ("dbt", "graph_promotion"),
        ("data_readiness", "graph_promotion"),
        ("llm_health_probe", "graph_promotion"),
        ("postgres_engine", "graph_promotion"),
        ("iceberg_catalog", "graph_promotion"),
        ("neo4j_driver", "graph_promotion"),
        ("phase2_pool_failure_rate", _DOWNSTREAM_PUBLISH_ASSET_KEY),
    ],
)
def test_daily_cycle_hard_stops_on_guarded_resource_init_failure(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    failing_resource_key: str,
    blocked_asset_key: str,
) -> None:
    dagster = dagster_module
    defs = _build_defs(
        dagster,
        monkeypatch=monkeypatch,
        stub_policy_path=stub_policy_path,
        failing_resource_key=failing_resource_key,
    )
    dagster.Definitions.validate_loadable(defs)

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        result = defs.get_job_def("daily_cycle_job").execute_in_process(
            instance=dagster_instance,
            raise_on_error=False,
            tags={"cycle_id": "cycle-20260416"},
        )

    materialized_keys = asset_materialization_keys(result)
    payload = _single_alert_for_resource(caplog.records, failing_resource_key)

    assert result.success is False
    assert dagster.AssetKey([blocked_asset_key]) not in materialized_keys
    assert payload["phase"] in {"phase0", "phase1", "phase2"}
    assert payload["failure_class"] == "infra"
    assert payload["action"] == "fail_run"
    assert payload["scenario_id"] == "infra_unavailable_hard_stop"
    assert payload["runbook_url"] == "docs/RUNBOOK_P5.md#phase2-infra-fail_run"
    assert payload["failed_node"] == failing_resource_key
    assert f"fake {failing_resource_key} unavailable" in str(payload["summary"])


def test_daily_cycle_alerts_when_gate_policy_resource_load_fails(
    dagster_module: object,
    dagster_instance: object,
    tmp_dbt_project: Path,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dagster = dagster_module
    missing_policy_path = tmp_path / "missing_gate_policy.yaml"
    defs = _build_defs(
        dagster,
        monkeypatch=monkeypatch,
        stub_policy_path=str(missing_policy_path),
        failing_resource_key="none",
    )
    dagster.Definitions.validate_loadable(defs)

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        result = defs.get_job_def("daily_cycle_job").execute_in_process(
            instance=dagster_instance,
            raise_on_error=False,
            tags={"cycle_id": "cycle-20260416"},
        )

    materialized_keys = asset_materialization_keys(result)
    payload = _single_alert_for_resource(caplog.records, "gate_policy")

    assert result.success is False
    assert dagster.AssetKey(["graph_promotion"]) not in materialized_keys
    assert payload["phase"] == "phase0"
    assert payload["failure_class"] == "infra"
    assert payload["action"] == "fail_run"
    assert payload["scenario_id"] == "infra_unavailable_hard_stop"
    assert payload["runbook_url"] == "docs/RUNBOOK_P5.md#phase2-infra-fail_run"
    assert payload["failed_node"] == "gate_policy"
    assert missing_policy_path.name in str(payload["summary"])


def test_build_definitions_alerts_when_provider_get_resources_fails(
    dagster_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import orchestrator.definitions as definitions_module
    from orchestrator.resources import InfrastructureUnavailableError

    class BrokenGraphProvider:
        infrastructure_resource_key = "neo4j_driver"

        def get_resources(self) -> dict[str, object]:
            raise RuntimeError("fake neo4j driver construction failed")

    with caplog.at_level(logging.WARNING, logger="orchestrator.alerting.dispatcher"):
        with pytest.raises(InfrastructureUnavailableError) as error:
            definitions_module.build_definitions(
                module_factories=[BrokenGraphProvider()],
                policy_path=stub_policy_path,
            )

    payload = _single_alert_for_resource(caplog.records, "neo4j_driver")

    assert error.value.event.resource_key == "neo4j_driver"
    assert error.value.decision.action.value == "fail_run"
    assert payload["phase"] == "phase0"
    assert payload["failure_class"] == "infra"
    assert payload["action"] == "fail_run"
    assert payload["failed_node"] == "neo4j_driver"
    assert "fake neo4j driver construction failed" in payload["summary"]


def _build_defs(
    dagster: Any,
    *,
    monkeypatch: pytest.MonkeyPatch,
    stub_policy_path: str,
    failing_resource_key: str,
) -> object:
    import orchestrator.definitions as definitions_module

    if failing_resource_key == "dbt":
        monkeypatch.setattr(
            definitions_module,
            "DbtCliResource",
            _raising_dbt_resource_type(dagster),
        )

    return definitions_module.build_definitions(
        module_factories=[
            _fake_phase0_surface_provider(dagster, failing_resource_key),
            _fake_phase1_provider(dagster, failing_resource_key),
            _fake_phase2_provider(dagster, failing_resource_key),
        ],
        policy_path=stub_policy_path,
    )


def _raising_dbt_resource_type(dagster: Any) -> type:
    class RaisingDbtResource(dagster.ConfigurableResource):
        project_dir: str
        profiles_dir: str

        def create_resource(self, context: object) -> object:
            raise RuntimeError("fake dbt unavailable")

    return RaisingDbtResource


def _fake_phase0_surface_provider(
    dagster: Any,
    failing_resource_key: str,
) -> object:
    from orchestrator.checks import DataReadinessSignal
    from orchestrator.jobs.phase0_constants import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
        PHASE0_GRAPH_STATUS_ASSET_KEY,
        PHASE0_GROUP_NAME,
        PHASE0_READINESS_ASSET_KEY,
    )
    from orchestrator.sensors.data_readiness import DATA_READINESS_RESOURCE_KEY

    class RaisingResource(dagster.ConfigurableResource):
        resource_key: str

        def create_resource(self, context: object) -> object:
            raise RuntimeError(f"fake {self.resource_key} unavailable")

    class FakeDataReadinessProvider:
        def get_data_readiness_signal(self) -> DataReadinessSignal:
            return DataReadinessSignal(ready=True, cycle_id="cycle-20260416")

    class FakeDataReadinessResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> FakeDataReadinessProvider:
            return FakeDataReadinessProvider()

    class FakeLLMHealthResult:
        healthy = True
        summary = "provider ready"
        provider = "fake-llm"

    class FakeLLMHealthProbe:
        def check_health(self) -> FakeLLMHealthResult:
            return FakeLLMHealthResult()

    class FakeLLMHealthProbeResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> FakeLLMHealthProbe:
            return FakeLLMHealthProbe()

    class FakeCoreResource(dagster.ConfigurableResource):
        resource_key: str

        def create_resource(self, context: object) -> dict[str, str]:
            return {"resource_key": self.resource_key}

    @dagster.asset(
        name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        group_name=PHASE0_GROUP_NAME,
        deps=[dagster.AssetKey([PHASE0_READINESS_ASSET_KEY])],
    )
    def candidate_freeze(
        data_readiness: dagster.ResourceParam[object],
        dbt: dagster.ResourceParam[object],
        postgres_engine: dagster.ResourceParam[object],
        iceberg_catalog: dagster.ResourceParam[object],
    ) -> str:
        return "frozen"

    @dagster.asset(
        name=PHASE0_GRAPH_STATUS_ASSET_KEY,
        group_name=PHASE0_GROUP_NAME,
    )
    def graph_status(
        candidate_freeze: str,
        neo4j_driver: dagster.ResourceParam[object],
    ) -> str:
        del candidate_freeze, neo4j_driver
        return "ready"

    @dagster.asset_check(
        asset=graph_status,
        name=PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
        blocking=True,
    )
    def neo4j_graph_consistency_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    class FakePhase0SurfaceProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (candidate_freeze, graph_status)

        def get_checks(self) -> tuple[object, ...]:
            return (neo4j_graph_consistency_check,)

        def get_resources(self) -> dict[str, object]:
            data_readiness_resource: object
            llm_health_probe_resource: object

            if failing_resource_key == DATA_READINESS_RESOURCE_KEY:
                data_readiness_resource = RaisingResource(
                    resource_key=DATA_READINESS_RESOURCE_KEY,
                )
            else:
                data_readiness_resource = FakeDataReadinessResource()

            if failing_resource_key == "llm_health_probe":
                llm_health_probe_resource = RaisingResource(
                    resource_key="llm_health_probe",
                )
            else:
                llm_health_probe_resource = FakeLLMHealthProbeResource()

            resources: dict[str, object] = {
                DATA_READINESS_RESOURCE_KEY: data_readiness_resource,
                "llm_health_probe": llm_health_probe_resource,
            }
            for resource_key in ("postgres_engine", "iceberg_catalog"):
                if failing_resource_key == resource_key:
                    resources[resource_key] = RaisingResource(
                        resource_key=resource_key,
                    )
                else:
                    resources[resource_key] = FakeCoreResource(
                        resource_key=resource_key,
                    )
            return resources

    return FakePhase0SurfaceProvider()


def _fake_phase1_provider(dagster: Any, failing_resource_key: str) -> object:
    from orchestrator.jobs.phase0_constants import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GRAPH_STATUS_ASSET_KEY,
        PHASE0_READINESS_ASSET_KEY,
    )
    from orchestrator.jobs.phase1 import (
        PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        PHASE1_GROUP_NAME,
    )

    @dagster.asset(
        name=PHASE1_GRAPH_PROMOTION_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
        deps=[
            dagster.AssetKey([PHASE0_READINESS_ASSET_KEY]),
            dagster.AssetKey([PHASE0_CANDIDATE_FREEZE_ASSET_KEY]),
            dagster.AssetKey([PHASE0_GRAPH_STATUS_ASSET_KEY]),
        ],
    )
    def graph_promotion(neo4j_driver: dagster.ResourceParam[object]) -> str:
        return "promoted"

    @dagster.asset(
        name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
    )
    def graph_snapshot(graph_promotion: str) -> str:
        return f"snapshot:{graph_promotion}"

    class RaisingResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> object:
            raise RuntimeError("fake neo4j_driver unavailable")

    class FakeNeo4jDriverResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> dict[str, str]:
            return {"resource_key": "neo4j_driver"}

    class FakePhase1Provider:
        def get_assets(self) -> tuple[object, ...]:
            return (graph_promotion, graph_snapshot)

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            if failing_resource_key == "neo4j_driver":
                return {"neo4j_driver": RaisingResource()}
            return {"neo4j_driver": FakeNeo4jDriverResource()}

    return FakePhase1Provider()


def _fake_phase2_provider(dagster: Any, failing_resource_key: str) -> object:
    from orchestrator.checks import (
        PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
        Phase2PoolFailureRateEvent,
    )
    from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME, PHASE2_STAGE_KEYS

    @dagster.asset(name=PHASE2_STAGE_KEYS[0], group_name=PHASE2_GROUP_NAME)
    def phase2_l1(graph_snapshot: str) -> str:
        return f"{graph_snapshot}:l1"

    @dagster.asset(name=PHASE2_STAGE_KEYS[1], group_name=PHASE2_GROUP_NAME)
    def phase2_l2(l1: str) -> str:
        return f"{l1}:l2"

    @dagster.asset(name=PHASE2_STAGE_KEYS[2], group_name=PHASE2_GROUP_NAME)
    def phase2_l3(l2: str) -> str:
        return f"{l2}:l3"

    @dagster.asset(name=PHASE2_STAGE_KEYS[3], group_name=PHASE2_GROUP_NAME)
    def phase2_l4(l3: str) -> str:
        return f"{l3}:l4"

    @dagster.asset(name=PHASE2_STAGE_KEYS[4], group_name=PHASE2_GROUP_NAME)
    def phase2_l5(l4: str) -> str:
        return f"{l4}:l5"

    @dagster.asset(name=PHASE2_STAGE_KEYS[5], group_name=PHASE2_GROUP_NAME)
    def phase2_l6(l5: str) -> str:
        return f"{l5}:l6"

    @dagster.asset(name=PHASE2_STAGE_KEYS[6], group_name=PHASE2_GROUP_NAME)
    def phase2_l7(l6: str) -> str:
        return f"{l6}:l7"

    @dagster.asset(name=_DOWNSTREAM_PUBLISH_ASSET_KEY, group_name=PHASE2_GROUP_NAME)
    def phase2_downstream_publish(l7: str) -> str:
        return f"{l7}:published"

    class RaisingResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> object:
            raise RuntimeError(
                f"fake {PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY} unavailable",
            )

    class FakePhase2PoolFailureRateResource(dagster.ConfigurableResource):
        def get_phase2_pool_failure_rate_event(
            self,
        ) -> Phase2PoolFailureRateEvent:
            return Phase2PoolFailureRateEvent(
                failed_count=0,
                total_count=10,
                failed_nodes=(),
                reason="healthy fake pool",
            )

    phase2_assets = (
        phase2_l1,
        phase2_l2,
        phase2_l3,
        phase2_l4,
        phase2_l5,
        phase2_l6,
        phase2_l7,
        phase2_downstream_publish,
    )

    class FakePhase2Provider:
        def get_assets(self) -> tuple[object, ...]:
            return phase2_assets

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            if failing_resource_key == PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY:
                return {PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: RaisingResource()}
            return {
                PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: (
                    FakePhase2PoolFailureRateResource()
                ),
            }

    return FakePhase2Provider()


def _single_alert_for_resource(
    records: Iterable[logging.LogRecord],
    resource_key: str,
) -> dict[str, object]:
    payloads = [
        payload
        for payload in _alert_payloads(records)
        if payload.get("failed_node") == resource_key
    ]
    assert len(payloads) == 1
    return payloads[0]


def _alert_payloads(records: Iterable[logging.LogRecord]) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for record in records:
        try:
            payload = json.loads(record.message)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads
