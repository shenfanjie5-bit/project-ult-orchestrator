from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest

from tests.integration.conftest import (
    asset_check_evaluations,
    asset_materialization_keys,
    metadata_value,
)


def test_p2_dry_run_materializes_current_cycle_l8_and_manifest_handoff(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.phase2 import PHASE2_STAGE_KEYS
    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
    )
    from orchestrator_adapters.p2_dry_run import (
        DefaultReasonerRuntimeGateway,
        P2DryRunAssetFactoryProvider,
    )

    reasoner_recorder = _ReasonerRecorder()
    publish_recorder = _PublishRecorder()
    provider = P2DryRunAssetFactoryProvider(
        reasoner_gateway=DefaultReasonerRuntimeGateway(
            client_factory=reasoner_recorder.client_factory,
            health_probe=_reasoner_health_probe(available=True),
        ),
        publish_port_factory=lambda: _FakePublishPort(publish_recorder),
    )
    defs = build_definitions(
        module_factories=[
            _fake_phase0_provider(dagster),
            _fake_phase1_provider(dagster),
            provider,
        ],
        policy_path=stub_policy_path,
    )
    dagster.Definitions.validate_loadable(defs)

    result = defs.get_job_def("daily_cycle_job").execute_in_process(
        instance=dagster_instance,
        tags={"cycle_id": "CYCLE_20260416"},
    )

    materialized_keys = asset_materialization_keys(result)
    provenance = publish_recorder.manifest_provenance
    formal_commit = result.output_for_node(PHASE3_FORMAL_COMMIT_ASSET_KEY)
    audit_bundle = formal_commit.audit_write_bundle

    assert result.success is True
    assert {dagster.AssetKey([stage]) for stage in PHASE2_STAGE_KEYS} <= materialized_keys
    assert dagster.AssetKey([PHASE3_FORMAL_COMMIT_ASSET_KEY]) in materialized_keys
    assert dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY]) in materialized_keys
    assert reasoner_recorder.layers == ["L4", "L6", "L6"]
    assert provenance is not None
    assert provenance["cycle_id"] == "CYCLE_20260416"
    assert provenance["current_cycle_id"] == "CYCLE_20260416"
    assert provenance["source_layer"] == "L8"
    assert provenance["source_kind"] == "current-cycle"
    assert _id_layers(provenance["audit_record_ids"]) >= {"l4", "l6", "l7", "l8"}
    assert _id_layers(provenance["replay_record_ids"]) >= {"l4", "l6", "l7", "l8"}
    assert _no_forbidden_marker(provenance["audit_record_ids"])
    assert _no_forbidden_marker(provenance["replay_record_ids"])
    assert len(audit_bundle.audit_records) >= 5
    assert len(audit_bundle.replay_records) >= 5
    assert all(record.manifest_cycle_id == "CYCLE_20260416" for record in audit_bundle.replay_records)


def test_p2_dry_run_hard_stops_without_llm_before_l8_or_publish(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.phase2 import PHASE2_STAGE_KEYS
    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_MANIFEST_ASSET_KEY,
    )
    from orchestrator_adapters.p2_dry_run import (
        DefaultReasonerRuntimeGateway,
        P2DryRunAssetFactoryProvider,
    )

    reasoner_recorder = _ReasonerRecorder()
    publish_recorder = _PublishRecorder()
    provider = P2DryRunAssetFactoryProvider(
        reasoner_gateway=DefaultReasonerRuntimeGateway(
            client_factory=reasoner_recorder.client_factory,
            health_probe=_reasoner_health_probe(available=False),
        ),
        publish_port_factory=lambda: _FakePublishPort(publish_recorder),
    )
    defs = build_definitions(
        module_factories=[
            _fake_phase0_provider(dagster),
            _fake_phase1_provider(dagster),
            provider,
        ],
        policy_path=stub_policy_path,
    )
    dagster.Definitions.validate_loadable(defs)

    result = defs.get_job_def("daily_cycle_job").execute_in_process(
        instance=dagster_instance,
        raise_on_error=False,
        tags={"cycle_id": "CYCLE_20260416"},
    )
    materialized_keys = asset_materialization_keys(result)
    llm_health_evaluation = _check_evaluation_by_name(result, "llm_health_check")

    assert result.success is False
    assert getattr(llm_health_evaluation, "passed", None) is False
    assert metadata_value(llm_health_evaluation, "scenario_id") == "phase0_llm_health_check_failed"
    assert dagster.AssetKey([PHASE2_STAGE_KEYS[7]]) not in materialized_keys
    assert dagster.AssetKey([PHASE3_FORMAL_COMMIT_ASSET_KEY]) not in materialized_keys
    assert dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY]) not in materialized_keys
    assert reasoner_recorder.calls == []
    assert publish_recorder.commit_calls == []
    assert publish_recorder.manifest_provenance is None


def test_p2_provenance_rejects_missing_or_smoke_audit_replay_ids() -> None:
    from orchestrator_adapters.p2_dry_run import P2DryRunState, P2LayerEvidence

    missing_l6_state = P2DryRunState(
        cycle_id="CYCLE_20260416",
        formal_objects={},
        evidence=(
            _evidence("L4"),
            _evidence("L7"),
            _evidence("L8"),
        ),
    )
    with pytest.raises(ValueError, match="missing layer evidence: L6"):
        missing_l6_state.recommendation_provenance(14)

    smoke_state = P2DryRunState(
        cycle_id="CYCLE_20260416",
        formal_objects={},
        evidence=(
            _evidence("L4"),
            _evidence("L6"),
            _evidence("L7"),
            P2LayerEvidence(
                layer="L8",
                object_ref="publish_bundle",
                audit_record_id="audit-p1c-smoke-CYCLE_20260416",
                replay_record_id="replay-p1c-smoke-CYCLE_20260416",
                called_llm=False,
            ),
        ),
    )
    with pytest.raises(ValueError, match="must not contain smoke"):
        smoke_state.recommendation_provenance(14)


@dataclass
class _ReasonerRecorder:
    calls: list[Mapping[str, Any]] = field(default_factory=list)

    @property
    def layers(self) -> list[str]:
        return [str(call["metadata"]["layer"]) for call in self.calls]

    def client_factory(self, profile: object, max_retries: int) -> object:
        return _FakeStructuredClient(self)


class _FakeStructuredClient:
    def __init__(self, recorder: _ReasonerRecorder) -> None:
        self._recorder = recorder

    def create_structured(
        self,
        *,
        messages: list[dict[str, Any]],
        response_model: type[Any],
        metadata: Mapping[str, Any] | None = None,
    ) -> object:
        from reasoner_runtime.structured import StructuredCallResult

        self._recorder.calls.append(
            {
                "messages": messages,
                "response_model": response_model,
                "metadata": dict(metadata or {}),
            }
        )
        if response_model.__name__ == "P2WorldStateDeltaPayload":
            payload = {"raw_delta": 0, "rationale": "current-cycle world state"}
        else:
            payload = {
                "score": 0.72,
                "confidence": 0.83,
                "rationale": "current-cycle alpha evidence",
                "similar_cases": [],
                "task_failed": False,
                "failure_reason": None,
            }
        return StructuredCallResult(
            parsed_result=payload,
            raw_output=json.dumps(payload, sort_keys=True),
            token_usage={"prompt": 1, "completion": 1, "total": 2},
            cost_estimate=0.0,
            latency_ms=1,
        )


@dataclass
class _PublishRecorder:
    commit_calls: list[dict[str, Any]] = field(default_factory=list)
    manifest_provenance: dict[str, object] | None = None


class _FakePublishPort:
    def __init__(self, recorder: _PublishRecorder) -> None:
        self._recorder = recorder

    def reserve_cycle_manifest_ref(self, *, cycle_id: str) -> str:
        return f"fake-manifest://{cycle_id}"

    def commit_formal_object(
        self,
        *,
        cycle_id: str,
        object_key: str,
        payload: Mapping[str, Any],
    ) -> object:
        from main_core.l8_publish import CommittedFormalObject

        snapshot_id = 1000 + len(self._recorder.commit_calls) + 1
        payload_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        self._recorder.commit_calls.append(
            {"cycle_id": cycle_id, "object_key": object_key, "payload": dict(payload)}
        )
        return CommittedFormalObject(
            object_key=object_key,
            ref=f"fake-formal://{object_key}/{snapshot_id}",
            snapshot_id=str(snapshot_id),
            payload_hash=payload_hash,
            row_count=_payload_row_count(payload),
        )

    def write_cycle_manifest(
        self,
        *,
        cycle_id: str,
        committed_objects: Sequence[object],
        expected_manifest_ref: str | None,
        recommendation_provenance: Mapping[str, object],
    ) -> object:
        from data_platform.cycle.manifest import FORMAL_RECOMMENDATION_SNAPSHOT, FormalTableSnapshot
        from data_platform.cycle.recommendation_provenance import (
            preflight_recommendation_snapshot_publish,
        )
        from main_core.l8_publish import ManifestWriteResult

        recommendation_snapshot_id = int(
            str(
                next(
                    committed.snapshot_id
                    for committed in committed_objects
                    if committed.object_key == "recommendation_snapshot"
                )
            )
        )
        preflight_recommendation_snapshot_publish(
            cycle_id=cycle_id,
            recommendation_snapshot=FormalTableSnapshot(
                table=FORMAL_RECOMMENDATION_SNAPSHOT,
                snapshot_id=recommendation_snapshot_id,
            ),
            provenance=recommendation_provenance,
        )
        self._recorder.manifest_provenance = dict(recommendation_provenance)
        return ManifestWriteResult(
            manifest_ref=expected_manifest_ref or self.reserve_cycle_manifest_ref(cycle_id=cycle_id),
            manifest_version="test-manifest",
            table_snapshots={
                str(committed.object_key): str(committed.snapshot_id)
                for committed in committed_objects
            },
        )


def _fake_phase0_provider(dagster: Any) -> object:
    from orchestrator.checks import DataReadinessSignal
    from orchestrator.jobs.phase0_constants import (
        PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
        PHASE0_GRAPH_STATUS_ASSET_KEY,
        PHASE0_GROUP_NAME,
    )
    from orchestrator.sensors.data_readiness import DATA_READINESS_RESOURCE_KEY

    class FakeDataReadinessProvider:
        def get_data_readiness_signal(self) -> DataReadinessSignal:
            return DataReadinessSignal(ready=True, cycle_id="CYCLE_20260416")

    class FakeDataReadinessResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> FakeDataReadinessProvider:
            return FakeDataReadinessProvider()

    @dagster.asset(
        name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
        group_name=PHASE0_GROUP_NAME,
    )
    def candidate_freeze() -> str:
        return "frozen"

    @dagster.asset(name=PHASE0_GRAPH_STATUS_ASSET_KEY, group_name=PHASE0_GROUP_NAME)
    def graph_status(candidate_freeze: str) -> str:
        return f"{candidate_freeze}:ready"

    @dagster.asset_check(
        asset=graph_status,
        name=PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
        blocking=True,
    )
    def neo4j_graph_consistency_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    class FakePhase0Provider:
        def get_assets(self) -> tuple[object, ...]:
            return (candidate_freeze, graph_status)

        def get_checks(self) -> tuple[object, ...]:
            return (neo4j_graph_consistency_check,)

        def get_resources(self) -> dict[str, object]:
            return {DATA_READINESS_RESOURCE_KEY: FakeDataReadinessResource()}

    return FakePhase0Provider()


def _fake_phase1_provider(dagster: Any) -> object:
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
    def graph_promotion() -> str:
        return "promoted"

    @dagster.asset(
        name=PHASE1_GRAPH_SNAPSHOT_ASSET_KEY,
        group_name=PHASE1_GROUP_NAME,
    )
    def graph_snapshot(graph_promotion: str) -> str:
        return f"snapshot:{graph_promotion}"

    class FakePhase1Provider:
        def get_assets(self) -> tuple[object, ...]:
            return (graph_promotion, graph_snapshot)

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {}

    return FakePhase1Provider()


def _reasoner_health_probe(*, available: bool) -> object:
    def probe(profile: object, timeout_s: float) -> object:
        import reasoner_runtime

        return reasoner_runtime.ProviderHealthStatus(
            provider=str(getattr(profile, "provider")),
            model=str(getattr(profile, "model")),
            reachable=available,
            latency_ms=1,
            quota_status=reasoner_runtime.QuotaStatus.ok
            if available
            else reasoner_runtime.QuotaStatus.exhausted,
            error=None if available else "test provider unavailable",
        )

    return probe


def _check_evaluation_by_name(result: object, check_name: str) -> object:
    for evaluation in asset_check_evaluations(result):
        if _check_name(evaluation) == check_name:
            return evaluation
    pytest.fail(f"missing asset check evaluation: {check_name}")


def _check_name(evaluation: object) -> object:
    check_key = getattr(evaluation, "check_key", None)
    if check_key is not None:
        return getattr(check_key, "name", None)
    return getattr(evaluation, "check_name", getattr(evaluation, "name", None))


def _payload_row_count(payload: Mapping[str, Any]) -> int:
    count = payload.get("count")
    if isinstance(count, int) and not isinstance(count, bool):
        return count
    items = payload.get("items")
    if isinstance(items, Sequence) and not isinstance(items, (str, bytes, bytearray)):
        return len(items)
    return 1


def _id_layers(values: object) -> set[str]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        return set()
    return {
        layer
        for value in values
        if isinstance(value, str)
        for layer in ("l4", "l6", "l7", "l8")
        if f"-{layer}-" in value.lower()
    }


def _no_forbidden_marker(values: object) -> bool:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        return False
    forbidden = ("smoke", "fixture", "historical")
    return all(
        isinstance(value, str) and not any(marker in value.lower() for marker in forbidden)
        for value in values
    )


def _evidence(layer: str) -> object:
    from orchestrator_adapters.p2_dry_run import P2LayerEvidence

    layer_slug = layer.lower()
    return P2LayerEvidence(
        layer=layer,
        object_ref=f"{layer_slug}_object",
        audit_record_id=f"audit-cycle-20260416-{layer_slug}-object",
        replay_record_id=f"replay-cycle-20260416-{layer_slug}-object",
        called_llm=False,
    )
