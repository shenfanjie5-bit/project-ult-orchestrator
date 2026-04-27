"""P2 dry-run provider wiring main-core, reasoner-runtime, and data-platform."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
import json
import os
from typing import Any, Protocol, cast

from pydantic import BaseModel, Field


_DEFAULT_CYCLE_ID = "CYCLE_20260416"
_DEFAULT_PROVIDER = "openai-codex"
_DEFAULT_MODEL = "gpt-5.5"
_DEFAULT_HEALTH_TIMEOUT_S = 30.0
_SOURCE_KIND = "current-cycle"
_SOURCE_LAYER = "L8"
_RECOMMENDATION_OBJECT_KEY = "recommendation_snapshot"
_REQUIRED_RECOMMENDATION_LAYERS = frozenset({"L4", "L6", "L7", "L8"})
_FORBIDDEN_PROVENANCE_MARKERS = ("smoke", "fixture", "historical")
_FORBIDDEN_INPUT_MARKERS = (*_FORBIDDEN_PROVENANCE_MARKERS, "synthetic", "ent_p2")
_INPUT_TABLE_DAILY = "main.stg_daily"
_INPUT_TABLE_STOCK_BASIC = "main.stg_stock_basic"


class P2ReasonerUnavailable(RuntimeError):
    """Raised when the reasoner-runtime health gate is unavailable."""


class P2WorldStateDeltaPayload(BaseModel):
    """Structured reasoner-runtime payload for L4 world-state deltas."""

    raw_delta: int = Field(ge=-1, le=1)
    rationale: str


class P2AlphaAnalysisPayload(BaseModel):
    """Structured reasoner-runtime payload for L6 alpha analysis."""

    score: float | None
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str
    task_failed: bool
    failure_reason: str | None


@dataclass(frozen=True, slots=True)
class P2LayerEvidence:
    """Audit/replay proof id emitted by one P2 layer."""

    layer: str
    object_ref: str
    audit_record_id: str
    replay_record_id: str
    called_llm: bool
    input_hash: str | None = None
    output_hash: str | None = None
    sanitized_input: str | None = None
    raw_output: str | None = None
    parsed_result: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class P2CurrentCycleInputs:
    """Frozen current-cycle inputs loaded from data-platform for P2 L1."""

    feature_bundles: tuple[object, ...]
    evidence: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class P2DryRunState:
    """Current-cycle formal objects and audit/replay evidence before commit."""

    cycle_id: str
    formal_objects: Mapping[str, object]
    evidence: tuple[P2LayerEvidence, ...]
    input_evidence: Mapping[str, object]

    def recommendation_provenance(self, recommendation_snapshot_id: int) -> dict[str, object]:
        """Return the fail-closed provenance payload for formal recommendation publish."""

        required_evidence = _required_layer_evidence(self.evidence)
        audit_record_ids = [item.audit_record_id for item in required_evidence]
        replay_record_ids = [item.replay_record_id for item in required_evidence]

        return {
            "cycle_id": self.cycle_id,
            "current_cycle_id": self.cycle_id,
            "source_layer": _SOURCE_LAYER,
            "source_kind": _SOURCE_KIND,
            "recommendation_snapshot_id": recommendation_snapshot_id,
            "audit_record_ids": audit_record_ids,
            "replay_record_ids": replay_record_ids,
        }


@dataclass(frozen=True, slots=True)
class P2CommittedFormalObjects:
    """Committed formal object refs waiting for manifest publication."""

    cycle_id: str
    state: P2DryRunState
    committed_objects: tuple[object, ...]
    recommendation_provenance: Mapping[str, object]
    audit_write_bundle: object
    persisted_audit_record_ids: tuple[str, ...]
    persisted_replay_record_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class P2PersistedAuditRecords:
    """Persisted audit/replay ids returned by audit-eval storage."""

    audit_record_ids: tuple[str, ...]
    replay_record_ids: tuple[str, ...]


class P2InputProvider(Protocol):
    """Current-cycle data-platform input boundary for P2 L1."""

    def load_current_cycle_inputs(
        self,
        *,
        cycle_id: str,
        graph_snapshot: str,
    ) -> P2CurrentCycleInputs:
        """Return feature bundles and source evidence for one frozen cycle."""


class P2AuditPersistencePort(Protocol):
    """Durable audit/replay persistence boundary for P2 Phase 3 handoff."""

    def persist(self, write_bundle: object) -> P2PersistedAuditRecords:
        """Persist AuditRecord and ReplayRecord rows and return their ids."""


class P2ReasonerGateway(Protocol):
    """Small reasoner-runtime facade used by the dry-run provider."""

    def check_health(self) -> object:
        """Return a reasoner-runtime health report."""

    def propose_world_state_delta(
        self,
        *,
        cycle_id: str,
        payload: Mapping[str, object],
    ) -> tuple[object, P2LayerEvidence]:
        """Return an L4 WorldStateDeltaDecision and evidence."""

    def analyze_alpha(
        self,
        *,
        cycle_id: str,
        entity_id: str,
        payload: Mapping[str, object],
    ) -> tuple[object, P2LayerEvidence]:
        """Return an L6 AlphaReasonerResponse and evidence."""


class P2PublishPort(Protocol):
    """Formal object commit and manifest publish boundary."""

    def reserve_cycle_manifest_ref(self, *, cycle_id: str) -> str:
        """Reserve the manifest ref before formal object commits."""

    def commit_formal_object(
        self,
        *,
        cycle_id: str,
        object_key: str,
        payload: Mapping[str, Any],
    ) -> object:
        """Commit one formal object and return a CommittedFormalObject."""

    def write_cycle_manifest(
        self,
        *,
        cycle_id: str,
        committed_objects: Sequence[object],
        expected_manifest_ref: str | None,
        recommendation_provenance: Mapping[str, object],
    ) -> object:
        """Publish the data-platform manifest with recommendation provenance."""


class DefaultReasonerRuntimeGateway:
    """Production adapter around reasoner-runtime Python APIs."""

    def __init__(
        self,
        *,
        provider: str | None = None,
        model: str | None = None,
        provider_profiles: Sequence[object] | None = None,
        client_factory: Callable[[object, int], object] | None = None,
        health_probe: Callable[[object, float], object] | None = None,
    ) -> None:
        self.provider = provider or os.environ.get("P2_REASONER_PROVIDER", _DEFAULT_PROVIDER)
        self.model = model or os.environ.get("P2_REASONER_MODEL", _DEFAULT_MODEL)
        self._provider_profiles = tuple(provider_profiles) if provider_profiles else None
        self._client_factory = client_factory
        self._health_probe = health_probe

    def check_health(self) -> object:
        import reasoner_runtime

        return reasoner_runtime.health_check(
            list(self._resolved_profiles()),
            probe=self._health_probe,
            timeout_s=_health_timeout_s(),
        )

    def propose_world_state_delta(
        self,
        *,
        cycle_id: str,
        payload: Mapping[str, object],
    ) -> tuple[object, P2LayerEvidence]:
        from main_core.l4_world_state.reasoner_port import WorldStateDeltaDecision

        request_id = _request_id(cycle_id, "L4", "world_state")
        parsed_result, evidence = self._generate(
            request_id=request_id,
            cycle_id=cycle_id,
            layer="L4",
            object_key="world_state_snapshot",
            target_schema="P2WorldStateDeltaPayload",
            response_model=P2WorldStateDeltaPayload,
            payload=payload,
        )
        return (
            WorldStateDeltaDecision(
                raw_delta=int(parsed_result["raw_delta"]),
                rationale=str(parsed_result["rationale"]),
                actual_model_used=evidence.object_ref.split(":", 1)[-1],
                actual_provider=self.provider,
                fallback_path=[],
            ),
            evidence,
        )

    def analyze_alpha(
        self,
        *,
        cycle_id: str,
        entity_id: str,
        payload: Mapping[str, object],
    ) -> tuple[object, P2LayerEvidence]:
        from main_core.l6_alpha.reasoner_port import AlphaReasonerResponse

        request_id = _request_id(cycle_id, "L6", entity_id)
        parsed_result, evidence = self._generate(
            request_id=request_id,
            cycle_id=cycle_id,
            layer="L6",
            object_key=f"alpha_result_snapshot/{entity_id}",
            target_schema="P2AlphaAnalysisPayload",
            response_model=P2AlphaAnalysisPayload,
            payload=payload,
        )
        return (
            AlphaReasonerResponse(
                score=cast(float | None, parsed_result["score"]),
                confidence=float(parsed_result["confidence"]),
                rationale=str(parsed_result["rationale"]),
                similar_cases=list(
                    cast(list[dict[str, Any]], parsed_result.get("similar_cases", []))
                ),
                task_failed=bool(parsed_result["task_failed"]),
                failure_reason=cast(str | None, parsed_result["failure_reason"]),
            ),
            evidence,
        )

    def _generate(
        self,
        *,
        request_id: str,
        cycle_id: str,
        layer: str,
        object_key: str,
        target_schema: str,
        response_model: type[BaseModel],
        payload: Mapping[str, object],
    ) -> tuple[dict[str, Any], P2LayerEvidence]:
        import reasoner_runtime

        request = reasoner_runtime.ReasonerRequest(
            request_id=request_id,
            caller_module="orchestrator-p2-dry-run",
            target_schema=target_schema,
            messages=[
                {
                    "role": "system",
                    "content": "Return JSON matching the requested P2 dry-run schema.",
                },
                {
                    "role": "user",
                    "content": json.dumps(payload, sort_keys=True, default=str),
                },
            ],
            configured_provider=self.provider,
            configured_model=self.model,
            max_retries=0,
            metadata={
                "cycle_id": cycle_id,
                "layer": layer,
                "object_key": object_key,
            },
        )
        kwargs: dict[str, object] = {}
        if self._client_factory is not None:
            kwargs["client_factory"] = self._client_factory
        result, bundle = reasoner_runtime.generate_structured_with_replay(
            request,
            provider_profiles=list(self._resolved_profiles()),
            schema_registry={target_schema: response_model},
            **kwargs,
        )
        parsed_result = dict(result.parsed_result)
        evidence = P2LayerEvidence(
            layer=layer,
            object_ref=f"{object_key}:{result.actual_provider}/{result.actual_model}",
            audit_record_id=f"audit-{request_id}",
            replay_record_id=f"replay-{request_id}-{bundle.replay_id}",
            called_llm=True,
            input_hash=str(bundle.input_hash),
            output_hash=str(bundle.output_hash),
            sanitized_input=str(bundle.sanitized_input),
            raw_output=str(bundle.raw_output),
            parsed_result=dict(bundle.parsed_result),
        )
        return parsed_result, evidence

    def _resolved_profiles(self) -> tuple[object, ...]:
        if self._provider_profiles is not None:
            return self._provider_profiles

        import reasoner_runtime

        return (
            reasoner_runtime.ProviderProfile(
                provider=self.provider,
                model=self.model,
                fallback_priority=0,
            ),
        )


class DataPlatformTushareCurrentCycleInputProvider:
    """Load P2 L1 inputs from frozen candidates and Tushare staging views."""

    def load_current_cycle_inputs(
        self,
        *,
        cycle_id: str,
        graph_snapshot: str,
    ) -> P2CurrentCycleInputs:
        cycle_date = _cycle_date(cycle_id)
        selected_candidates = _load_frozen_candidate_symbols(cycle_id)
        symbols = tuple(candidate["ts_code"] for candidate in selected_candidates)
        if not symbols:
            raise ValueError("P2 dry-run requires frozen current-cycle Tushare symbols")

        rows = _load_tushare_staging_rows(cycle_date=cycle_date, symbols=symbols)
        rows_by_symbol = {str(row["ts_code"]): row for row in rows}
        missing_symbols = [symbol for symbol in symbols if symbol not in rows_by_symbol]
        if missing_symbols:
            raise ValueError(
                "P2 dry-run Tushare staging input missing frozen symbols: "
                + ", ".join(missing_symbols)
            )

        feature_bundles = tuple(
            _feature_bundle_from_tushare_row(
                cycle_id=cycle_id,
                graph_snapshot=graph_snapshot,
                row=rows_by_symbol[symbol],
            )
            for symbol in symbols
        )
        evidence = {
            "cycle_id": cycle_id,
            "trade_date": cycle_date.isoformat(),
            "symbols": list(symbols),
            "candidate_ids": [candidate["candidate_id"] for candidate in selected_candidates],
            "candidate_count": len(selected_candidates),
            "input_tables": [_INPUT_TABLE_DAILY, _INPUT_TABLE_STOCK_BASIC],
            "input_row_count": len(rows),
            "source_run_ids": sorted(
                {
                    str(row[field_name])
                    for row in rows
                    for field_name in ("daily_source_run_id", "stock_basic_source_run_id")
                    if row.get(field_name)
                }
            ),
            "raw_loaded_at": sorted(
                {
                    str(row[field_name])
                    for row in rows
                    for field_name in ("daily_raw_loaded_at", "stock_basic_raw_loaded_at")
                    if row.get(field_name)
                }
            ),
            "partition_date": cycle_date.isoformat(),
            "source": "data-platform:tushare-staging:frozen-candidates",
        }
        return P2CurrentCycleInputs(
            feature_bundles=feature_bundles,
            evidence=evidence,
        )


class AuditEvalPersistencePort:
    """Persist P2 audit/replay bundles through audit-eval storage."""

    def __init__(
        self,
        storage_factory: Callable[[], object] | None = None,
    ) -> None:
        self._storage_factory = storage_factory

    def persist(self, write_bundle: object) -> P2PersistedAuditRecords:
        from audit_eval.audit import (
            get_default_storage_adapter,
            persist_audit_write_bundle,
        )

        storage = (
            self._storage_factory()
            if self._storage_factory is not None
            else get_default_storage_adapter()
        )
        audit_record_ids, replay_record_ids = persist_audit_write_bundle(
            cast(Any, write_bundle),
            storage,
        )
        audit_record_ids = tuple(audit_record_ids)
        replay_record_ids = tuple(replay_record_ids)
        _assert_persisted_bundle_ids(
            write_bundle=write_bundle,
            audit_record_ids=audit_record_ids,
            replay_record_ids=replay_record_ids,
        )
        return P2PersistedAuditRecords(
            audit_record_ids=audit_record_ids,
            replay_record_ids=replay_record_ids,
        )


class DataPlatformIcebergPublishPort:
    """Data-platform formal Iceberg writer + publish_manifest adapter."""

    def reserve_cycle_manifest_ref(self, *, cycle_id: str) -> str:
        return f"data-platform://cycle_publish_manifest/{cycle_id}"

    def commit_formal_object(
        self,
        *,
        cycle_id: str,
        object_key: str,
        payload: Mapping[str, Any],
    ) -> object:
        from main_core.l8_publish import CommittedFormalObject

        row_count = _payload_row_count(payload)
        payload_json = json.dumps(payload, sort_keys=True, allow_nan=False, default=str)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        snapshot_id = _write_formal_payload_snapshot(
            cycle_id=cycle_id,
            object_key=object_key,
            payload_json=payload_json,
            payload_hash=payload_hash,
            row_count=row_count,
        )
        return CommittedFormalObject(
            object_key=object_key,
            ref=f"data-platform://formal/{object_key}/snapshots/{snapshot_id}",
            snapshot_id=str(snapshot_id),
            payload_hash=payload_hash,
            row_count=row_count,
        )

    def write_cycle_manifest(
        self,
        *,
        cycle_id: str,
        committed_objects: Sequence[object],
        expected_manifest_ref: str | None,
        recommendation_provenance: Mapping[str, object],
    ) -> object:
        from data_platform.cycle import publish_manifest
        from main_core.l8_publish import ManifestWriteResult

        table_snapshots = {
            f"formal.{str(getattr(committed, 'object_key'))}": int(
                str(getattr(committed, "snapshot_id"))
            )
            for committed in committed_objects
        }
        manifest = publish_manifest(
            cycle_id,
            table_snapshots,
            recommendation_provenance=dict(recommendation_provenance),
        )
        object_snapshots = {
            table.removeprefix("formal."): str(snapshot.snapshot_id)
            for table, snapshot in manifest.formal_table_snapshots.items()
        }
        manifest_ref = expected_manifest_ref or self.reserve_cycle_manifest_ref(
            cycle_id=cycle_id
        )
        return ManifestWriteResult(
            manifest_ref=manifest_ref,
            manifest_version=manifest.published_at.isoformat(),
            table_snapshots=object_snapshots,
        )


class P2DryRunAssetFactoryProvider:
    """AssetFactoryProvider implementing the minimal real P2 dry-run chain."""

    def __init__(
        self,
        *,
        reasoner_gateway: P2ReasonerGateway | None = None,
        input_provider: P2InputProvider | None = None,
        publish_port_factory: Callable[[], P2PublishPort] | None = None,
        audit_persistence_port: P2AuditPersistencePort | None = None,
        provide_llm_health_probe: bool = True,
        provide_io_manager: bool = True,
        require_cycle_tag: bool = False,
    ) -> None:
        self.reasoner_gateway = reasoner_gateway or DefaultReasonerRuntimeGateway()
        self.input_provider = input_provider or DataPlatformTushareCurrentCycleInputProvider()
        self.publish_port_factory = publish_port_factory or DataPlatformIcebergPublishPort
        self.audit_persistence_port = audit_persistence_port or AuditEvalPersistencePort()
        self.provide_llm_health_probe = provide_llm_health_probe
        self.provide_io_manager = provide_io_manager
        self.require_cycle_tag = require_cycle_tag

    def get_assets(self) -> tuple[object, ...]:
        import dagster

        from main_core.common.contexts import AlphaAnalysisContext
        from main_core.l4_world_state import derive_world_state
        from main_core.l5_universe import select_official_alpha_pool
        from main_core.l6_alpha import SinglePromptAnalyzer, analyze_stock
        from main_core.l7_recommendation import generate_recommendations
        from main_core.l8_publish.manifest import commit_formal_objects
        from main_core.l8_publish.refs import (
            ALPHA_RESULT_SNAPSHOT_KEY,
            OFFICIAL_ALPHA_POOL_KEY,
            RECOMMENDATION_SNAPSHOT_KEY,
            WORLD_STATE_SNAPSHOT_KEY,
        )
        from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME, PHASE2_STAGE_KEYS
        from orchestrator.jobs.phase3 import (
            PHASE3_FORMAL_COMMIT_ASSET_KEY,
            PHASE3_GROUP_NAME,
            PHASE3_MANIFEST_ASSET_KEY,
        )

        gateway = self.reasoner_gateway
        input_provider = self.input_provider
        publish_port_factory = self.publish_port_factory
        audit_persistence_port = self.audit_persistence_port
        require_cycle_tag = self.require_cycle_tag

        @dagster.asset(name=PHASE2_STAGE_KEYS[0], group_name=PHASE2_GROUP_NAME)
        def l1(context, graph_snapshot: str):
            cycle_id = _cycle_id_from_context(
                context,
                require_tag=require_cycle_tag,
            )
            _reject_non_current_cycle_id(cycle_id)
            _reset_gateway_evidence(gateway)
            return input_provider.load_current_cycle_inputs(
                cycle_id=cycle_id,
                graph_snapshot=graph_snapshot,
            )

        @dagster.asset(name=PHASE2_STAGE_KEYS[1], group_name=PHASE2_GROUP_NAME)
        def l2(l1):
            return l1

        @dagster.asset(name=PHASE2_STAGE_KEYS[2], group_name=PHASE2_GROUP_NAME)
        def l3(l2):
            return l2

        @dagster.asset(name=PHASE2_STAGE_KEYS[3], group_name=PHASE2_GROUP_NAME)
        def l4(l3):
            feature_bundles = _feature_bundles_from_inputs(l3)
            cycle_id = str(feature_bundles[0].cycle_id)
            reasoner_port = _WorldStateReasonerPortAdapter(gateway, cycle_id=cycle_id)
            return derive_world_state(
                feature_bundles[0],
                reasoner_port=reasoner_port,
                macro_context={"entity_count": len(feature_bundles)},
            )

        @dagster.asset(name=PHASE2_STAGE_KEYS[4], group_name=PHASE2_GROUP_NAME)
        def l5(l4, l3):
            feature_bundles = _feature_bundles_from_inputs(l3)
            return select_official_alpha_pool(
                l4,
                feature_bundles,
                capacity=len(feature_bundles),
            )

        @dagster.asset(name=PHASE2_STAGE_KEYS[5], group_name=PHASE2_GROUP_NAME)
        def l6(
            l5,
            l4,
            l3,
        ):
            cycle_id = str(getattr(l5, "cycle_id"))
            feature_bundles = _feature_bundles_from_inputs(l3)
            bundles_by_entity = {str(bundle.entity_id): bundle for bundle in feature_bundles}
            analyzer = SinglePromptAnalyzer(
                _AlphaReasonerPortAdapter(gateway, cycle_id=cycle_id)
            )
            return tuple(
                analyze_stock(
                    entity_id,
                    AlphaAnalysisContext(
                        cycle_id=cycle_id,
                        entity_id=entity_id,
                        feature_bundle=bundles_by_entity[str(entity_id)],
                        world_state=l4,
                        similar_cases=[],
                    ),
                    analyzer=analyzer,
                )
                for entity_id in getattr(l5, "selected_entities")
            )

        @dagster.asset(name=PHASE2_STAGE_KEYS[6], group_name=PHASE2_GROUP_NAME)
        def l7(l5, l6, l4):
            cycle_id = str(getattr(l5, "cycle_id"))
            recommendations = tuple(generate_recommendations(l5, l6, l4, overrides=()))
            return recommendations

        @dagster.asset(name=PHASE2_STAGE_KEYS[7], group_name=PHASE2_GROUP_NAME)
        def l8(
            l3,
            l4,
            l5,
            l6,
            l7,
        ):
            cycle_id = str(getattr(l5, "cycle_id"))
            formal_objects = {
                WORLD_STATE_SNAPSHOT_KEY: l4,
                OFFICIAL_ALPHA_POOL_KEY: l5,
                ALPHA_RESULT_SNAPSHOT_KEY: l6,
                RECOMMENDATION_SNAPSHOT_KEY: l7,
            }
            _assert_current_cycle_recommendations(cycle_id, l7)
            evidence = (
                *_gateway_evidence(gateway),
                _non_llm_evidence(cycle_id, "L7", RECOMMENDATION_SNAPSHOT_KEY),
                _non_llm_evidence(cycle_id, "L8", "publish_bundle"),
            )
            return P2DryRunState(
                cycle_id=cycle_id,
                formal_objects=formal_objects,
                evidence=evidence,
                input_evidence=_input_evidence_from_inputs(l3),
            )

        @dagster.asset(
            name=PHASE3_FORMAL_COMMIT_ASSET_KEY,
            group_name=PHASE3_GROUP_NAME,
        )
        def formal_objects_commit(
            context,
            l8,
        ):
            publish_port = publish_port_factory()
            committed = commit_formal_objects(l8.cycle_id, l8.formal_objects, publish_port)
            recommendation_snapshot_id = _snapshot_id_for_object(
                committed,
                _RECOMMENDATION_OBJECT_KEY,
            )
            audit_write_bundle = _build_audit_write_bundle(
                cycle_id=l8.cycle_id,
                evidence=l8.evidence,
                committed_objects=committed,
                dagster_run_id=_run_id_from_context(context),
            )
            persisted_audit = audit_persistence_port.persist(audit_write_bundle)
            recommendation_provenance = l8.recommendation_provenance(
                recommendation_snapshot_id
            )
            _assert_production_recommendation_provenance(
                cycle_id=l8.cycle_id,
                provenance=recommendation_provenance,
            )
            _assert_persisted_provenance_ids(
                provenance=recommendation_provenance,
                persisted=persisted_audit,
            )
            return P2CommittedFormalObjects(
                cycle_id=l8.cycle_id,
                state=l8,
                committed_objects=committed,
                recommendation_provenance=recommendation_provenance,
                audit_write_bundle=audit_write_bundle,
                persisted_audit_record_ids=persisted_audit.audit_record_ids,
                persisted_replay_record_ids=persisted_audit.replay_record_ids,
            )

        @dagster.asset(
            name=PHASE3_MANIFEST_ASSET_KEY,
            group_name=PHASE3_GROUP_NAME,
        )
        def cycle_publish_manifest(formal_objects_commit):
            publish_port = publish_port_factory()
            expected_manifest_ref = publish_port.reserve_cycle_manifest_ref(
                cycle_id=formal_objects_commit.cycle_id,
            )
            _assert_production_recommendation_provenance(
                cycle_id=formal_objects_commit.cycle_id,
                provenance=formal_objects_commit.recommendation_provenance,
            )
            return publish_port.write_cycle_manifest(
                cycle_id=formal_objects_commit.cycle_id,
                committed_objects=formal_objects_commit.committed_objects,
                expected_manifest_ref=expected_manifest_ref,
                recommendation_provenance=formal_objects_commit.recommendation_provenance,
            )

        return (
            l1,
            l2,
            l3,
            l4,
            l5,
            l6,
            l7,
            l8,
            formal_objects_commit,
            cycle_publish_manifest,
        )

    def get_checks(self) -> tuple[object, ...]:
        return ()

    def get_resources(self) -> dict[str, object]:
        import dagster

        from orchestrator.checks import (
            PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY,
            Phase2PoolFailureRateEvent,
        )

        gateway = self.reasoner_gateway

        class P2LLMHealthProbeResource(dagster.ConfigurableResource):
            def create_resource(self, context: object) -> object:
                return _GatewayHealthProbe(gateway)

        class P2PoolFailureRateResource(dagster.ConfigurableResource):
            def get_phase2_pool_failure_rate_event(
                self,
            ) -> Phase2PoolFailureRateEvent:
                return Phase2PoolFailureRateEvent(
                    failed_count=0,
                    total_count=2,
                    failed_nodes=(),
                )

        resources: dict[str, object] = {
            PHASE2_POOL_FAILURE_RATE_RESOURCE_KEY: P2PoolFailureRateResource(),
        }
        if self.provide_llm_health_probe:
            resources["llm_health_probe"] = P2LLMHealthProbeResource()
        if self.provide_io_manager:
            resources["io_manager"] = dagster.mem_io_manager
        return resources


def p2_dry_run_provider() -> P2DryRunAssetFactoryProvider:
    """Zero-argument factory usable from ORCHESTRATOR_MODULE_FACTORIES."""

    return P2DryRunAssetFactoryProvider()


class _GatewayHealthProbe:
    def __init__(self, gateway: P2ReasonerGateway) -> None:
        self.gateway = gateway

    def check_health(self) -> object:
        return self.gateway.check_health()


class _WorldStateReasonerPortAdapter:
    def __init__(self, gateway: P2ReasonerGateway, *, cycle_id: str) -> None:
        self.gateway = gateway
        self.cycle_id = cycle_id

    def propose_delta(self, inputs: object, baseline_regime: str) -> object:
        decision, evidence = self.gateway.propose_world_state_delta(
            cycle_id=self.cycle_id,
            payload={
                "cycle_id": self.cycle_id,
                "baseline_regime": baseline_regime,
                "feature_bundle": _model_dump(inputs),
            },
        )
        _append_gateway_evidence(self.gateway, evidence)
        return decision


class _AlphaReasonerPortAdapter:
    def __init__(self, gateway: P2ReasonerGateway, *, cycle_id: str) -> None:
        self.gateway = gateway
        self.cycle_id = cycle_id

    def analyze_alpha(self, entity_id: object, context: object) -> object:
        response, evidence = self.gateway.analyze_alpha(
            cycle_id=self.cycle_id,
            entity_id=str(entity_id),
            payload={
                "cycle_id": self.cycle_id,
                "entity_id": str(entity_id),
                "context": _model_dump(context),
            },
        )
        _append_gateway_evidence(self.gateway, evidence)
        return response


def _append_gateway_evidence(
    gateway: P2ReasonerGateway,
    evidence: P2LayerEvidence,
) -> None:
    evidence_list = getattr(gateway, "_p2_layer_evidence", None)
    if not isinstance(evidence_list, list):
        evidence_list = []
        setattr(gateway, "_p2_layer_evidence", evidence_list)
    evidence_list.append(evidence)


def _reset_gateway_evidence(gateway: P2ReasonerGateway) -> None:
    setattr(gateway, "_p2_layer_evidence", [])


def _gateway_evidence(gateway: P2ReasonerGateway) -> tuple[P2LayerEvidence, ...]:
    evidence = getattr(gateway, "_p2_layer_evidence", [])
    return tuple(item for item in evidence if isinstance(item, P2LayerEvidence))


def _feature_bundles_from_inputs(value: object) -> tuple[object, ...]:
    if isinstance(value, P2CurrentCycleInputs):
        feature_bundles = value.feature_bundles
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        feature_bundles = tuple(value)
    else:
        raise TypeError("P2 L1 input must be P2CurrentCycleInputs or a sequence")
    if not feature_bundles:
        raise ValueError("P2 dry-run requires non-empty current-cycle feature input")
    return feature_bundles


def _input_evidence_from_inputs(value: object) -> Mapping[str, object]:
    if isinstance(value, P2CurrentCycleInputs):
        return dict(value.evidence)
    raise TypeError("P2 input evidence must come from P2CurrentCycleInputs")


def _required_layer_evidence(
    evidence: Sequence[P2LayerEvidence],
) -> tuple[P2LayerEvidence, ...]:
    required = tuple(
        item for item in evidence if item.layer in _REQUIRED_RECOMMENDATION_LAYERS
    )
    observed_layers = {item.layer for item in required}
    missing_layers = sorted(_REQUIRED_RECOMMENDATION_LAYERS - observed_layers)
    if missing_layers:
        msg = "P2 recommendation provenance missing layer evidence: "
        raise ValueError(msg + ", ".join(missing_layers))

    audit_record_ids = [item.audit_record_id for item in required]
    replay_record_ids = [item.replay_record_id for item in required]
    if not audit_record_ids or not replay_record_ids:
        raise ValueError("P2 recommendation provenance requires audit/replay ids")
    _validate_provenance_ids("audit_record_ids", audit_record_ids)
    _validate_provenance_ids("replay_record_ids", replay_record_ids)
    return required


def _validate_provenance_ids(field_name: str, values: Sequence[str]) -> None:
    if not values:
        raise ValueError(f"P2 recommendation provenance {field_name} must not be empty")
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"P2 recommendation provenance {field_name} must be non-empty")
        lowered = value.lower()
        if any(marker in lowered for marker in _FORBIDDEN_PROVENANCE_MARKERS):
            raise ValueError(
                f"P2 recommendation provenance {field_name} must not contain "
                "smoke, fixture, or historical ids"
            )


def _assert_production_recommendation_provenance(
    *,
    cycle_id: str,
    provenance: Mapping[str, object],
) -> None:
    if provenance.get("cycle_id") != cycle_id or provenance.get("current_cycle_id") != cycle_id:
        raise ValueError("P2 recommendation provenance must bind to the current cycle")
    if provenance.get("source_layer") != _SOURCE_LAYER:
        raise ValueError("P2 recommendation provenance source_layer must be L8")
    if provenance.get("source_kind") != _SOURCE_KIND:
        raise ValueError("P2 recommendation provenance source_kind must be current-cycle")
    _validate_provenance_ids(
        "audit_record_ids",
        _coerce_provenance_id_sequence(
            "audit_record_ids",
            provenance.get("audit_record_ids"),
        ),
    )
    _validate_provenance_ids(
        "replay_record_ids",
        _coerce_provenance_id_sequence(
            "replay_record_ids",
            provenance.get("replay_record_ids"),
        ),
    )


def _assert_persisted_provenance_ids(
    *,
    provenance: Mapping[str, object],
    persisted: P2PersistedAuditRecords,
) -> None:
    provenance_audit_ids = _coerce_provenance_id_sequence(
        "audit_record_ids",
        provenance.get("audit_record_ids"),
    )
    provenance_replay_ids = _coerce_provenance_id_sequence(
        "replay_record_ids",
        provenance.get("replay_record_ids"),
    )
    if tuple(provenance_audit_ids) != persisted.audit_record_ids:
        raise ValueError("P2 provenance audit ids must match persisted AuditRecord ids")
    if tuple(provenance_replay_ids) != persisted.replay_record_ids:
        raise ValueError("P2 provenance replay ids must match persisted ReplayRecord ids")


def _assert_persisted_bundle_ids(
    *,
    write_bundle: object,
    audit_record_ids: Sequence[str],
    replay_record_ids: Sequence[str],
) -> None:
    expected_audit_ids = tuple(
        str(record.record_id)
        for record in getattr(write_bundle, "audit_records", ())
    )
    expected_replay_ids = tuple(
        str(record.replay_id)
        for record in getattr(write_bundle, "replay_records", ())
    )
    if tuple(audit_record_ids) != expected_audit_ids:
        raise ValueError("persisted AuditRecord ids did not match the write bundle")
    if tuple(replay_record_ids) != expected_replay_ids:
        raise ValueError("persisted ReplayRecord ids did not match the write bundle")


def _coerce_provenance_id_sequence(
    field_name: str,
    value: object,
) -> tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise ValueError(f"P2 recommendation provenance {field_name} must be a sequence")
    return tuple(cast(Sequence[str], value))


def _build_audit_write_bundle(
    *,
    cycle_id: str,
    evidence: Sequence[P2LayerEvidence],
    committed_objects: Sequence[object],
    dagster_run_id: str,
) -> object:
    from audit_eval.contracts import AuditRecord, AuditWriteBundle, ReplayRecord

    formal_snapshot_refs = {
        str(getattr(committed, "object_key")): str(getattr(committed, "ref"))
        for committed in committed_objects
    }
    if not formal_snapshot_refs:
        raise ValueError("P2 audit/replay binding requires committed formal refs")

    created_at = datetime.now(UTC)
    required = _required_layer_evidence(evidence)
    audit_records = [
        AuditRecord(
            record_id=item.audit_record_id,
            cycle_id=cycle_id,
            layer=cast(Any, item.layer),
            object_ref=item.object_ref,
            params_snapshot={
                "cycle_id": cycle_id,
                "source_kind": _SOURCE_KIND,
                "source_layer": item.layer,
            },
            llm_lineage=_llm_lineage_for_evidence(item),
            llm_cost={},
            sanitized_input=item.sanitized_input if item.called_llm else None,
            input_hash=item.input_hash if item.called_llm else None,
            raw_output=item.raw_output if item.called_llm else None,
            parsed_result=dict(item.parsed_result or {}) if item.called_llm else None,
            output_hash=item.output_hash if item.called_llm else None,
            degradation_flags={},
            created_at=created_at,
        )
        for item in required
    ]
    replay_records = [
        ReplayRecord(
            replay_id=item.replay_record_id,
            cycle_id=cycle_id,
            object_ref=item.object_ref,
            audit_record_ids=[item.audit_record_id],
            manifest_cycle_id=cycle_id,
            formal_snapshot_refs=formal_snapshot_refs,
            graph_snapshot_ref=None,
            dagster_run_id=dagster_run_id,
            created_at=created_at,
        )
        for item in required
    ]
    return AuditWriteBundle(
        bundle_id=f"audit-write-{_request_id(cycle_id, 'L8', 'publish_manifest')}",
        manifest_cycle_id=cycle_id,
        audit_records=audit_records,
        replay_records=replay_records,
        submitted_at=created_at,
        metadata={
            "source_kind": _SOURCE_KIND,
            "source_layer": _SOURCE_LAYER,
        },
    )


def _llm_lineage_for_evidence(item: P2LayerEvidence) -> dict[str, object]:
    lineage: dict[str, object] = {"called": item.called_llm}
    if item.called_llm:
        lineage.update(
            {
                "input_hash": item.input_hash,
                "output_hash": item.output_hash,
            }
        )
    return lineage


def _non_llm_evidence(cycle_id: str, layer: str, object_key: str) -> P2LayerEvidence:
    stable_id = _request_id(cycle_id, layer, object_key)
    return P2LayerEvidence(
        layer=layer,
        object_ref=object_key,
        audit_record_id=f"audit-{stable_id}",
        replay_record_id=f"replay-{stable_id}",
        called_llm=False,
    )


def _load_frozen_candidate_symbols(cycle_id: str) -> tuple[dict[str, object], ...]:
    from data_platform.cycle.repository import _create_engine, _text

    engine = _create_engine()
    try:
        with engine.connect() as connection:
            rows = (
                connection.execute(
                    _text(
                        """
                        SELECT
                            selection.candidate_id,
                            candidate_queue.payload,
                            candidate_queue.submitted_by
                        FROM data_platform.cycle_candidate_selection AS selection
                        JOIN data_platform.candidate_queue AS candidate_queue
                          ON candidate_queue.id = selection.candidate_id
                        WHERE selection.cycle_id = :cycle_id
                        ORDER BY candidate_queue.ingest_seq ASC
                        """
                    ),
                    {"cycle_id": cycle_id},
                )
                .mappings()
                .all()
            )
    finally:
        engine.dispose()

    candidates: list[dict[str, object]] = []
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, Mapping):
            raise ValueError("P2 frozen candidate payload must be a JSON object")
        ts_code = payload.get("ts_code") or payload.get("entity_id")
        if not isinstance(ts_code, str) or not ts_code.strip():
            raise ValueError("P2 frozen candidate payload requires ts_code")
        _reject_forbidden_input_marker(ts_code, "ts_code")
        submitted_by = str(row["submitted_by"])
        _reject_forbidden_input_marker(submitted_by, "submitted_by")
        candidates.append(
            {
                "candidate_id": int(row["candidate_id"]),
                "ts_code": ts_code.strip(),
                "submitted_by": submitted_by,
            }
        )
    return tuple(candidates)


def _load_tushare_staging_rows(
    *,
    cycle_date: date,
    symbols: Sequence[str],
) -> tuple[dict[str, object], ...]:
    if not symbols:
        return ()
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("duckdb is required for P2 Tushare staging input") from exc

    from data_platform.config import get_settings

    placeholders = ", ".join("?" for _ in symbols)
    sql = f"""
        SELECT
            daily.ts_code,
            daily.trade_date,
            daily.close,
            daily.pre_close,
            daily.pct_chg,
            daily.vol,
            daily.amount,
            daily.source_run_id AS daily_source_run_id,
            daily.raw_loaded_at AS daily_raw_loaded_at,
            stock_basic.name,
            stock_basic.industry,
            stock_basic.market,
            stock_basic.source_run_id AS stock_basic_source_run_id,
            stock_basic.raw_loaded_at AS stock_basic_raw_loaded_at
        FROM stg_daily AS daily
        LEFT JOIN stg_stock_basic AS stock_basic
          ON stock_basic.ts_code = daily.ts_code
        WHERE daily.trade_date = ?
          AND daily.ts_code IN ({placeholders})
        ORDER BY daily.ts_code
    """
    connection = duckdb.connect(str(get_settings().duckdb_path))
    try:
        rows = connection.execute(sql, [cycle_date, *list(symbols)]).fetchall()
        columns = [column[0] for column in connection.description]
    finally:
        connection.close()
    result = tuple(dict(zip(columns, row, strict=True)) for row in rows)
    for row in result:
        for field_name in ("daily_source_run_id", "stock_basic_source_run_id"):
            value = row.get(field_name)
            if value:
                _reject_forbidden_input_marker(str(value), field_name)
    return result


def _feature_bundle_from_tushare_row(
    *,
    cycle_id: str,
    graph_snapshot: str,
    row: Mapping[str, object],
) -> object:
    ts_code = str(row["ts_code"])
    pct_chg = _float_value(row.get("pct_chg"))
    close = _float_value(row.get("close"))
    pre_close = _float_value(row.get("pre_close"))
    volume = _float_value(row.get("vol"))
    amount = _float_value(row.get("amount"))
    return _feature_bundle(
        cycle_id,
        ts_code,
        pct_chg / 100.0,
        graph_snapshot,
        feature_values={
            "momentum": pct_chg / 100.0,
            "close": close,
            "pre_close": pre_close,
            "volume": volume,
            "amount": amount,
        },
        signal_values={
            "source": "tushare-staging",
            "trade_date": str(row.get("trade_date")),
            "daily_source_run_id": row.get("daily_source_run_id"),
            "daily_raw_loaded_at": row.get("daily_raw_loaded_at"),
            "stock_basic_source_run_id": row.get("stock_basic_source_run_id"),
            "stock_basic_raw_loaded_at": row.get("stock_basic_raw_loaded_at"),
            "name": row.get("name"),
            "industry": row.get("industry"),
            "market": row.get("market"),
        },
    )


def _feature_bundle(
    cycle_id: str,
    entity_id: str,
    momentum: float,
    graph_snapshot: str,
    *,
    feature_values: Mapping[str, float] | None = None,
    signal_values: Mapping[str, object] | None = None,
) -> object:
    from main_core.common.schemas import FeatureSignalBundle

    return FeatureSignalBundle(
        cycle_id=cycle_id,
        entity_id=entity_id,
        feature_values=dict(feature_values or {"momentum": momentum}),
        signal_values=dict(signal_values or {"source": "current-cycle-test-input"}),
        graph_features={"graph_snapshot_ref": graph_snapshot},
        feature_weight_multiplier={
            feature_name: 1.0
            for feature_name in dict(feature_values or {"momentum": momentum})
        },
    )


def _cycle_id_from_context(context: object, *, require_tag: bool = False) -> str:
    for tag_container in (
        getattr(context, "run", None),
        context,
    ):
        tags = getattr(tag_container, "tags", None) or getattr(
            tag_container,
            "run_tags",
            None,
        )
        if isinstance(tags, Mapping):
            cycle_id = tags.get("cycle_id")
            if isinstance(cycle_id, str) and cycle_id:
                return cycle_id
    if require_tag:
        raise ValueError(
            "production P2 provider requires Dagster run tag 'cycle_id'; "
            "fixed current-cycle fallback is not allowed",
        )
    return _DEFAULT_CYCLE_ID


def _cycle_date(cycle_id: str) -> date:
    try:
        return datetime.strptime(cycle_id.removeprefix("CYCLE_"), "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(
            "P2 dry-run requires a current data-platform cycle_id like CYCLE_YYYYMMDD"
        ) from exc


def _run_id_from_context(context: object) -> str:
    run = getattr(context, "run", None)
    run_id = getattr(run, "run_id", None) or getattr(context, "run_id", None)
    if isinstance(run_id, str) and run_id.strip():
        return run_id
    return f"dagster-run-{_DEFAULT_CYCLE_ID.lower()}"


def _reject_non_current_cycle_id(cycle_id: str) -> None:
    if not cycle_id.startswith("CYCLE_"):
        raise ValueError(
            "P2 dry-run requires a current data-platform cycle_id like CYCLE_YYYYMMDD",
        )
    _cycle_date(cycle_id)


def _reject_forbidden_input_marker(value: str, field_name: str) -> None:
    lowered = value.lower()
    if any(marker in lowered for marker in _FORBIDDEN_INPUT_MARKERS):
        raise ValueError(
            f"P2 current-cycle input {field_name} must not contain "
            "smoke, fixture, historical, synthetic, or ENT_P2 markers"
        )


def _assert_current_cycle_recommendations(
    cycle_id: str,
    recommendations: Sequence[object],
) -> None:
    if not recommendations:
        raise ValueError("P2 dry-run L8 requires current-cycle recommendations")
    for recommendation in recommendations:
        if getattr(recommendation, "cycle_id", None) != cycle_id:
            raise ValueError(
                "P2 dry-run must not publish historical recommendation output",
            )


def _snapshot_id_for_object(
    committed_objects: Sequence[object],
    object_key: str,
) -> int:
    for committed in committed_objects:
        if getattr(committed, "object_key", None) == object_key:
            return int(str(getattr(committed, "snapshot_id")))
    raise ValueError(f"missing committed formal object: {object_key}")


def _payload_row_count(payload: Mapping[str, Any]) -> int:
    count = payload.get("count")
    if isinstance(count, int) and not isinstance(count, bool):
        return count
    items = payload.get("items")
    if isinstance(items, Sequence) and not isinstance(items, (str, bytes, bytearray)):
        return len(items)
    return 1


def _float_value(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 0.0
        return float(stripped)
    return float(value)  # type: ignore[arg-type]


def _health_timeout_s() -> float:
    raw_timeout = os.environ.get("P2_REASONER_HEALTH_TIMEOUT_S")
    if raw_timeout is None:
        return _DEFAULT_HEALTH_TIMEOUT_S
    try:
        timeout = float(raw_timeout)
    except ValueError:
        return _DEFAULT_HEALTH_TIMEOUT_S
    if timeout <= 0:
        return _DEFAULT_HEALTH_TIMEOUT_S
    return timeout


def _write_formal_payload_snapshot(
    *,
    cycle_id: str,
    object_key: str,
    payload_json: str,
    payload_hash: str,
    row_count: int,
) -> int:
    import pyarrow as pa  # type: ignore[import-untyped]

    from data_platform.serving.catalog import ensure_namespaces, load_catalog

    table_identifier = f"formal.{object_key}"
    schema = pa.schema(
        [
            pa.field("cycle_id", pa.string()),
            pa.field("object_key", pa.string()),
            pa.field("payload_json", pa.string()),
            pa.field("payload_hash", pa.string()),
            pa.field("row_count", pa.int64()),
            pa.field("written_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    catalog = load_catalog()
    ensure_namespaces(catalog, [("formal",)])
    table = catalog.create_table_if_not_exists(table_identifier, schema=schema)
    table.overwrite(
        pa.table(
            {
                "cycle_id": [cycle_id],
                "object_key": [object_key],
                "payload_json": [payload_json],
                "payload_hash": [payload_hash],
                "row_count": [row_count],
                "written_at": [datetime.now(UTC)],
            },
            schema=schema,
        )
    )
    snapshot = table.refresh().current_snapshot()
    if snapshot is None:
        raise RuntimeError(f"formal object {object_key} commit did not create a snapshot")
    return int(snapshot.snapshot_id)


def _request_id(cycle_id: str, layer: str, object_key: str) -> str:
    normalized = (
        f"{cycle_id}-{layer}-{object_key}"
        .replace("_", "-")
        .replace("/", "-")
        .replace(":", "-")
        .lower()
    )
    return normalized


def _model_dump(value: object) -> object:
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        return dump(mode="json")
    return value


__all__ = [
    "AuditEvalPersistencePort",
    "DataPlatformIcebergPublishPort",
    "DataPlatformTushareCurrentCycleInputProvider",
    "DefaultReasonerRuntimeGateway",
    "P2AlphaAnalysisPayload",
    "P2CommittedFormalObjects",
    "P2CurrentCycleInputs",
    "P2DryRunAssetFactoryProvider",
    "P2DryRunState",
    "P2InputProvider",
    "P2LayerEvidence",
    "P2PersistedAuditRecords",
    "P2ReasonerUnavailable",
    "P2WorldStateDeltaPayload",
    "p2_dry_run_provider",
]
