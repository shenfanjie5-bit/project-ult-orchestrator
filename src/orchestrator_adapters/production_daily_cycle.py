"""Production daily-cycle provider entrypoint.

The provider exposes the real checked-in Phase 0, Phase 1, Phase 2, Phase 3,
and audit hook surfaces. Runtime dependencies still fail closed unless the
environment supplies the corresponding data-platform, graph-engine, and
audit-eval backing stores. The Phase 2 pool gate derives from the current L8
output during production runs, with a persisted metric artifact as the durable
handoff path.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
import json
import os
from pathlib import Path
from typing import Final, Protocol

from orchestrator_adapters.p2_dry_run import (
    P2DryRunAssetFactoryProvider,
    P2PublishedManifest,
)

PRODUCTION_DAILY_CYCLE_FACTORY: Final[str] = (
    "orchestrator_adapters.production_daily_cycle:production_daily_cycle_provider"
)
BLOCKER_CODE: Final[str] = "ORCH_PRODUCTION_DAILY_CYCLE_PROVIDER_BLOCKED"
SUPPORTED_SURFACES: Final[tuple[str, ...]] = (
    "phase0_current_cycle_selection",
    "phase0_data_platform_candidate_freeze_asset",
    "phase0_data_readiness_resource",
    "phase0_graph_status_asset",
    "phase0_neo4j_graph_consistency_check",
    "phase1_graph_promotion_asset",
    "phase1_graph_snapshot_asset",
    "phase2_current_cycle_canonical_inputs",
    "phase2_main_core_l1_l8",
    "phase3_formal_objects_commit",
    "phase3_cycle_publish_manifest",
    "audit_eval_formal_audit_replay_persistence",
    "audit_eval_retrospective_hook_asset",
)
MISSING_SURFACES: Final[tuple[str, ...]] = ()
RUNTIME_BLOCKERS: Final[tuple[str, ...]] = (
    "configured_data_platform_current_cycle_runtime",
    "configured_graph_phase0_status_runtime",
    "configured_graph_phase1_runtime",
    "configured_reasoner_runtime",
    "configured_audit_eval_retrospective_hook_runtime",
    "production_current_cycle_dagster_run_evidence",
)
CURRENT_CYCLE_BINDING: Final[str] = "dagster_run_tag:cycle_id"
PHASE2_POOL_FAILURE_RATE_EVENT_ENV: Final[str] = (
    "ORCHESTRATOR_PHASE2_POOL_FAILURE_RATE_EVENT_JSON"
)
PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV: Final[str] = (
    "ORCHESTRATOR_PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT"
)
DATA_READINESS_RESOURCE_KEY: Final[str] = "data_readiness"
GRAPH_STATUS_PROVIDER_RESOURCE_KEY: Final[str] = "graph_status_provider"
AUDIT_RETROSPECTIVE_RUNTIME_RESOURCE_KEY: Final[str] = (
    "audit_eval_retrospective_hook_runtime"
)
_ALPHA_RESULT_SNAPSHOT_KEY: Final[str] = "alpha_result_snapshot"
_OFFICIAL_ALPHA_POOL_KEY: Final[str] = "official_alpha_pool"
_NON_PRODUCTION_ENV_JSON_FALLBACK_SOURCE: Final[str] = (
    "non-production env JSON fallback"
)


class GraphStatusProvider(Protocol):
    """Phase 0 graph-status runtime boundary."""

    def get_graph_status(self, *, candidate_freeze: object, cycle_id: str) -> object:
        """Return a ready graph status object or raise fail-closed."""


class RetrospectiveHookRuntime(Protocol):
    """Runtime boundary for audit-eval retrospective hooks."""

    def run(self, cycle_publish_manifest: object) -> object:
        """Validate published lineage and return a retrospective hook result."""


@dataclass(frozen=True, slots=True)
class ProductionDailyCycleProviderStatus:
    """Evidence-friendly status for the production provider set."""

    blocker_code: str
    factory: str
    blocked: bool
    supported_surfaces: tuple[str, ...]
    missing_surfaces: tuple[str, ...]
    runtime_blockers: tuple[str, ...]
    current_cycle_binding: str
    non_claims: tuple[str, ...]


class ProductionPhase0Provider:
    """Data-platform current-cycle readiness and Phase 0 freeze assets."""

    def __init__(
        self,
        *,
        graph_status_provider: GraphStatusProvider | None = None,
    ) -> None:
        self.graph_status_provider = graph_status_provider

    def get_assets(self) -> tuple[object, ...]:
        import dagster

        from orchestrator.jobs.phase0_constants import (
            PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
            PHASE0_GRAPH_STATUS_ASSET_KEY,
            PHASE0_GROUP_NAME,
        )

        @dagster.asset(
            name=PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
            group_name=PHASE0_GROUP_NAME,
        )
        def candidate_freeze(context) -> dict:
            from data_platform.cycle import freeze_current_cycle_candidates

            tag_cycle_id = _require_cycle_id_from_context(context)
            result = freeze_current_cycle_candidates()
            evidence = dict(result.evidence)
            if tag_cycle_id != result.selection.cycle_id:
                raise ValueError(
                    "current-cycle selector disagrees with Dagster run tag "
                    f"'cycle_id': selector={result.selection.cycle_id!r}, "
                    f"tag={tag_cycle_id!r}",
                )
            return {
                "cycle_id": result.selection.cycle_id,
                "trade_date": result.selection.trade_date.isoformat(),
                "selection_ref": f"cycle_candidate_selection:{result.selection.cycle_id}",
                "candidate_ids": list(result.frozen_candidate_ids),
                "evidence": evidence,
            }

        @dagster.asset(
            name=PHASE0_GRAPH_STATUS_ASSET_KEY,
            group_name=PHASE0_GROUP_NAME,
            required_resource_keys={GRAPH_STATUS_PROVIDER_RESOURCE_KEY},
        )
        def graph_status(context, candidate_freeze: dict) -> object:
            provider = getattr(context.resources, GRAPH_STATUS_PROVIDER_RESOURCE_KEY)
            cycle_id = _cycle_id_from_mapping(candidate_freeze)
            return provider.get_graph_status(
                candidate_freeze=candidate_freeze,
                cycle_id=cycle_id,
            )

        return (candidate_freeze, graph_status)

    def get_checks(self) -> tuple[object, ...]:
        import dagster

        from orchestrator.jobs.phase0_constants import (
            PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
            PHASE0_GRAPH_STATUS_ASSET_KEY,
        )

        @dagster.asset_check(
            asset=PHASE0_GRAPH_STATUS_ASSET_KEY,
            name=PHASE0_GRAPH_CONSISTENCY_CHECK_NAME,
        )
        def neo4j_graph_consistency_check(graph_status: object) -> object:
            ready = _graph_status_ready(graph_status)
            metadata = {"graph_status": _graph_status_label(graph_status)}
            return dagster.AssetCheckResult(
                passed=ready,
                metadata=metadata,
                description=(
                    "Phase 0 graph status must be ready before Phase 1 "
                    "promotion can write graph deltas."
                ),
            )

        return (neo4j_graph_consistency_check,)

    def get_resources(self) -> dict[str, object]:
        import dagster

        from data_platform.cycle import CurrentCycleReadinessProvider

        graph_status_provider = self.graph_status_provider or _FailClosedGraphStatusProvider()
        return {
            DATA_READINESS_RESOURCE_KEY: dagster.ResourceDefinition.hardcoded_resource(
                CurrentCycleReadinessProvider(),
            ),
            GRAPH_STATUS_PROVIDER_RESOURCE_KEY: (
                dagster.ResourceDefinition.hardcoded_resource(graph_status_provider)
            ),
        }


class ProductionAuditEvalProvider:
    """Audit-eval retrospective hook asset provider."""

    def __init__(
        self,
        *,
        runtime: RetrospectiveHookRuntime | None = None,
    ) -> None:
        self.runtime = runtime

    def get_assets(self) -> tuple[object, ...]:
        import dagster

        from orchestrator.jobs.audit import (
            AUDIT_EVAL_GROUP_NAME,
            RETROSPECTIVE_HOOK_ASSET_KEY,
        )

        @dagster.asset(
            name=RETROSPECTIVE_HOOK_ASSET_KEY,
            group_name=AUDIT_EVAL_GROUP_NAME,
            required_resource_keys={AUDIT_RETROSPECTIVE_RUNTIME_RESOURCE_KEY},
        )
        def retrospective_hook(context, cycle_publish_manifest: object) -> object:
            runtime = getattr(context.resources, AUDIT_RETROSPECTIVE_RUNTIME_RESOURCE_KEY)
            return runtime.run(cycle_publish_manifest)

        return (retrospective_hook,)

    def get_checks(self) -> tuple[object, ...]:
        return ()

    def get_resources(self) -> dict[str, object]:
        import dagster

        runtime = self.runtime or _EnvBackedRetrospectiveHookRuntime()
        return {
            AUDIT_RETROSPECTIVE_RUNTIME_RESOURCE_KEY: (
                dagster.ResourceDefinition.hardcoded_resource(runtime)
            ),
        }


class ProductionDailyCycleProvider:
    """Composite provider for the production daily-cycle surface."""

    def __init__(
        self,
        *,
        phase0_provider: ProductionPhase0Provider | None = None,
        graph_phase1_provider: object | None = None,
        p2_provider: P2DryRunAssetFactoryProvider | None = None,
        audit_provider: ProductionAuditEvalProvider | None = None,
    ) -> None:
        self.phase0_provider = phase0_provider or ProductionPhase0Provider()
        self.graph_phase1_provider = graph_phase1_provider or _default_graph_phase1_provider()
        self.p2_provider = p2_provider or P2DryRunAssetFactoryProvider(
            phase2_pool_failure_rate_provider=_ProductionPhase2PoolFailureRateResource(),
            require_cycle_tag=True,
        )
        self.audit_provider = audit_provider or ProductionAuditEvalProvider()

    def get_assets(self) -> tuple[object, ...]:
        return _collect("get_assets", self._providers())

    def get_checks(self) -> tuple[object, ...]:
        return _collect("get_checks", self._providers())

    def get_resources(self) -> dict[str, object]:
        resources: dict[str, object] = {}
        for provider in self._providers():
            for key, resource in provider.get_resources().items():
                if key in resources:
                    raise ValueError(f"duplicate resource key: {key}")
                resources[key] = resource
        return resources

    def status(self) -> ProductionDailyCycleProviderStatus:
        return production_daily_cycle_status()

    def _providers(self) -> tuple[object, ...]:
        return (
            self.phase0_provider,
            self.graph_phase1_provider,
            self.p2_provider,
            self.audit_provider,
        )


class _FailClosedGraphStatusProvider:
    def get_graph_status(self, *, candidate_freeze: object, cycle_id: str) -> object:
        raise RuntimeError(
            "Graph Phase 0 status runtime is not configured; provide a real "
            "Neo4j graph status provider backed by the project graph status store.",
        )


class _EnvBackedRetrospectiveHookRuntime:
    """Audit hook runtime using audit-eval's managed DuckDB repository."""

    def run(self, cycle_publish_manifest: object) -> object:
        from audit_eval.audit import DataPlatformManifestGateway
        from audit_eval.audit.storage import DuckDBReplayRepository
        from audit_eval.audit.writer import (
            AUDIT_EVAL_AUDIT_TABLE_ENV,
            AUDIT_EVAL_DUCKDB_PATH_ENV,
            AUDIT_EVAL_REPLAY_TABLE_ENV,
        )
        from audit_eval.retro import (
            InMemoryRetrospectiveHookStatusStorage,
            RetrospectiveHookRequest,
            run_real_retrospective_hook,
        )

        duckdb_path = os.environ.get(AUDIT_EVAL_DUCKDB_PATH_ENV)
        if not duckdb_path:
            raise RuntimeError(
                "Audit-eval retrospective hook runtime requires "
                f"{AUDIT_EVAL_DUCKDB_PATH_ENV}",
            )

        published = _require_p2_published_manifest(cycle_publish_manifest)
        repository = DuckDBReplayRepository(
            duckdb_path,
            audit_table=os.environ.get(AUDIT_EVAL_AUDIT_TABLE_ENV, "audit_eval.audit_records"),
            replay_table=os.environ.get(AUDIT_EVAL_REPLAY_TABLE_ENV, "audit_eval.replay_records"),
        )
        request = RetrospectiveHookRequest(
            cycle_id=published.cycle_id,
            date_ref=_cycle_date_from_cycle_id(published.cycle_id),
            manifest_ref=published.manifest_ref,
            manifest=published.audit_eval_manifest_draft(),
            replay_ids=published.persisted_replay_record_ids,
            audit_record_ids=published.persisted_audit_record_ids,
            provenance=published.recommendation_provenance,
        )
        return run_real_retrospective_hook(
            request,
            repository=repository,
            manifest_gateway=DataPlatformManifestGateway(),
            require_manifest_gateway=True,
            status_storage=InMemoryRetrospectiveHookStatusStorage(),
        )


class _ProductionPhase2PoolFailureRateResource:
    """Production Phase 2 pool gate input from current P2 output or metric artifact."""

    def get_phase2_pool_failure_rate_event(
        self,
        *,
        current_cycle_p2_output: object | None = None,
    ) -> object:
        if current_cycle_p2_output is not None:
            return _phase2_pool_event_from_current_cycle_p2_output(
                current_cycle_p2_output,
            )

        artifact = os.environ.get(PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV)
        if artifact:
            payload = _load_json_event_artifact(artifact)
            return _phase2_pool_event_from_metric_payload(
                payload,
                source="persisted P2 metric artifact",
                require_cycle_id=True,
            )

        raw_payload = os.environ.get(PHASE2_POOL_FAILURE_RATE_EVENT_ENV)
        if raw_payload:
            payload = _load_json_event_payload(raw_payload)
            return _phase2_pool_event_from_metric_payload(
                payload,
                source=_NON_PRODUCTION_ENV_JSON_FALLBACK_SOURCE,
                require_cycle_id=False,
            )

        raise RuntimeError(
            "Production phase2_pool_failure_rate runtime has no current-cycle "
            "metric; run the Phase 2 L8 output-backed check or set "
            f"{PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV} to a persisted P2 "
            "metric artifact. "
            f"{PHASE2_POOL_FAILURE_RATE_EVENT_ENV} is retained only as a "
            "non-production JSON fallback.",
        )


def production_daily_cycle_provider() -> ProductionDailyCycleProvider:
    """Factory usable from ``ORCHESTRATOR_MODULE_FACTORIES``."""

    return ProductionDailyCycleProvider()


def production_daily_cycle_status() -> ProductionDailyCycleProviderStatus:
    """Return truthful provider-set status without importing Dagster assets."""

    return ProductionDailyCycleProviderStatus(
        blocker_code=BLOCKER_CODE,
        factory=PRODUCTION_DAILY_CYCLE_FACTORY,
        blocked=True,
        supported_surfaces=SUPPORTED_SURFACES,
        missing_surfaces=MISSING_SURFACES,
        runtime_blockers=RUNTIME_BLOCKERS,
        current_cycle_binding=CURRENT_CYCLE_BINDING,
        non_claims=(
            "not_p5_shadow_run_readiness",
            "not_full_production_dagster_current_cycle_freeze_proof",
            "not_production_daily_cycle_pass_certificate",
            "not_fixed_cycle_20260415_replay",
        ),
    )


def _default_graph_phase1_provider() -> object:
    from graph_engine.providers import build_graph_phase1_provider

    return build_graph_phase1_provider()


def _collect(method_name: str, providers: Sequence[object]) -> tuple[object, ...]:
    values: list[object] = []
    for provider in providers:
        method = getattr(provider, method_name)
        values.extend(method())
    return tuple(values)


def _cycle_id_from_mapping(value: Mapping[str, object]) -> str:
    cycle_id = value.get("cycle_id")
    if not isinstance(cycle_id, str) or not cycle_id.strip():
        raise ValueError("candidate_freeze output must include cycle_id")
    return cycle_id


def _cycle_id_from_context(context: object) -> str | None:
    for container in (
        context,
        getattr(context, "run", None),
        getattr(context, "dagster_run", None),
    ):
        if container is None:
            continue
        for attr in ("run_tags", "tags"):
            tags = getattr(container, attr, None)
            if isinstance(tags, Mapping):
                cycle_id = tags.get("cycle_id")
                if isinstance(cycle_id, str) and cycle_id:
                    return cycle_id
    return None


def _require_cycle_id_from_context(context: object) -> str:
    cycle_id = _cycle_id_from_context(context)
    if cycle_id is None:
        raise ValueError(
            "production candidate_freeze requires Dagster run tag 'cycle_id' "
            "before any Phase 0 freeze side effect",
        )
    return cycle_id


def _load_json_event_payload(raw_payload: str) -> Mapping[str, object]:
    source = raw_payload.strip()
    if not source:
        raise RuntimeError("phase2 pool failure-rate event payload is empty")
    if not source.startswith("{"):
        with open(source, encoding="utf-8") as handle:
            parsed = json.load(handle)
    else:
        parsed = json.loads(source)
    if not isinstance(parsed, Mapping):
        raise RuntimeError("phase2 pool failure-rate event payload must be a JSON object")
    return parsed


def _load_json_event_artifact(path_value: str) -> Mapping[str, object]:
    source = path_value.strip()
    if not source:
        raise RuntimeError("phase2 pool failure-rate metric artifact path is empty")
    if source.startswith("{"):
        raise RuntimeError(
            f"{PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV} must be a filesystem "
            "path to a persisted JSON artifact, not inline JSON",
        )
    path = Path(source).expanduser()
    if not path.is_file():
        raise RuntimeError(
            "phase2 pool failure-rate metric artifact path does not exist or is "
            f"not a file: {path}",
        )
    with path.open(encoding="utf-8") as handle:
        parsed = json.load(handle)
    if not isinstance(parsed, Mapping):
        raise RuntimeError("phase2 pool failure-rate metric artifact must be a JSON object")
    return parsed


def _phase2_pool_event_from_current_cycle_p2_output(value: object) -> object:
    from orchestrator.checks import Phase2PoolFailureRateEvent

    cycle_id = _cycle_id_from_p2_output(value)
    _cycle_date_from_cycle_id(cycle_id)
    formal_objects = _formal_objects_from_p2_output(value)
    alpha_results = _alpha_results_from_formal_objects(formal_objects)
    selected_entities = _selected_entities_from_formal_objects(formal_objects)
    if selected_entities is not None:
        _assert_alpha_results_match_selected_entities(alpha_results, selected_entities)

    failed_nodes: list[str] = []
    for result in alpha_results:
        result_cycle_id = _required_string_field(result, "cycle_id", "alpha result")
        if result_cycle_id != cycle_id:
            raise RuntimeError(
                "phase2 pool failure-rate current-cycle output has alpha result "
                f"cycle_id {result_cycle_id!r}; expected {cycle_id!r}",
            )
        entity_id = _required_string_field(result, "entity_id", "alpha result")
        if _alpha_result_failed(result):
            failed_nodes.append(f"l6:{entity_id}")

    return Phase2PoolFailureRateEvent(
        failed_count=len(failed_nodes),
        total_count=len(alpha_results),
        failed_nodes=tuple(failed_nodes),
        reason="derived from current-cycle P2 outputs",
        cycle_id=cycle_id,
    )


def _phase2_pool_event_from_metric_payload(
    payload: Mapping[str, object],
    *,
    source: str,
    require_cycle_id: bool,
) -> object:
    from orchestrator.checks import Phase2PoolFailureRateEvent

    cycle_id = _optional_string(
        payload.get("cycle_id") or payload.get("current_cycle_id"),
        "cycle_id",
    )
    if cycle_id is None:
        if require_cycle_id:
            raise RuntimeError(
                f"phase2 pool failure-rate {source} must include cycle_id",
            )
    else:
        _cycle_date_from_cycle_id(cycle_id)

    reason = _optional_string(payload.get("reason"), "reason")
    return Phase2PoolFailureRateEvent(
        failed_count=_required_non_bool_int(payload, "failed_count"),
        total_count=_required_non_bool_int(payload, "total_count"),
        failed_nodes=tuple(_required_string_sequence(payload, "failed_nodes")),
        reason=_labeled_metric_reason(source, reason),
        cycle_id=cycle_id,
    )


def _cycle_id_from_p2_output(value: object) -> str:
    cycle_id = _mapping_or_attr(value, "cycle_id")
    if not isinstance(cycle_id, str) or not cycle_id.strip():
        raise RuntimeError(
            "phase2 pool failure-rate current-cycle output must include cycle_id",
        )
    return cycle_id


def _formal_objects_from_p2_output(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping) and _ALPHA_RESULT_SNAPSHOT_KEY in value:
        return value

    formal_objects = _mapping_or_attr(value, "formal_objects")
    if not isinstance(formal_objects, Mapping):
        raise RuntimeError(
            "phase2 pool failure-rate current-cycle output must include "
            "formal_objects with alpha_result_snapshot",
        )
    return formal_objects


def _alpha_results_from_formal_objects(
    formal_objects: Mapping[str, object],
) -> tuple[object, ...]:
    alpha_results = formal_objects.get(_ALPHA_RESULT_SNAPSHOT_KEY)
    if isinstance(alpha_results, (str, bytes)) or not isinstance(
        alpha_results,
        Sequence,
    ):
        raise RuntimeError(
            "phase2 pool failure-rate current-cycle metric requires "
            "formal_objects.alpha_result_snapshot",
        )
    if not alpha_results:
        raise RuntimeError(
            "phase2 pool failure-rate current-cycle metric requires at least "
            "one alpha result",
        )
    return tuple(alpha_results)


def _selected_entities_from_formal_objects(
    formal_objects: Mapping[str, object],
) -> tuple[str, ...] | None:
    pool = formal_objects.get(_OFFICIAL_ALPHA_POOL_KEY)
    if pool is None:
        return None

    selected_entities = _mapping_or_attr(pool, "selected_entities")
    if selected_entities is None:
        return None
    if isinstance(selected_entities, (str, bytes)) or not isinstance(
        selected_entities,
        Sequence,
    ):
        raise RuntimeError(
            "phase2 pool failure-rate current-cycle metric requires "
            "official_alpha_pool.selected_entities to be a sequence",
        )

    values: list[str] = []
    for entity_id in selected_entities:
        if not isinstance(entity_id, str) or not entity_id.strip():
            raise RuntimeError(
                "phase2 pool failure-rate current-cycle metric requires "
                "official_alpha_pool.selected_entities to contain strings",
            )
        values.append(entity_id)
    return tuple(values)


def _assert_alpha_results_match_selected_entities(
    alpha_results: Sequence[object],
    selected_entities: Sequence[str],
) -> None:
    result_entities = tuple(
        _required_string_field(result, "entity_id", "alpha result")
        for result in alpha_results
    )
    if len(set(result_entities)) != len(result_entities):
        raise RuntimeError(
            "phase2 pool failure-rate current-cycle metric has duplicate "
            "alpha result entity_id values",
        )
    if set(result_entities) != set(selected_entities):
        raise RuntimeError(
            "phase2 pool failure-rate current-cycle metric alpha results must "
            "match official_alpha_pool.selected_entities",
        )


def _alpha_result_failed(value: object) -> bool:
    status = _mapping_or_attr(value, "status")
    if status == "inconclusive":
        return True
    if status == "ok":
        return False

    task_failed = _mapping_or_attr(value, "task_failed")
    if isinstance(task_failed, bool):
        return task_failed

    raise RuntimeError(
        "phase2 pool failure-rate current-cycle metric requires alpha result "
        "status 'ok' or 'inconclusive'",
    )


def _required_string_field(value: object, key: str, subject: str) -> str:
    field = _mapping_or_attr(value, key)
    if not isinstance(field, str) or not field.strip():
        raise RuntimeError(f"phase2 pool failure-rate {subject} {key} must be a string")
    return field


def _mapping_or_attr(value: object, key: str) -> object:
    if isinstance(value, Mapping):
        return value.get(key)
    return getattr(value, key, None)


def _labeled_metric_reason(source: str, reason: str | None) -> str:
    if reason is None or not reason.strip():
        return source
    if reason.startswith(source):
        return reason
    return f"{source}: {reason}"


def _required_non_bool_int(payload: Mapping[str, object], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"phase2 pool failure-rate event {key!r} must be an integer")
    return value


def _required_string_sequence(
    payload: Mapping[str, object],
    key: str,
) -> tuple[str, ...]:
    value = payload.get(key, ())
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise RuntimeError(f"phase2 pool failure-rate event {key!r} must be a string list")
    strings: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise RuntimeError(
                f"phase2 pool failure-rate event {key!r} must contain only strings",
            )
        strings.append(item)
    return tuple(strings)


def _optional_string(value: object, key: str) -> str | None:
    if value is None or isinstance(value, str):
        return value
    raise RuntimeError(f"phase2 pool failure-rate event {key!r} must be a string")


def _graph_status_ready(value: object) -> bool:
    if isinstance(value, Mapping):
        return value.get("graph_status") == "ready" and value.get("writer_lock_token") is None
    graph_status = getattr(value, "graph_status", None)
    writer_lock = getattr(value, "writer_lock_token", None)
    return graph_status == "ready" and writer_lock is None


def _graph_status_label(value: object) -> str:
    if isinstance(value, Mapping):
        return str(value.get("graph_status", "unknown"))
    return str(getattr(value, "graph_status", "unknown"))


def _require_p2_published_manifest(value: object) -> P2PublishedManifest:
    if isinstance(value, P2PublishedManifest):
        return value
    raise TypeError(
        "retrospective_hook requires the enriched P2PublishedManifest emitted by "
        "cycle_publish_manifest",
    )


def _cycle_date_from_cycle_id(cycle_id: str) -> date:
    try:
        return datetime.strptime(cycle_id, "CYCLE_%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"invalid cycle_id for production daily cycle: {cycle_id!r}") from exc


__all__ = [
    "AUDIT_RETROSPECTIVE_RUNTIME_RESOURCE_KEY",
    "BLOCKER_CODE",
    "CURRENT_CYCLE_BINDING",
    "DATA_READINESS_RESOURCE_KEY",
    "GRAPH_STATUS_PROVIDER_RESOURCE_KEY",
    "MISSING_SURFACES",
    "PHASE2_POOL_FAILURE_RATE_EVENT_ENV",
    "PHASE2_POOL_FAILURE_RATE_METRIC_ARTIFACT_ENV",
    "PRODUCTION_DAILY_CYCLE_FACTORY",
    "RUNTIME_BLOCKERS",
    "SUPPORTED_SURFACES",
    "GraphStatusProvider",
    "ProductionAuditEvalProvider",
    "ProductionDailyCycleProvider",
    "ProductionDailyCycleProviderStatus",
    "ProductionPhase0Provider",
    "RetrospectiveHookRuntime",
    "production_daily_cycle_provider",
    "production_daily_cycle_status",
]
