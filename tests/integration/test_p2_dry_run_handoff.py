from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from types import SimpleNamespace
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
        AuditEvalPersistencePort,
        DefaultReasonerRuntimeGateway,
        P2DryRunAssetFactoryProvider,
    )
    from audit_eval.audit import InMemoryFormalAuditStorageAdapter

    reasoner_recorder = _ReasonerRecorder()
    publish_recorder = _PublishRecorder()
    audit_storage = InMemoryFormalAuditStorageAdapter()
    provider = P2DryRunAssetFactoryProvider(
        reasoner_gateway=DefaultReasonerRuntimeGateway(
            client_factory=reasoner_recorder.client_factory,
            health_probe=_reasoner_health_probe(available=True),
        ),
        input_provider=_StaticCurrentCycleInputProvider(),
        publish_port_factory=lambda: _FakePublishPort(publish_recorder),
        audit_persistence_port=AuditEvalPersistencePort(
            storage_factory=lambda: audit_storage,
        ),
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
    assert formal_commit.persisted_audit_record_ids == tuple(
        record.record_id for record in audit_bundle.audit_records
    )
    assert formal_commit.persisted_replay_record_ids == tuple(
        record.replay_id for record in audit_bundle.replay_records
    )
    assert len(audit_storage.audit_rows) == len(audit_bundle.audit_records)
    assert len(audit_storage.replay_rows) == len(audit_bundle.replay_records)
    assert formal_commit.state.input_evidence["cycle_id"] == "CYCLE_20260416"
    assert formal_commit.state.input_evidence["selection_ref"] == (
        "cycle_candidate_selection:CYCLE_20260416"
    )
    assert formal_commit.state.input_evidence["entity_ids"] == [
        "ENT_STOCK_600519.SH",
        "ENT_STOCK_000001.SZ",
    ]
    assert formal_commit.state.input_evidence["canonical_dataset_refs"] == [
        "price_bar",
        "security_master",
    ]
    assert "stg_" not in json.dumps(formal_commit.state.input_evidence)
    pool_payload = next(
        call["payload"]
        for call in publish_recorder.commit_calls
        if call["object_key"] == "official_alpha_pool"
    )
    committed_entities = set(pool_payload["selected_entities"])
    assert "ENT_STOCK_600519.SH" in committed_entities
    assert "ENT_P2_A" not in committed_entities
    assert "ENT_P2_B" not in committed_entities


def test_p2_dry_run_handoff_uses_data_platform_canonical_provider(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dagster = dagster_module

    from audit_eval.audit import InMemoryFormalAuditStorageAdapter
    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.phase3 import PHASE3_FORMAL_COMMIT_ASSET_KEY
    from orchestrator_adapters import p2_dry_run
    from orchestrator_adapters.p2_dry_run import (
        AuditEvalPersistencePort,
        DataPlatformCanonicalCurrentCycleInputProvider,
        DefaultReasonerRuntimeGateway,
        P2DryRunAssetFactoryProvider,
    )

    def fake_candidates(cycle_id: str) -> tuple[dict[str, object], ...]:
        assert cycle_id == "CYCLE_20260416"
        return (
            {"candidate_id": 10, "ts_code": "600519.SH", "submitted_by": "candidate-freeze"},
            {"candidate_id": 11, "ts_code": "000001.SZ", "submitted_by": "candidate-freeze"},
        )

    def fake_current_cycle_inputs(
        cycle_id: str,
        selection_ref: str,
        candidate_ids: Sequence[str | int],
        as_of_snapshot: Mapping[str, int] | None = None,
    ) -> tuple[dict[str, object], ...]:
        assert cycle_id == "CYCLE_20260416"
        assert selection_ref == "cycle_candidate_selection:CYCLE_20260416"
        assert tuple(candidate_ids) == ("600519.SH", "000001.SZ")
        assert as_of_snapshot is None
        return (
            _canonical_input_row("ENT_STOCK_600519.SH", 0.012),
            _canonical_input_row("ENT_STOCK_000001.SZ", -0.004),
        )

    ex3_graph_signal = _safe_ex3_graph_signal()

    def fake_ex3_graph_signals(cycle_id: str) -> tuple[dict[str, object], ...]:
        assert cycle_id == "CYCLE_20260416"
        return (ex3_graph_signal,)

    monkeypatch.setattr(p2_dry_run, "_load_frozen_candidate_symbols", fake_candidates)
    monkeypatch.setattr(p2_dry_run, "_load_frozen_ex3_graph_signals", fake_ex3_graph_signals)
    import data_platform.cycle as data_platform_cycle

    monkeypatch.setattr(
        data_platform_cycle,
        "load_current_cycle_inputs",
        fake_current_cycle_inputs,
    )

    reasoner_recorder = _ReasonerRecorder()
    publish_recorder = _PublishRecorder()
    audit_storage = InMemoryFormalAuditStorageAdapter()
    provider = P2DryRunAssetFactoryProvider(
        reasoner_gateway=DefaultReasonerRuntimeGateway(
            client_factory=reasoner_recorder.client_factory,
            health_probe=_reasoner_health_probe(available=True),
        ),
        publish_port_factory=lambda: _FakePublishPort(publish_recorder),
        audit_persistence_port=AuditEvalPersistencePort(
            storage_factory=lambda: audit_storage,
        ),
    )
    assert isinstance(provider.input_provider, DataPlatformCanonicalCurrentCycleInputProvider)
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

    formal_commit = result.output_for_node(PHASE3_FORMAL_COMMIT_ASSET_KEY)
    pool_payload = next(
        call["payload"]
        for call in publish_recorder.commit_calls
        if call["object_key"] == "official_alpha_pool"
    )
    committed_entities = set(pool_payload["selected_entities"])

    assert result.success is True
    assert formal_commit.state.input_evidence["candidate_ids"] == [10, 11]
    assert formal_commit.state.input_evidence["entity_ids"] == [
        "ENT_STOCK_600519.SH",
        "ENT_STOCK_000001.SZ",
    ]
    assert formal_commit.state.input_evidence["canonical_snapshot_ids"] == {
        "price_bar": 101,
        "security_master": 202,
    }
    assert _no_source_specific_input_evidence(formal_commit.state.input_evidence)
    assert committed_entities == {"ENT_STOCK_600519.SH", "ENT_STOCK_000001.SZ"}
    assert "ENT_P2_A" not in committed_entities
    assert "ENT_P2_B" not in committed_entities
    assert publish_recorder.manifest_provenance is not None
    l6_payloads = [
        json.loads(str(call["messages"][1]["content"]))
        for call in reasoner_recorder.calls
        if call["metadata"]["layer"] == "L6"
    ]
    graph_features = l6_payloads[0]["context"]["feature_bundle"]["graph_features"]
    assert graph_features["ex3_graph_signals"] == [ex3_graph_signal]
    assert graph_features["same_cycle_ex3_graph_signals"] == [ex3_graph_signal]
    assert _no_unsafe_ex3_graph_signal_fields(graph_features)


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
        AuditEvalPersistencePort,
        DefaultReasonerRuntimeGateway,
        P2DryRunAssetFactoryProvider,
    )
    from audit_eval.audit import InMemoryFormalAuditStorageAdapter

    reasoner_recorder = _ReasonerRecorder()
    publish_recorder = _PublishRecorder()
    audit_storage = InMemoryFormalAuditStorageAdapter()
    provider = P2DryRunAssetFactoryProvider(
        reasoner_gateway=DefaultReasonerRuntimeGateway(
            client_factory=reasoner_recorder.client_factory,
            health_probe=_reasoner_health_probe(available=False),
        ),
        input_provider=_StaticCurrentCycleInputProvider(),
        publish_port_factory=lambda: _FakePublishPort(publish_recorder),
        audit_persistence_port=AuditEvalPersistencePort(
            storage_factory=lambda: audit_storage,
        ),
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


def test_p2_dry_run_does_not_publish_when_audit_persistence_fails(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.phase3 import PHASE3_MANIFEST_ASSET_KEY
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
        input_provider=_StaticCurrentCycleInputProvider(),
        publish_port_factory=lambda: _FakePublishPort(publish_recorder),
        audit_persistence_port=_FailingAuditPersistencePort(),
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

    assert result.success is False
    assert dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY]) not in asset_materialization_keys(result)
    assert publish_recorder.manifest_provenance is None


@pytest.mark.parametrize("forbidden_marker", ["smoke", "fixture", "historical"])
def test_p2_provenance_rejects_missing_or_forbidden_audit_replay_ids(
    forbidden_marker: str,
) -> None:
    from orchestrator_adapters.p2_dry_run import P2DryRunState, P2LayerEvidence

    missing_l6_state = P2DryRunState(
        cycle_id="CYCLE_20260416",
        formal_objects={},
        evidence=(
            _evidence("L4"),
            _evidence("L7"),
            _evidence("L8"),
        ),
        input_evidence={},
    )
    with pytest.raises(ValueError, match="missing layer evidence: L6"):
        missing_l6_state.recommendation_provenance(14)

    forbidden_state = P2DryRunState(
        cycle_id="CYCLE_20260416",
        formal_objects={},
        evidence=(
            _evidence("L4"),
            _evidence("L6"),
            _evidence("L7"),
            P2LayerEvidence(
                layer="L8",
                object_ref="publish_bundle",
                audit_record_id=f"audit-p1c-{forbidden_marker}-CYCLE_20260416",
                replay_record_id=f"replay-p1c-{forbidden_marker}-CYCLE_20260416",
                called_llm=False,
            ),
        ),
        input_evidence={},
    )
    with pytest.raises(ValueError, match="must not contain smoke, fixture, or historical"):
        forbidden_state.recommendation_provenance(14)


def test_audit_eval_persistence_port_uses_retry_safe_bundle_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import audit_eval.audit as audit_module
    from orchestrator_adapters.p2_dry_run import AuditEvalPersistencePort

    storage = object()
    write_bundle = SimpleNamespace(
        audit_records=[SimpleNamespace(record_id="audit-current-cycle-l8")],
        replay_records=[SimpleNamespace(replay_id="replay-current-cycle-l8")],
    )
    calls: list[tuple[object, object]] = []

    def persist_bundle(write_bundle_arg: object, storage_arg: object) -> tuple[list[str], list[str]]:
        calls.append((write_bundle_arg, storage_arg))
        return ["audit-current-cycle-l8"], ["replay-current-cycle-l8"]

    monkeypatch.setattr(audit_module, "persist_audit_write_bundle", persist_bundle)

    result = AuditEvalPersistencePort(storage_factory=lambda: storage).persist(write_bundle)

    assert calls == [(write_bundle, storage)]
    assert result.audit_record_ids == ("audit-current-cycle-l8",)
    assert result.replay_record_ids == ("replay-current-cycle-l8",)


def test_audit_eval_persistence_port_durable_retry_recovers_half_written_audit_rows(
    tmp_path: Path,
) -> None:
    duckdb = pytest.importorskip("duckdb")

    from audit_eval.audit import (
        DuckDBReplayRepository,
        ManagedDuckDBFormalAuditStorageAdapter,
        persist_audit_records,
    )
    from orchestrator_adapters.p2_dry_run import AuditEvalPersistencePort

    db_path = tmp_path / "audit_eval.duckdb"
    storage = ManagedDuckDBFormalAuditStorageAdapter(db_path)
    write_bundle = _sample_p2_audit_write_bundle(dagster_run_id="dagster-run-first")
    retried_bundle = _sample_p2_audit_write_bundle(dagster_run_id="dagster-run-retry")
    expected_audit_ids = [record.record_id for record in write_bundle.audit_records]
    expected_replay_ids = [record.replay_id for record in write_bundle.replay_records]

    persist_audit_records(write_bundle, storage)
    first = AuditEvalPersistencePort(storage_factory=lambda: storage).persist(retried_bundle)
    second = AuditEvalPersistencePort(storage_factory=lambda: storage).persist(
        _sample_p2_audit_write_bundle(dagster_run_id="dagster-run-second-retry")
    )
    repository = DuckDBReplayRepository(db_path)

    assert first.audit_record_ids == tuple(expected_audit_ids)
    assert first.replay_record_ids == tuple(expected_replay_ids)
    assert second == first
    assert write_bundle.metadata["dagster_run_id"] == "dagster-run-first"
    assert retried_bundle.metadata["dagster_run_id"] == "dagster-run-retry"
    assert retried_bundle.audit_records == write_bundle.audit_records
    assert retried_bundle.replay_records == write_bundle.replay_records
    assert repository.get_audit_records(expected_audit_ids) == list(retried_bundle.audit_records)
    assert repository.get_replay_record_by_id(expected_replay_ids[0]) == retried_bundle.replay_records[0]

    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        audit_count = connection.execute(
            'SELECT count(*) FROM "audit_eval"."audit_eval"."audit_records"'
        ).fetchone()[0]
        replay_count = connection.execute(
            'SELECT count(*) FROM "audit_eval"."audit_eval"."replay_records"'
        ).fetchone()[0]
    finally:
        connection.close()
    assert audit_count == len(expected_audit_ids)
    assert replay_count == len(expected_replay_ids)


def test_data_platform_canonical_provider_loads_current_cycle_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orchestrator_adapters import p2_dry_run

    def fake_candidates(cycle_id: str) -> tuple[dict[str, object], ...]:
        assert cycle_id == "CYCLE_20260416"
        return (
            {"candidate_id": 10, "ts_code": "600519.SH", "submitted_by": "candidate-freeze"},
            {"candidate_id": 11, "ts_code": "000001.SZ", "submitted_by": "candidate-freeze"},
        )

    def fake_current_cycle_inputs(
        cycle_id: str,
        selection_ref: str,
        candidate_ids: Sequence[str | int],
        as_of_snapshot: Mapping[str, int] | None = None,
    ) -> tuple[dict[str, object], ...]:
        assert cycle_id == "CYCLE_20260416"
        assert selection_ref == "cycle_candidate_selection:CYCLE_20260416"
        assert tuple(candidate_ids) == ("600519.SH", "000001.SZ")
        assert as_of_snapshot is None
        return (
            _canonical_input_row("ENT_STOCK_600519.SH", 0.012),
            _canonical_input_row("ENT_STOCK_000001.SZ", -0.004),
        )

    monkeypatch.setattr(p2_dry_run, "_load_frozen_candidate_symbols", fake_candidates)
    monkeypatch.setattr(p2_dry_run, "_load_frozen_ex3_graph_signals", lambda cycle_id: ())
    import data_platform.cycle as data_platform_cycle

    monkeypatch.setattr(
        data_platform_cycle,
        "load_current_cycle_inputs",
        fake_current_cycle_inputs,
    )

    inputs = p2_dry_run.DataPlatformCanonicalCurrentCycleInputProvider().load_current_cycle_inputs(
        cycle_id="CYCLE_20260416",
        graph_snapshot="graph://snapshot/current",
    )

    assert [bundle.entity_id for bundle in inputs.feature_bundles] == [
        "ENT_STOCK_600519.SH",
        "ENT_STOCK_000001.SZ",
    ]
    assert inputs.evidence["candidate_ids"] == [10, 11]
    assert inputs.evidence["entity_ids"] == [
        "ENT_STOCK_600519.SH",
        "ENT_STOCK_000001.SZ",
    ]
    assert inputs.evidence["canonical_dataset_refs"] == ["price_bar", "security_master"]
    assert inputs.evidence["canonical_snapshot_ids"] == {
        "price_bar": 101,
        "security_master": 202,
    }
    assert _no_source_specific_input_evidence(inputs.evidence)


@pytest.mark.parametrize("marker", ["ENT_P2_A", "ENT_P2_B", "synthetic-current-cycle"])
def test_load_frozen_candidate_symbols_rejects_synthetic_candidates(
    marker: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from data_platform.cycle import repository
    from orchestrator_adapters import p2_dry_run

    rows = [
        {
            "candidate_id": 1,
            "payload": json.dumps({"ts_code": marker}),
            "submitted_by": "candidate-freeze",
        }
    ]
    monkeypatch.setattr(repository, "_create_engine", lambda: _FakeEngine(rows))

    with pytest.raises(ValueError, match="synthetic|ENT_P2"):
        p2_dry_run._load_frozen_candidate_symbols("CYCLE_20260416")


def test_load_frozen_ex3_graph_signals_sanitizes_accepted_contract_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from data_platform.cycle import repository
    from orchestrator_adapters import p2_dry_run

    payload = _ex3_graph_delta_payload(
        properties={
            "impact_score": 0.91,
            "safe_details": {"direction": "positive", "chunk": "drop"},
            "raw_text": "drop",
            "chunk": {"text": "drop"},
            "light_rag_artifact": {"artifact_id": "drop"},
            "large_blob": "x" * 4096,
            "metadata": {"source": "drop"},
            "private_note": "drop",
        }
    )
    payload["payload_type"] = "Ex-3"
    payload["submitted_by"] = "candidate-freeze"
    rows = [{"candidate_id": 40, "payload": json.dumps(payload)}]
    engine = _FakeEngine(rows)
    monkeypatch.setattr(repository, "_create_engine", lambda: engine)

    signals = p2_dry_run._load_frozen_ex3_graph_signals("CYCLE_20260416")

    assert signals == (
        {
            "delta_id": "delta-ex3-bridge",
            "source_node": "ENT_STOCK_600519.SH",
            "target_node": "ENT_STOCK_000001.SZ",
            "relation_type": "supplier_of",
            "properties": {
                "impact_score": 0.91,
                "safe_details": {"direction": "positive"},
            },
            "evidence_refs": ["evidence-ex3-bridge"],
            "cycle_id": "CYCLE_20260416",
            "candidate_id": 40,
            "selection_ref": "cycle_candidate_selection:CYCLE_20260416",
        },
    )
    statement, params = engine.connection.executed[0]
    sql = str(statement)
    assert params == {"cycle_id": "CYCLE_20260416", "payload_type": "Ex-3"}
    assert "validation_status = 'accepted'" in sql
    assert "payload_type = :payload_type" in sql
    assert _no_unsafe_ex3_graph_signal_fields(signals)


@pytest.mark.parametrize("marker", ["ENT_P2_A", "ENT_P2_B", "synthetic-current-cycle"])
def test_load_tushare_staging_rows_reads_duckdb_and_rejects_synthetic_sources(
    marker: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    duckdb = pytest.importorskip("duckdb")
    from data_platform.config import reset_settings_cache
    from orchestrator_adapters import p2_dry_run

    db_path = tmp_path / "data_platform.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        _seed_tushare_staging_tables(connection)
    finally:
        connection.close()

    monkeypatch.setenv("DP_PG_DSN", "postgresql://user@localhost:5432/proj")
    monkeypatch.setenv("DP_RAW_ZONE_PATH", str(tmp_path / "raw"))
    monkeypatch.setenv("DP_ICEBERG_WAREHOUSE_PATH", str(tmp_path / "warehouse"))
    monkeypatch.setenv("DP_DUCKDB_PATH", str(db_path))
    reset_settings_cache()
    try:
        rows = p2_dry_run._load_tushare_staging_rows(
            cycle_date=date(2026, 4, 16),
            symbols=("600519.SH",),
        )
        assert [row["ts_code"] for row in rows] == ["600519.SH"]
        assert rows[0]["daily_source_run_id"] == "daily-run-600519.SH"

        connection = duckdb.connect(str(db_path))
        try:
            connection.execute(
                "UPDATE stg_daily SET source_run_id = ? WHERE ts_code = '600519.SH'",
                [marker],
            )
        finally:
            connection.close()

        with pytest.raises(ValueError, match="synthetic|ENT_P2"):
            p2_dry_run._load_tushare_staging_rows(
                cycle_date=date(2026, 4, 16),
                symbols=("600519.SH",),
            )
    finally:
        reset_settings_cache()


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


class _StaticCurrentCycleInputProvider:
    def load_current_cycle_inputs(
        self,
        *,
        cycle_id: str,
        graph_snapshot: str,
    ) -> object:
        from main_core.common.schemas import FeatureSignalBundle
        from orchestrator_adapters.p2_dry_run import P2CurrentCycleInputs

        return P2CurrentCycleInputs(
            feature_bundles=(
                FeatureSignalBundle(
                    cycle_id=cycle_id,
                    entity_id="ENT_STOCK_600519.SH",
                    feature_values={"momentum": 0.012, "close": 1700.0},
                    signal_values={
                        "origin": "canonical-current-cycle",
                        "trade_date": "2026-04-16",
                        "canonical_dataset_refs": ["price_bar", "security_master"],
                    },
                    graph_features={"graph_snapshot_ref": graph_snapshot},
                    feature_weight_multiplier={"momentum": 1.0, "close": 1.0},
                ),
                FeatureSignalBundle(
                    cycle_id=cycle_id,
                    entity_id="ENT_STOCK_000001.SZ",
                    feature_values={"momentum": -0.004, "close": 11.0},
                    signal_values={
                        "origin": "canonical-current-cycle",
                        "trade_date": "2026-04-16",
                        "canonical_dataset_refs": ["price_bar", "security_master"],
                    },
                    graph_features={"graph_snapshot_ref": graph_snapshot},
                    feature_weight_multiplier={"momentum": 1.0, "close": 1.0},
                ),
            ),
            evidence={
                "cycle_id": cycle_id,
                "trade_date": "2026-04-16",
                "selection_ref": f"cycle_candidate_selection:{cycle_id}",
                "candidate_ids": [10, 11],
                "entity_ids": ["ENT_STOCK_600519.SH", "ENT_STOCK_000001.SZ"],
                "canonical_dataset_refs": ["price_bar", "security_master"],
                "canonical_snapshot_ids": {"price_bar": 101, "security_master": 202},
                "row_count": 2,
                "lineage_refs": [
                    f"cycle:{cycle_id}",
                    f"selection:cycle_candidate_selection:{cycle_id}",
                    "candidate:10",
                    "candidate:11",
                    "canonical:price_bar@101",
                    "canonical:security_master@202",
                ],
            },
        )


class _FailingAuditPersistencePort:
    def persist(self, write_bundle: object) -> object:
        raise RuntimeError("audit storage unavailable")


class _FakeEngine:
    def __init__(self, rows: Sequence[Mapping[str, object]]) -> None:
        self._rows = rows
        self.connection = _FakeConnection(rows)
        self.disposed = False

    def connect(self) -> object:
        return self.connection

    def dispose(self) -> None:
        self.disposed = True


class _FakeConnection:
    def __init__(self, rows: Sequence[Mapping[str, object]]) -> None:
        self._rows = rows
        self.executed: list[tuple[object, Mapping[str, object]]] = []

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: object, parameters: Mapping[str, object]) -> object:
        assert parameters["cycle_id"] == "CYCLE_20260416"
        if "payload_type" in parameters:
            assert parameters["payload_type"] == "Ex-3"
        else:
            assert parameters == {"cycle_id": "CYCLE_20260416"}
        self.executed.append((statement, dict(parameters)))
        return _FakeResult(self._rows)


class _FakeResult:
    def __init__(self, rows: Sequence[Mapping[str, object]]) -> None:
        self._rows = rows

    def mappings(self) -> "_FakeResult":
        return self

    def all(self) -> list[Mapping[str, object]]:
        return list(self._rows)


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


def _tushare_staging_row(ts_code: str, pct_chg: float) -> dict[str, object]:
    return {
        "ts_code": ts_code,
        "trade_date": date(2026, 4, 16),
        "close": 1700.0 if ts_code == "600519.SH" else 11.0,
        "pre_close": 1680.0 if ts_code == "600519.SH" else 11.1,
        "pct_chg": pct_chg,
        "vol": 1200.0,
        "amount": 2100.0,
        "daily_source_run_id": f"daily-run-{ts_code}",
        "daily_raw_loaded_at": "2026-04-16 16:00:00",
        "name": "Kweichow Moutai" if ts_code == "600519.SH" else "Ping An Bank",
        "industry": "liquor" if ts_code == "600519.SH" else "banking",
        "market": "main",
        "stock_basic_source_run_id": f"stock-basic-run-{ts_code}",
        "stock_basic_raw_loaded_at": "2026-04-16 16:01:00",
    }


def _canonical_input_row(entity_id: str, return_1d: float) -> dict[str, object]:
    return {
        "entity_id": entity_id,
        "trade_date": "2026-04-16",
        "close": 1700.0 if entity_id.endswith("600519.SH") else 11.0,
        "pre_close": 1680.0 if entity_id.endswith("600519.SH") else 11.1,
        "return_1d": return_1d,
        "volume": 1200.0 if entity_id.endswith("600519.SH") else 2200.0,
        "amount": 2100.0 if entity_id.endswith("600519.SH") else 3100.0,
        "market": "main",
        "industry": "liquor" if entity_id.endswith("600519.SH") else "banking",
        "canonical_dataset_refs": ["price_bar", "security_master"],
        "canonical_snapshot_ids": {"price_bar": 101, "security_master": 202},
        "lineage_refs": [
            "cycle:CYCLE_20260416",
            "selection:cycle_candidate_selection:CYCLE_20260416",
            f"candidate:{entity_id}",
            "canonical:price_bar@101",
            "canonical:security_master@202",
        ],
    }


def _ex3_graph_delta_payload(
    *,
    properties: Mapping[str, object] | None = None,
) -> dict[str, object]:
    return {
        "subsystem_id": "subsystem-news",
        "delta_id": "delta-ex3-bridge",
        "delta_type": "edge_add",
        "source_node": "ENT_STOCK_600519.SH",
        "target_node": "ENT_STOCK_000001.SZ",
        "relation_type": "supplier_of",
        "properties": dict(properties or {"impact_score": 0.91}),
        "evidence": ["evidence-ex3-bridge"],
    }


def _safe_ex3_graph_signal() -> dict[str, object]:
    return {
        "delta_id": "delta-ex3-bridge",
        "source_node": "ENT_STOCK_600519.SH",
        "target_node": "ENT_STOCK_000001.SZ",
        "relation_type": "supplier_of",
        "properties": {"impact_score": 0.91},
        "evidence_refs": ["evidence-ex3-bridge"],
        "cycle_id": "CYCLE_20260416",
        "candidate_id": 40,
        "selection_ref": "cycle_candidate_selection:CYCLE_20260416",
    }


def _no_unsafe_ex3_graph_signal_fields(value: object) -> bool:
    serialized = json.dumps(value, sort_keys=True, default=str).lower()
    return not any(
        marker in serialized
        for marker in (
            "raw_text",
            "chunk",
            "light_rag_artifact",
            "large_blob",
            "metadata",
            "private_note",
            "payload_type",
            "submitted_by",
            "ingest_seq",
            "validation_status",
            "rejection_reason",
        )
    )


def _no_source_specific_input_evidence(evidence: Mapping[str, object]) -> bool:
    serialized = json.dumps(evidence, sort_keys=True, default=str).lower()
    return not any(
        marker in serialized
        for marker in (
            "stg_daily",
            "stg_stock_basic",
            "tushare-staging",
            "doc_api",
            "source_run_id",
            "raw_loaded_at",
        )
    )


def _seed_tushare_staging_tables(connection: Any) -> None:
    connection.execute(
        """
        CREATE TABLE stg_daily (
            ts_code VARCHAR,
            trade_date DATE,
            close DOUBLE,
            pre_close DOUBLE,
            pct_chg DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            source_run_id VARCHAR,
            raw_loaded_at TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE stg_stock_basic (
            ts_code VARCHAR,
            name VARCHAR,
            industry VARCHAR,
            market VARCHAR,
            source_run_id VARCHAR,
            raw_loaded_at TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        INSERT INTO stg_daily VALUES
            ('600519.SH', DATE '2026-04-16', 1700.0, 1680.0, 1.2, 1200.0, 2100.0,
             'daily-run-600519.SH', TIMESTAMP '2026-04-16 16:00:00'),
            ('000001.SZ', DATE '2026-04-16', 11.0, 11.1, -0.4, 2200.0, 3100.0,
             'daily-run-000001.SZ', TIMESTAMP '2026-04-16 16:00:00')
        """
    )
    connection.execute(
        """
        INSERT INTO stg_stock_basic VALUES
            ('600519.SH', 'Kweichow Moutai', 'liquor', 'main',
             'stock-basic-run-600519.SH', TIMESTAMP '2026-04-16 16:01:00'),
            ('000001.SZ', 'Ping An Bank', 'banking', 'main',
             'stock-basic-run-000001.SZ', TIMESTAMP '2026-04-16 16:01:00')
        """
    )


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


def _sample_p2_audit_write_bundle(
    *,
    dagster_run_id: str = "dagster-run-CYCLE_20260416",
) -> object:
    from orchestrator_adapters.p2_dry_run import _build_audit_write_bundle

    cycle_id = "CYCLE_20260416"
    evidence = tuple(
        _sample_p2_layer_evidence(cycle_id=cycle_id, layer=layer)
        for layer in ("L4", "L6", "L7", "L8")
    )
    committed_objects = (
        SimpleNamespace(
            object_key="recommendation_snapshot",
            ref="data-platform://formal/recommendation_snapshot/snapshots/1001",
        ),
    )
    return _build_audit_write_bundle(
        cycle_id=cycle_id,
        evidence=evidence,
        committed_objects=committed_objects,
        dagster_run_id=dagster_run_id,
    )


def _sample_p2_layer_evidence(*, cycle_id: str, layer: str) -> object:
    from orchestrator_adapters.p2_dry_run import P2LayerEvidence

    sanitized_input = json.dumps({"cycle_id": cycle_id, "layer": layer}, sort_keys=True)
    raw_output = json.dumps({"ok": True, "layer": layer}, sort_keys=True)
    return P2LayerEvidence(
        layer=layer,
        object_ref=f"{layer.lower()}_formal_object",
        audit_record_id=f"audit-{cycle_id.lower()}-{layer.lower()}-formal-object",
        replay_record_id=f"replay-{cycle_id.lower()}-{layer.lower()}-formal-object",
        called_llm=layer in {"L4", "L6"},
        input_hash=hashlib.sha256(sanitized_input.encode()).hexdigest(),
        output_hash=hashlib.sha256(raw_output.encode()).hexdigest(),
        sanitized_input=sanitized_input,
        raw_output=raw_output,
        parsed_result={"ok": True, "layer": layer},
    )
