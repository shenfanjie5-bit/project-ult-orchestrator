from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from orchestrator.policy import load_gate_policy
from orchestrator.temporal import (
    TemporalCycleRequest,
    TemporalHandoffResult,
    TemporalHandoffStartError,
    build_temporal_cycle_request_from_phase0_run,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"


class RecordingHandoffClient:
    def __init__(
        self,
        result: TemporalHandoffResult | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.result = result or TemporalHandoffResult(
            status="started",
            workflow_id="workflow-cycle-20260416",
            run_id="temporal-run-1",
        )
        self.error = error
        self.requests: list[TemporalCycleRequest] = []

    def start_cycle(self, request: TemporalCycleRequest) -> TemporalHandoffResult:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.result


@pytest.fixture
def temporal_sensor_exports() -> dict[str, Any]:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.sensors.temporal_handoff import (
        TEMPORAL_HANDOFF_CLIENT_RESOURCE_KEY,
        TEMPORAL_HANDOFF_SENSOR_REQUIRED_RESOURCE_KEYS,
        RESOURCE_BUNDLE_RESOURCE_KEY,
        build_temporal_handoff_sensor,
        evaluate_temporal_handoff_sensor,
    )

    return {
        "RunRequest": dagster.RunRequest,
        "SkipReason": dagster.SkipReason,
        "TEMPORAL_HANDOFF_CLIENT_RESOURCE_KEY": (
            TEMPORAL_HANDOFF_CLIENT_RESOURCE_KEY
        ),
        "TEMPORAL_HANDOFF_SENSOR_REQUIRED_RESOURCE_KEYS": (
            TEMPORAL_HANDOFF_SENSOR_REQUIRED_RESOURCE_KEYS
        ),
        "RESOURCE_BUNDLE_RESOURCE_KEY": RESOURCE_BUNDLE_RESOURCE_KEY,
        "build_temporal_handoff_sensor": build_temporal_handoff_sensor,
        "evaluate_temporal_handoff_sensor": evaluate_temporal_handoff_sensor,
    }


def test_temporal_cycle_request_uses_phase1_3_specs_and_preserves_metadata() -> None:
    policy = _temporal_policy()
    tags = {
        "cycle_id": "cycle-20260416",
        "scenario_id": "phase0_data_readiness_delayed",
        "runbook_url": "https://runbooks.example/phase0",
        "rerun_request_status": "none",
    }

    request = build_temporal_cycle_request_from_phase0_run(
        policy=policy,
        cycle_id="cycle-20260416",
        phase0_run_id="phase0-run-1",
        dagster_run_id="dagster-run-1",
        tags=tags,
    )

    assert request.cycle_id == "cycle-20260416"
    assert request.phase0_run_id == "phase0-run-1"
    assert request.dagster_run_id == "dagster-run-1"
    assert request.policy_version == policy.policy_version
    assert request.contract_version == policy.contract_version
    assert [spec.phase.value for spec in request.phase_specs] == [
        "phase1",
        "phase2",
        "phase3",
    ]
    assert all("phase0" not in spec.asset_selection for spec in request.phase_specs)
    assert request.tags["scenario_id"] == "phase0_data_readiness_delayed"
    assert request.tags["runbook_url"] == "https://runbooks.example/phase0"
    assert request.tags["rerun_request_status"] == "none"


def test_started_handoff_does_not_emit_failover_run_request(
    temporal_sensor_exports: dict[str, Any],
) -> None:
    SkipReason = temporal_sensor_exports["SkipReason"]
    evaluate_temporal_handoff_sensor = temporal_sensor_exports[
        "evaluate_temporal_handoff_sensor"
    ]
    client = RecordingHandoffClient()

    result = evaluate_temporal_handoff_sensor(
        policy=_temporal_policy(),
        phase0_run_id="phase0-run-1",
        dagster_run_id="dagster-run-1",
        cycle_id="cycle-20260416",
        tags=_tags(),
        client=client,
    )

    assert isinstance(result, SkipReason)
    assert "started" in result.skip_message
    assert len(client.requests) == 1
    assert client.requests[0].phase0_run_id == "phase0-run-1"


def test_failed_over_handoff_emits_dagster_only_run_request(
    temporal_sensor_exports: dict[str, Any],
) -> None:
    RunRequest = temporal_sensor_exports["RunRequest"]
    evaluate_temporal_handoff_sensor = temporal_sensor_exports[
        "evaluate_temporal_handoff_sensor"
    ]
    client = RecordingHandoffClient(
        TemporalHandoffResult(status="failed_over", reason="Temporal unavailable"),
    )

    result = evaluate_temporal_handoff_sensor(
        policy=_temporal_policy(),
        phase0_run_id="phase0-run-1",
        dagster_run_id="dagster-run-1",
        cycle_id="cycle-20260416",
        tags=_tags(),
        client=client,
    )

    assert isinstance(result, RunRequest)
    assert result.job_name == "daily_cycle_job"
    assert result.tags["execution_backend"] == "dagster_only"
    assert result.tags["temporal_failover_of"] == "phase0-run-1"
    assert result.tags["cycle_id"] == "cycle-20260416"
    assert result.tags["scenario_id"] == "phase0_data_readiness_delayed"


def test_handoff_start_error_emits_failover_when_enabled(
    temporal_sensor_exports: dict[str, Any],
) -> None:
    RunRequest = temporal_sensor_exports["RunRequest"]
    evaluate_temporal_handoff_sensor = temporal_sensor_exports[
        "evaluate_temporal_handoff_sensor"
    ]
    client = RecordingHandoffClient(error=TemporalHandoffStartError("start failed"))

    result = evaluate_temporal_handoff_sensor(
        policy=_temporal_policy(),
        phase0_run_id="phase0-run-1",
        dagster_run_id="dagster-run-1",
        cycle_id="cycle-20260416",
        tags=_tags(),
        client=client,
    )

    assert isinstance(result, RunRequest)
    assert result.job_name == "daily_cycle_job"
    assert result.tags["temporal_failover_of"] == "phase0-run-1"
    assert result.tags["temporal_handoff_reason"] == "start failed"


def test_dagster_only_policy_skips_temporal_handoff(
    temporal_sensor_exports: dict[str, Any],
) -> None:
    SkipReason = temporal_sensor_exports["SkipReason"]
    evaluate_temporal_handoff_sensor = temporal_sensor_exports[
        "evaluate_temporal_handoff_sensor"
    ]

    result = evaluate_temporal_handoff_sensor(
        policy=load_gate_policy(LITE_POLICY_PATH),
        phase0_run_id="phase0-run-1",
        dagster_run_id="dagster-run-1",
        cycle_id="cycle-20260416",
        tags=_tags(),
        client=RecordingHandoffClient(),
    )

    assert isinstance(result, SkipReason)
    assert "dagster_only" in result.skip_message


def test_missing_handoff_client_defaults_to_failed_over(
    temporal_sensor_exports: dict[str, Any],
) -> None:
    RunRequest = temporal_sensor_exports["RunRequest"]
    evaluate_temporal_handoff_sensor = temporal_sensor_exports[
        "evaluate_temporal_handoff_sensor"
    ]

    result = evaluate_temporal_handoff_sensor(
        policy=_temporal_policy(),
        phase0_run_id="phase0-run-1",
        dagster_run_id="dagster-run-1",
        cycle_id="cycle-20260416",
        tags=_tags(),
        client=None,
    )

    assert isinstance(result, RunRequest)
    assert result.tags["execution_backend"] == "dagster_only"
    assert "not configured" in result.tags["temporal_handoff_reason"]


def test_temporal_handoff_sensor_declares_client_discovery_resources(
    temporal_sensor_exports: dict[str, Any],
) -> None:
    required_resource_keys = temporal_sensor_exports[
        "TEMPORAL_HANDOFF_SENSOR_REQUIRED_RESOURCE_KEYS"
    ]

    assert "gate_policy" in required_resource_keys
    assert (
        temporal_sensor_exports["RESOURCE_BUNDLE_RESOURCE_KEY"]
        in required_resource_keys
    )
    assert (
        temporal_sensor_exports["TEMPORAL_HANDOFF_CLIENT_RESOURCE_KEY"]
        in required_resource_keys
    )


def test_temporal_handoff_sensor_uses_client_from_resource_bundle_and_cursors_run(
    temporal_sensor_exports: dict[str, Any],
) -> None:
    SkipReason = temporal_sensor_exports["SkipReason"]
    build_temporal_handoff_sensor = temporal_sensor_exports[
        "build_temporal_handoff_sensor"
    ]
    client = RecordingHandoffClient()
    context = _FakeTemporalSensorContext(
        resources={
            "resource_bundle": SimpleNamespace(
                resources={"temporal_handoff_client": client},
            ),
        },
        runs=[
            _phase0_run_record("phase0-run-2", "cycle-20260417", timestamp=2.0),
            _phase0_run_record("phase0-run-1", "cycle-20260416", timestamp=1.0),
        ],
    )
    sensor_definition = build_temporal_handoff_sensor(policy=_temporal_policy())

    result = sensor_definition.evaluation_fn(context)

    assert isinstance(result, SkipReason)
    assert "started" in result.skip_message
    assert [request.phase0_run_id for request in client.requests] == ["phase0-run-2"]
    assert client.requests[0].tags["temporal_workflow_id"] == (
        "orchestrator-cycle-cycle-20260417-phase1-3-phase0-run-2"
    )
    assert context.updated_cursor is not None
    cursor = json.loads(context.updated_cursor)
    assert cursor["last_run_id"] == "phase0-run-2"
    assert cursor["processed_run_ids"] == ["phase0-run-2"]
    assert cursor["high_water_timestamp"] == 2.0


def test_temporal_handoff_sensor_cursor_does_not_reprocess_older_run(
    temporal_sensor_exports: dict[str, Any],
) -> None:
    SkipReason = temporal_sensor_exports["SkipReason"]
    build_temporal_handoff_sensor = temporal_sensor_exports[
        "build_temporal_handoff_sensor"
    ]
    client = RecordingHandoffClient()
    cursor = json.dumps(
        {
            "version": 1,
            "last_run_id": "phase0-run-2",
            "processed_run_ids": ["phase0-run-2"],
            "high_water_timestamp": 2.0,
        }
    )
    context = _FakeTemporalSensorContext(
        cursor=cursor,
        resources={
            "resource_bundle": SimpleNamespace(
                resources={"temporal_handoff_client": client},
            ),
        },
        runs=[
            _phase0_run_record("phase0-run-2", "cycle-20260417", timestamp=2.0),
            _phase0_run_record("phase0-run-1", "cycle-20260416", timestamp=1.0),
        ],
    )
    sensor_definition = build_temporal_handoff_sensor(policy=_temporal_policy())

    result = sensor_definition.evaluation_fn(context)

    assert isinstance(result, SkipReason)
    assert "no successful daily_cycle_phase0_job run is ready" in result.skip_message
    assert client.requests == []
    assert context.updated_cursor is None


def test_temporal_handoff_sensor_processes_unseen_run_at_same_timestamp(
    temporal_sensor_exports: dict[str, Any],
) -> None:
    SkipReason = temporal_sensor_exports["SkipReason"]
    build_temporal_handoff_sensor = temporal_sensor_exports[
        "build_temporal_handoff_sensor"
    ]
    client = RecordingHandoffClient()
    context = _FakeTemporalSensorContext(
        resources={
            "resource_bundle": SimpleNamespace(
                resources={"temporal_handoff_client": client},
            ),
        },
        runs=[
            _phase0_run_record("phase0-run-2", "cycle-20260417", timestamp=2.0),
            _phase0_run_record("phase0-run-1", "cycle-20260416", timestamp=2.0),
        ],
    )
    sensor_definition = build_temporal_handoff_sensor(policy=_temporal_policy())

    first_result = sensor_definition.evaluation_fn(context)
    second_result = sensor_definition.evaluation_fn(context)
    third_result = sensor_definition.evaluation_fn(context)

    assert isinstance(first_result, SkipReason)
    assert isinstance(second_result, SkipReason)
    assert isinstance(third_result, SkipReason)
    assert [request.phase0_run_id for request in client.requests] == [
        "phase0-run-2",
        "phase0-run-1",
    ]
    assert "no successful daily_cycle_phase0_job run is ready" in (
        third_result.skip_message
    )
    assert context.updated_cursor is not None
    cursor = json.loads(context.updated_cursor)
    assert cursor["processed_run_ids"] == ["phase0-run-1", "phase0-run-2"]
    assert cursor["high_water_timestamp"] == 2.0


def test_temporal_handoff_sensor_legacy_cursor_stops_at_processed_run(
    temporal_sensor_exports: dict[str, Any],
) -> None:
    SkipReason = temporal_sensor_exports["SkipReason"]
    build_temporal_handoff_sensor = temporal_sensor_exports[
        "build_temporal_handoff_sensor"
    ]
    client = RecordingHandoffClient()
    context = _FakeTemporalSensorContext(
        cursor="phase0-run-2",
        resources={
            "resource_bundle": SimpleNamespace(
                resources={"temporal_handoff_client": client},
            ),
        },
        runs=[
            _phase0_run_record("phase0-run-2", "cycle-20260417", timestamp=2.0),
            _phase0_run_record("phase0-run-1", "cycle-20260416", timestamp=1.0),
        ],
    )
    sensor_definition = build_temporal_handoff_sensor(policy=_temporal_policy())

    result = sensor_definition.evaluation_fn(context)

    assert isinstance(result, SkipReason)
    assert "no successful daily_cycle_phase0_job run is ready" in result.skip_message
    assert client.requests == []
    assert context.updated_cursor is None


def _temporal_policy() -> Any:
    policy = load_gate_policy(LITE_POLICY_PATH)
    return policy.model_copy(update={"execution_backend": "dagster_plus_temporal"})


def _tags() -> Mapping[str, str]:
    return {
        "cycle_id": "cycle-20260416",
        "scenario_id": "phase0_data_readiness_delayed",
        "runbook_url": "https://runbooks.example/phase0",
    }


def _phase0_run_record(
    run_id: str,
    cycle_id: str,
    *,
    timestamp: float,
) -> Mapping[str, object]:
    return {
        "create_timestamp": timestamp,
        "dagster_run": {
            "run_id": run_id,
            "job_name": "daily_cycle_phase0_job",
            "status": "SUCCESS",
            "tags": _tags() | {"cycle_id": cycle_id},
        },
    }


class _FakeRunStorage:
    def __init__(self, runs: list[object]) -> None:
        self.runs = runs

    def get_run_records(self, **_kwargs: object) -> tuple[object, ...]:
        return tuple(self.runs)


class _FakeTemporalSensorContext:
    def __init__(
        self,
        *,
        runs: list[object],
        resources: Mapping[str, object],
        cursor: str | None = None,
    ) -> None:
        self.cursor = cursor
        self.updated_cursor: str | None = None
        self.instance = _FakeRunStorage(runs)
        self.resources = SimpleNamespace(**resources)

    def update_cursor(self, cursor: str) -> None:
        self.updated_cursor = cursor
        self.cursor = cursor
