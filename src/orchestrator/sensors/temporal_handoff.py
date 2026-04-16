"""Temporal handoff sensor for successful Phase 0 Dagster runs."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from orchestrator.jobs.cycle import daily_cycle_job, daily_cycle_phase0_job
from orchestrator.policy import GatePolicyProfile
from orchestrator.temporal.handoff import (
    DefaultTemporalHandoffClient,
    TEMPORAL_HANDOFF_CLIENT_RESOURCE_KEY,
    TemporalFailoverMode,
    TemporalHandoffClient,
    TemporalHandoffStartError,
    start_temporal_handoff,
)


GATE_POLICY_RESOURCE_KEY = "gate_policy"
RESOURCE_BUNDLE_RESOURCE_KEY = "resource_bundle"
TEMPORAL_HANDOFF_SENSOR_NAME = "temporal_handoff_sensor"
TEMPORAL_HANDOFF_SENSOR_REQUIRED_RESOURCE_KEYS = frozenset(
    {
        GATE_POLICY_RESOURCE_KEY,
        RESOURCE_BUNDLE_RESOURCE_KEY,
    },
)
_TEMPORAL_HANDOFF_CURSOR_VERSION = 1
_TEMPORAL_WORKFLOW_ID_TAG = "temporal_workflow_id"


@dataclass(frozen=True, slots=True)
class _HandoffCursor:
    processed_run_ids: frozenset[str] = frozenset()
    high_water_timestamp: float | None = None


@dataclass(frozen=True, slots=True)
class _Phase0RunCandidate:
    raw_run: object
    run: object


def evaluate_temporal_handoff_sensor(
    *,
    policy: GatePolicyProfile,
    phase0_run_id: str,
    dagster_run_id: str,
    cycle_id: str,
    tags: Mapping[str, str],
    client: TemporalHandoffClient | None = None,
    failover_mode: TemporalFailoverMode = "failed_over",
    failover_job_name: str = "daily_cycle_job",
) -> RunRequest | SkipReason:
    """Evaluate a single successful Phase 0 run for Temporal handoff."""

    if policy.execution_backend != "dagster_plus_temporal":
        return SkipReason("temporal handoff disabled for dagster_only backend")

    active_client = client or DefaultTemporalHandoffClient(
        failover_mode=failover_mode,
    )
    try:
        result = start_temporal_handoff(
            policy=policy,
            phase0_run_id=phase0_run_id,
            dagster_run_id=dagster_run_id,
            cycle_id=cycle_id,
            tags=tags,
            client=active_client,
        )
    except TemporalHandoffStartError as exc:
        if failover_mode == "failed_over":
            return _failover_run_request(
                failover_job_name=failover_job_name,
                phase0_run_id=phase0_run_id,
                cycle_id=cycle_id,
                tags=tags,
                reason=str(exc),
            )
        return SkipReason(f"temporal handoff deferred: {exc}")

    if result.status == "started":
        workflow_id = result.workflow_id or "unknown-workflow"
        return SkipReason(f"temporal handoff started: {workflow_id}")
    if result.status == "deferred":
        return SkipReason(
            "temporal handoff deferred"
            + (f": {result.reason}" if result.reason else ""),
        )
    return _failover_run_request(
        failover_job_name=failover_job_name,
        phase0_run_id=phase0_run_id,
        cycle_id=cycle_id,
        tags=tags,
        reason=result.reason,
    )


def build_temporal_handoff_sensor(
    *,
    policy: GatePolicyProfile | None = None,
    phase0_job: object = daily_cycle_phase0_job,
    failover_job: object = daily_cycle_job,
    name: str = TEMPORAL_HANDOFF_SENSOR_NAME,
    failover_mode: TemporalFailoverMode = "failed_over",
) -> object:
    """Build the sensor that hands successful Phase 0 runs to Temporal."""

    phase0_job_name = _job_name(phase0_job)
    failover_job_name = _job_name(failover_job)

    @sensor(
        job=failover_job,
        name=name,
        required_resource_keys=TEMPORAL_HANDOFF_SENSOR_REQUIRED_RESOURCE_KEYS,
    )
    def _temporal_handoff_sensor(
        context: SensorEvaluationContext,
    ) -> RunRequest | SkipReason:
        cursor = _parse_handoff_cursor(context.cursor)
        candidate = _latest_successful_phase0_run(context, phase0_job_name, cursor)
        if candidate is None:
            return SkipReason(
                f"no successful {phase0_job_name} run is ready for Temporal handoff",
            )

        phase0_run = candidate.run
        phase0_run_id = _run_id(phase0_run)
        run_tags = _string_tags(_run_tags(phase0_run))
        cycle_id = _cycle_id_from_run(phase0_run, run_tags)
        result = evaluate_temporal_handoff_sensor(
            policy=policy or _gate_policy_from_context(context),
            phase0_run_id=phase0_run_id,
            dagster_run_id=phase0_run_id,
            cycle_id=cycle_id,
            tags=_temporal_handoff_tags(
                run_tags,
                cycle_id=cycle_id,
                phase0_run_id=phase0_run_id,
            ),
            client=_handoff_client_from_context(context),
            failover_mode=failover_mode,
            failover_job_name=failover_job_name,
        )
        if _handoff_result_consumed_phase0_run(result):
            context.update_cursor(
                _advance_handoff_cursor(
                    cursor,
                    raw_run=candidate.raw_run,
                    run=phase0_run,
                )
            )
        return result

    return _temporal_handoff_sensor


temporal_handoff_sensor = build_temporal_handoff_sensor()


def _failover_run_request(
    *,
    failover_job_name: str,
    phase0_run_id: str,
    cycle_id: str,
    tags: Mapping[str, str],
    reason: str | None,
) -> RunRequest:
    failover_tags = dict(tags)
    failover_tags.update(
        {
            "execution_backend": "dagster_only",
            "temporal_failover_of": phase0_run_id,
            "cycle_id": cycle_id,
        }
    )
    if reason:
        failover_tags.setdefault("temporal_handoff_reason", reason)

    return RunRequest(
        job_name=failover_job_name,
        run_key=f"temporal-failover:{phase0_run_id}",
        run_config={},
        tags=failover_tags,
    )


def _handoff_result_consumed_phase0_run(result: RunRequest | SkipReason) -> bool:
    if isinstance(result, RunRequest):
        return True

    message = getattr(result, "skip_message", "")
    return isinstance(message, str) and message.startswith("temporal handoff started")


def _latest_successful_phase0_run(
    context: SensorEvaluationContext,
    phase0_job_name: str,
    cursor: _HandoffCursor,
) -> _Phase0RunCandidate | None:
    instance = getattr(context, "instance", None)
    if instance is None:
        return None

    candidates: list[_Phase0RunCandidate] = []
    for raw_run in _query_runs(instance, phase0_job_name):
        run = _dagster_run(raw_run)
        if _run_job_name(run) != phase0_job_name:
            continue
        if not _run_succeeded(run):
            continue
        if _cursor_consumed_phase0_run(cursor, raw_run=raw_run, run=run):
            if cursor.high_water_timestamp is None:
                break
            continue
        candidates.append(_Phase0RunCandidate(raw_run=raw_run, run=run))

    if not candidates:
        return None

    candidates_with_timestamps = [
        candidate
        for candidate in candidates
        if _run_timestamp(candidate.raw_run, candidate.run) is not None
    ]
    if candidates_with_timestamps:
        return max(
            candidates_with_timestamps,
            key=lambda candidate: _run_timestamp(candidate.raw_run, candidate.run)
            or 0.0,
        )
    return candidates[0]


def _query_runs(instance: object, phase0_job_name: str) -> tuple[object, ...]:
    filters = _runs_filter(phase0_job_name)
    for method_name in ("get_run_records", "get_runs"):
        method = getattr(instance, method_name, None)
        if not callable(method):
            continue

        if filters is not None:
            for kwargs in (
                {"filters": filters, "limit": 25},
                {"run_filter": filters, "limit": 25},
                {"filters": filters},
                {"run_filter": filters},
            ):
                try:
                    return _as_tuple(method(**kwargs))
                except TypeError:
                    continue

        try:
            return _as_tuple(method(limit=25))
        except TypeError:
            try:
                return _as_tuple(method())
            except TypeError:
                continue

    return ()


def _runs_filter(phase0_job_name: str) -> object | None:
    try:
        from dagster import DagsterRunStatus, RunsFilter
    except Exception:
        return None

    try:
        return RunsFilter(
            job_name=phase0_job_name,
            statuses=[DagsterRunStatus.SUCCESS],
        )
    except TypeError:
        return None


def _dagster_run(raw_run: object) -> object:
    return _mapping_or_attr(raw_run, "dagster_run") or raw_run


def _run_id(run: object) -> str:
    run_id = _run_id_or_none(run)
    if run_id is None:
        raise TypeError("Phase 0 run must expose a non-empty run_id")
    return run_id


def _run_id_or_none(run: object) -> str | None:
    value = _mapping_or_attr(run, "run_id")
    return value if isinstance(value, str) and value else None


def _run_job_name(run: object) -> str | None:
    for attribute_name in ("job_name", "pipeline_name"):
        value = _mapping_or_attr(run, attribute_name)
        if isinstance(value, str) and value:
            return value
    return None


def _run_succeeded(run: object) -> bool:
    status = _mapping_or_attr(run, "status")
    if isinstance(status, str):
        return status.lower() in {"success", "succeeded"}

    for attribute_name in ("name", "value"):
        value = getattr(status, attribute_name, None)
        if isinstance(value, str) and value.lower() in {"success", "succeeded"}:
            return True
    return False


def _run_tags(run: object) -> Mapping[str, object]:
    tags = _mapping_or_attr(run, "tags")
    return tags if isinstance(tags, Mapping) else {}


def _cycle_id_from_run(run: object, tags: Mapping[str, str]) -> str:
    cycle_id = tags.get("cycle_id")
    if cycle_id:
        return cycle_id
    return _run_id(run)


def _parse_handoff_cursor(raw_cursor: str | None) -> _HandoffCursor:
    if not raw_cursor:
        return _HandoffCursor()

    try:
        decoded = json.loads(raw_cursor)
    except json.JSONDecodeError:
        return _HandoffCursor(processed_run_ids=frozenset({raw_cursor}))

    if not isinstance(decoded, Mapping):
        return _HandoffCursor(processed_run_ids=frozenset({raw_cursor}))

    processed_run_ids = {
        value
        for value in decoded.get("processed_run_ids", ())
        if isinstance(value, str) and value
    }
    last_run_id = decoded.get("last_run_id")
    if isinstance(last_run_id, str) and last_run_id:
        processed_run_ids.add(last_run_id)

    high_water_timestamp = decoded.get("high_water_timestamp")
    if isinstance(high_water_timestamp, bool):
        timestamp = None
    elif isinstance(high_water_timestamp, (int, float)):
        timestamp = float(high_water_timestamp)
    else:
        timestamp = None

    return _HandoffCursor(
        processed_run_ids=frozenset(processed_run_ids),
        high_water_timestamp=timestamp,
    )


def _advance_handoff_cursor(
    cursor: _HandoffCursor,
    *,
    raw_run: object,
    run: object,
) -> str:
    run_id = _run_id(run)
    processed_run_ids = set(cursor.processed_run_ids)
    processed_run_ids.add(run_id)

    run_timestamp = _run_timestamp(raw_run, run)
    high_water_timestamp = cursor.high_water_timestamp
    if run_timestamp is not None:
        high_water_timestamp = max(
            high_water_timestamp if high_water_timestamp is not None else run_timestamp,
            run_timestamp,
        )

    payload: dict[str, object] = {
        "version": _TEMPORAL_HANDOFF_CURSOR_VERSION,
        "last_run_id": run_id,
        "processed_run_ids": sorted(processed_run_ids),
    }
    if high_water_timestamp is not None:
        payload["high_water_timestamp"] = high_water_timestamp

    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _cursor_consumed_phase0_run(
    cursor: _HandoffCursor,
    *,
    raw_run: object,
    run: object,
) -> bool:
    run_id = _run_id_or_none(run)
    if run_id is not None and run_id in cursor.processed_run_ids:
        return True

    run_timestamp = _run_timestamp(raw_run, run)
    return (
        cursor.high_water_timestamp is not None
        and run_timestamp is not None
        and run_timestamp < cursor.high_water_timestamp
    )


def _run_timestamp(raw_run: object, run: object) -> float | None:
    for source in (raw_run, run):
        for attribute_name in (
            "create_timestamp",
            "start_time",
            "end_time",
            "update_timestamp",
            "timestamp",
        ):
            value = _mapping_or_attr(source, attribute_name)
            parsed = _timestamp_value(value)
            if parsed is not None:
                return parsed
    return None


def _timestamp_value(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    timestamp = getattr(value, "timestamp", None)
    if callable(timestamp):
        parsed = timestamp()
        if isinstance(parsed, (int, float)):
            return float(parsed)
    return None


def _temporal_handoff_tags(
    tags: Mapping[str, str],
    *,
    cycle_id: str,
    phase0_run_id: str,
) -> dict[str, str]:
    handoff_tags = dict(tags)
    handoff_tags.setdefault(
        _TEMPORAL_WORKFLOW_ID_TAG,
        _deterministic_temporal_workflow_id(
            cycle_id=cycle_id,
            phase0_run_id=phase0_run_id,
        ),
    )
    return handoff_tags


def _deterministic_temporal_workflow_id(
    *,
    cycle_id: str,
    phase0_run_id: str,
) -> str:
    return (
        "orchestrator-cycle-"
        f"{_workflow_id_component(cycle_id)}-"
        f"phase1-3-{_workflow_id_component(phase0_run_id)}"
    )


def _workflow_id_component(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in {"-", "_"} else "-"
        for character in value
    ).strip("-") or "unknown"


def _gate_policy_from_context(context: SensorEvaluationContext) -> GatePolicyProfile:
    resource = _resource_from_context(context, GATE_POLICY_RESOURCE_KEY)
    if isinstance(resource, GatePolicyProfile):
        return resource

    policy = getattr(resource, "policy", None)
    if isinstance(policy, GatePolicyProfile):
        return policy

    if resource is None:
        raise RuntimeError("gate_policy resource is required")

    raise TypeError(
        "gate_policy resource must be a GatePolicyProfile or expose "
        "a GatePolicyProfile policy",
    )


def _handoff_client_from_context(
    context: SensorEvaluationContext,
) -> TemporalHandoffClient | None:
    resource_bundle = _resource_from_context(context, RESOURCE_BUNDLE_RESOURCE_KEY)
    bundle_resources = getattr(resource_bundle, "resources", None)
    if isinstance(bundle_resources, Mapping):
        bundled = bundle_resources.get(TEMPORAL_HANDOFF_CLIENT_RESOURCE_KEY)
        if _is_handoff_client(bundled):
            return bundled

    direct = _resource_from_context(context, TEMPORAL_HANDOFF_CLIENT_RESOURCE_KEY)
    if _is_handoff_client(direct):
        return direct

    return None


def _is_handoff_client(value: object) -> bool:
    return callable(getattr(value, "start_cycle", None))


def _resource_from_context(context: SensorEvaluationContext, key: str) -> Any:
    resources = getattr(context, "resources", None)
    if resources is None:
        return None

    if isinstance(resources, Mapping):
        return resources.get(key)

    try:
        return getattr(resources, key)
    except AttributeError:
        return None


def _string_tags(tags: Mapping[str, object]) -> dict[str, str]:
    return {key: value for key, value in tags.items() if isinstance(value, str)}


def _mapping_or_attr(value: object, name: str) -> object | None:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _as_tuple(value: object) -> tuple[object, ...]:
    if value is None:
        return ()
    if isinstance(value, tuple):
        return value
    if isinstance(value, list):
        return tuple(value)
    return tuple(value) if hasattr(value, "__iter__") else (value,)


def _job_name(job: object) -> str:
    name = getattr(job, "name", None)
    if isinstance(name, str) and name:
        return name
    raise TypeError("Dagster job must expose a non-empty name")


__all__ = [
    "TEMPORAL_HANDOFF_CLIENT_RESOURCE_KEY",
    "TEMPORAL_HANDOFF_SENSOR_NAME",
    "TEMPORAL_HANDOFF_SENSOR_REQUIRED_RESOURCE_KEYS",
    "build_temporal_handoff_sensor",
    "evaluate_temporal_handoff_sensor",
    "temporal_handoff_sensor",
]
