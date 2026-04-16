"""Adapters for dbt test failure events emitted through Dagster."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.decision_handler import dispatch_gate_decision_alert
from orchestrator.checks.models import GateDecision
from orchestrator.policy import FailureClass, GateAction, GatePolicyProfile, PhaseEnum
from orchestrator.rerun import (
    PartialRerunPlan,
    RunHistorySnapshot,
    compute_partial_rerun_plan,
)
from orchestrator.rerun_request import DEFAULT_REQUEST_DIR, write_rerun_request

_RERUN_REQUEST_DIR_ENV = "ORCHESTRATOR_RERUN_REQUEST_DIR"
_FAILED_DBT_RESULT_STATUSES = frozenset({"error", "fail"})
_DEFAULT_FAILED_DBT_ASSET_KEY = "dbt_test_failure"


@dataclass(frozen=True, slots=True)
class _DbtGateEvent:
    failure_class: FailureClass
    dbt_node_name: str | None = None
    asset_key: str | None = None


@dataclass(frozen=True, slots=True)
class DbtGateHandlingResult:
    """Side-effect result for one failed dbt test gate event."""

    event: _DbtGateEvent
    decision: GateDecision
    plan: PartialRerunPlan | None = None
    request_path: Path | None = None
    request_write_error: str | None = None
    plan_error: str | None = None


@dataclass(frozen=True, slots=True)
class _FallbackAssetObservation:
    asset_key: str
    metadata: Mapping[str, object]
    description: str


def classify_dbt_test_failure(
    event: object,
    policy: GatePolicyProfile,
) -> GateDecision:
    """Classify a dbt test failure as the Phase 0 task-level gate outcome."""

    gate_event = _dbt_test_failure_gate_event(event)

    return classify_gate_result(PhaseEnum.PHASE0, gate_event, policy)


def plan_dbt_test_partial_rerun(
    run_id: str,
    failed_asset_key: object,
    event: object,
    policy: GatePolicyProfile,
) -> PartialRerunPlan:
    """Plan the minimal rerun selection for a failed Phase 0 dbt asset."""

    gate_event = _dbt_test_failure_gate_event(event)
    requested_failed_node = _stable_asset_key_string(failed_asset_key)
    failed_node = _validated_failed_node_from_event(
        requested_failed_node,
        gate_event.asset_key,
    )
    run_history = RunHistorySnapshot(
        run_id=run_id,
        node_to_phase={failed_node: PhaseEnum.PHASE0},
        node_dependencies={failed_node: ()},
        failed_nodes=(failed_node,),
        repairable_nodes={},
        node_failure_classes={failed_node: FailureClass.TASK_LEVEL},
    )

    return compute_partial_rerun_plan(run_id, failed_node, run_history, policy)


def stream_dbt_events_with_gate_handling(
    *,
    context: object,
    dbt_invocation: object,
    policy: GatePolicyProfile,
    request_dir: str | Path | None = None,
) -> Iterator[object]:
    """Stream dbt events and route failed dbt tests through the gate pipeline."""

    failed_events: list[_DbtGateEvent] = []
    stream_error: BaseException | None = None
    stream_traceback: object | None = None

    try:
        for event in dbt_invocation.stream():
            failed_events.extend(_failed_dbt_test_gate_events_from_stream_event(event))
            yield event
    except Exception as exc:
        stream_error = exc
        stream_traceback = exc.__traceback__

    failed_events.extend(_failed_dbt_test_gate_events_from_artifacts(dbt_invocation))
    handling_results = handle_dbt_test_failures(
        failed_events,
        run_id=_run_id_from_context(context),
        policy=policy,
        request_dir=request_dir,
        cycle_id=_cycle_id_from_context(context),
    )
    for result in handling_results:
        yield _build_gate_observation(result)

    if stream_error is not None:
        write_errors = tuple(
            result.request_write_error
            for result in handling_results
            if result.request_write_error
        )
        if write_errors and hasattr(stream_error, "add_note"):
            stream_error.add_note(
                "orchestrator rerun request write error(s): "
                + "; ".join(write_errors),
            )
        raise stream_error.with_traceback(stream_traceback)


def handle_dbt_test_failures(
    events: Sequence[object],
    *,
    run_id: str,
    policy: GatePolicyProfile,
    request_dir: str | Path | None = None,
    cycle_id: str | None = None,
) -> tuple[DbtGateHandlingResult, ...]:
    """Classify failed dbt test events, alert, and persist rerun request files."""

    handling_results: list[DbtGateHandlingResult] = []
    for gate_event in _unique_dbt_gate_events(events):
        decision = classify_gate_result(PhaseEnum.PHASE0, gate_event, policy)
        plan, plan_error = _plan_dbt_gate_rerun(
            run_id,
            gate_event,
            event=gate_event,
            policy=policy,
        )
        request_path, request_write_error = _write_dbt_rerun_request_if_needed(
            decision,
            plan,
            request_dir,
        )
        result = DbtGateHandlingResult(
            event=gate_event,
            decision=decision,
            plan=plan,
            request_path=request_path,
            request_write_error=request_write_error,
            plan_error=plan_error,
        )
        dispatch_gate_decision_alert(
            decision,
            cycle_id=cycle_id or run_id,
            failed_node=gate_event.asset_key,
            summary=_alert_summary(result),
            channels=policy.alert_channels,
        )
        handling_results.append(result)

    return tuple(handling_results)


def _plan_dbt_gate_rerun(
    run_id: str,
    gate_event: _DbtGateEvent,
    *,
    event: object,
    policy: GatePolicyProfile,
) -> tuple[PartialRerunPlan | None, str | None]:
    if gate_event.asset_key is None:
        return None, "dbt failure event asset_key metadata is required"

    try:
        return (
            plan_dbt_test_partial_rerun(
                run_id,
                gate_event.asset_key,
                event,
                policy,
            ),
            None,
        )
    except Exception as exc:
        return None, str(exc)


def _write_dbt_rerun_request_if_needed(
    decision: GateDecision,
    plan: PartialRerunPlan | None,
    request_dir: str | Path | None,
) -> tuple[Path | None, str | None]:
    if decision.action is not GateAction.PARTIAL_RERUN or plan is None:
        return None, None

    try:
        return _write_rerun_request(plan, _request_dir(request_dir)), None
    except OSError as exc:
        return None, str(exc)


def _write_rerun_request(plan: PartialRerunPlan, request_dir: Path) -> Path:
    return write_rerun_request(plan, request_dir)


def _request_dir(request_dir: str | Path | None) -> Path:
    if request_dir is not None:
        return Path(request_dir)
    return Path(os.environ.get(_RERUN_REQUEST_DIR_ENV, DEFAULT_REQUEST_DIR))


def _alert_summary(result: DbtGateHandlingResult) -> str:
    summary = result.decision.reason or "dbt test failed"
    details: list[str] = []
    if result.plan_error:
        details.append(f"rerun plan failed: {result.plan_error}")
    if result.request_write_error:
        details.append(f"rerun request write failed: {result.request_write_error}")
    if details:
        return f"{summary} ({'; '.join(details)})"
    return summary


def _build_gate_observation(result: DbtGateHandlingResult) -> object:
    asset_key = result.event.asset_key or _DEFAULT_FAILED_DBT_ASSET_KEY
    metadata = _gate_observation_metadata(result)
    description = "dbt test failure gate decision"

    try:
        from dagster import AssetKey, AssetObservation

        return AssetObservation(
            asset_key=AssetKey.from_user_string(asset_key),
            metadata=metadata,
            description=description,
        )
    except ModuleNotFoundError:
        return _FallbackAssetObservation(
            asset_key=asset_key,
            metadata=metadata,
            description=description,
        )


def _gate_observation_metadata(
    result: DbtGateHandlingResult,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "phase": result.decision.phase.value,
        "action": result.decision.action.value,
        "failure_class": (
            result.decision.failure_class.value
            if result.decision.failure_class
            else ""
        ),
        "reason": result.decision.reason or "",
        "dbt_node_name": result.event.dbt_node_name or "",
        "failed_node": result.event.asset_key or "",
    }
    if result.plan is not None:
        metadata["rerun_selection"] = json.dumps(list(result.plan.rerun_selection))
        metadata["rerun_mode"] = result.plan.rerun_mode
        metadata["requires_manual_ack"] = result.plan.requires_manual_ack
    if result.request_path is not None:
        metadata["rerun_request_path"] = str(result.request_path)
    if result.request_write_error:
        metadata["rerun_request_write_error"] = result.request_write_error
    if result.plan_error:
        metadata["rerun_plan_error"] = result.plan_error
    return metadata


def _failed_dbt_test_gate_events_from_stream_event(
    event: object,
) -> tuple[_DbtGateEvent, ...]:
    gate_events: list[_DbtGateEvent] = []
    for candidate in _event_and_nested_check_candidates(event):
        if not _is_failed_check_candidate(candidate):
            continue
        gate_events.append(_dbt_test_failure_gate_event(candidate))

    if gate_events:
        return tuple(gate_events)

    if _is_failed_dbt_result_mapping(event):
        return (_dbt_test_failure_gate_event(event),)

    return ()


def _failed_dbt_test_gate_events_from_artifacts(
    dbt_invocation: object,
) -> tuple[_DbtGateEvent, ...]:
    run_results = _dbt_artifact(dbt_invocation, "run_results.json")
    if not isinstance(run_results, Mapping):
        return ()

    manifest = _dbt_artifact(dbt_invocation, "manifest.json")
    results = run_results.get("results")
    if not isinstance(results, Sequence) or isinstance(
        results,
        (str, bytes, bytearray),
    ):
        return ()

    gate_events: list[_DbtGateEvent] = []
    for result in results:
        if not isinstance(result, Mapping):
            continue
        status = result.get("status")
        if (
            not isinstance(status, str)
            or status.lower() not in _FAILED_DBT_RESULT_STATUSES
        ):
            continue
        unique_id = result.get("unique_id")
        if not isinstance(unique_id, str) or not _is_dbt_test_unique_id(
            unique_id,
            manifest,
        ):
            continue

        dbt_node_name = _dbt_test_name_from_artifact(unique_id, manifest)
        asset_key = _asset_key_from_dbt_test_artifact(unique_id, manifest)
        gate_events.append(
            _DbtGateEvent(
                failure_class=FailureClass.TASK_LEVEL,
                dbt_node_name=dbt_node_name,
                asset_key=asset_key,
            ),
        )

    return tuple(gate_events)


def _dbt_artifact(dbt_invocation: object, artifact_name: str) -> object | None:
    get_artifact = getattr(dbt_invocation, "get_artifact", None)
    if not callable(get_artifact):
        return None

    try:
        return get_artifact(artifact_name)
    except Exception:
        return None


def _is_dbt_test_unique_id(unique_id: str, manifest: object | None) -> bool:
    node = _manifest_node(manifest, unique_id)
    resource_type = node.get("resource_type") if node else None
    return resource_type == "test" or unique_id.startswith("test.")


def _dbt_test_name_from_artifact(
    unique_id: str,
    manifest: object | None,
) -> str:
    node = _manifest_node(manifest, unique_id)
    if node is not None:
        name = node.get("name")
        if isinstance(name, str) and name:
            return name
    return unique_id


def _asset_key_from_dbt_test_artifact(
    unique_id: str,
    manifest: object | None,
) -> str | None:
    test_node = _manifest_node(manifest, unique_id)
    if test_node is None:
        return None

    depends_on = test_node.get("depends_on")
    if not isinstance(depends_on, Mapping):
        return None
    dependency_ids = depends_on.get("nodes")
    if not isinstance(dependency_ids, Sequence) or isinstance(
        dependency_ids,
        (str, bytes, bytearray),
    ):
        return None

    for dependency_id in dependency_ids:
        if not isinstance(dependency_id, str):
            continue
        if dependency_id.startswith("test."):
            continue
        asset_key = _asset_key_from_manifest_node(manifest, dependency_id)
        if asset_key:
            return asset_key
    return None


def _asset_key_from_manifest_node(
    manifest: object | None,
    unique_id: str,
) -> str | None:
    node = _manifest_node(manifest, unique_id)
    if node is None:
        return None

    dagster_meta = _dagster_meta_from_manifest_node(node)
    explicit_asset_key = dagster_meta.get("asset_key")
    try:
        if explicit_asset_key is not None:
            return _stable_asset_key_string(explicit_asset_key)
    except (TypeError, ValueError):
        pass

    for field in ("alias", "name"):
        value = node.get(field)
        if isinstance(value, str) and value:
            return value
    return None


def _dagster_meta_from_manifest_node(
    node: Mapping[str, object],
) -> Mapping[str, object]:
    for container in (node.get("meta"), _mapping_value(node.get("config")).get("meta")):
        container_mapping = _mapping_value(container)
        dagster_meta = container_mapping.get("dagster")
        if isinstance(dagster_meta, Mapping):
            return dagster_meta
    return {}


def _mapping_value(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _manifest_node(
    manifest: object | None,
    unique_id: str,
) -> Mapping[str, object] | None:
    if not isinstance(manifest, Mapping):
        return None
    nodes = manifest.get("nodes")
    if not isinstance(nodes, Mapping):
        return None
    node = nodes.get(unique_id)
    return node if isinstance(node, Mapping) else None


def _event_and_nested_check_candidates(event: object) -> tuple[object, ...]:
    candidates: list[object] = [event]
    event_specific_data = _mapping_or_attr(event, "event_specific_data")
    if event_specific_data is not None:
        for name in ("asset_check_evaluation", "evaluation"):
            candidate = _mapping_or_attr(event_specific_data, name)
            if candidate is not None:
                candidates.append(candidate)

    asset_check_evaluation = _mapping_or_attr(event, "asset_check_evaluation")
    if asset_check_evaluation is not None:
        candidates.append(asset_check_evaluation)

    return tuple(candidates)


def _is_failed_check_candidate(candidate: object) -> bool:
    passed = _mapping_or_attr(candidate, "passed")
    if passed is not False:
        return False

    if _extract_asset_key(candidate) is None:
        return False

    return _looks_like_dbt_test_candidate(candidate)


def _looks_like_dbt_test_candidate(candidate: object) -> bool:
    node_name = _extract_dbt_node_name(candidate)
    if node_name and (
        "test." in node_name
        or node_name.startswith(
            ("not_null_", "unique_", "accepted_values_", "relationships_"),
        )
    ):
        return True

    metadata = _event_metadata(candidate)
    for key in ("unique_id", "dbt_unique_id", "dagster_dbt/unique_id"):
        value = metadata.get(key)
        if isinstance(value, str) and value.startswith("test."):
            return True

    return node_name is not None


def _is_failed_dbt_result_mapping(event: object) -> bool:
    if not isinstance(event, Mapping):
        return False
    status = event.get("status")
    if not isinstance(status, str) or status.lower() not in _FAILED_DBT_RESULT_STATUSES:
        return False
    resource_type = event.get("resource_type")
    unique_id = event.get("unique_id")
    return resource_type == "test" or (
        isinstance(unique_id, str) and unique_id.startswith("test.")
    )


def _unique_dbt_gate_events(events: Sequence[object]) -> tuple[_DbtGateEvent, ...]:
    output: list[_DbtGateEvent] = []
    seen: set[str] = set()
    for event in events:
        gate_event = (
            event
            if isinstance(event, _DbtGateEvent)
            else _dbt_test_failure_gate_event(event)
        )
        dedupe_key = gate_event.asset_key or gate_event.dbt_node_name
        if dedupe_key is None:
            dedupe_key = f"event:{len(output)}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        output.append(gate_event)
    return tuple(output)


def _run_id_from_context(context: object) -> str:
    run_id = _mapping_or_attr(context, "run_id")
    if isinstance(run_id, str) and run_id:
        return run_id
    run = _mapping_or_attr(context, "run")
    run_id = _mapping_or_attr(run, "run_id") if run is not None else None
    if isinstance(run_id, str) and run_id:
        return run_id
    return "unknown-dbt-run"


def _cycle_id_from_context(context: object) -> str | None:
    tags = _context_tags(context)
    value = tags.get("cycle_id")
    return value if isinstance(value, str) and value else None


def _context_tags(context: object) -> Mapping[str, object]:
    for value in (
        _mapping_or_attr(context, "run_tags"),
        _mapping_or_attr(context, "tags"),
    ):
        if isinstance(value, Mapping):
            return value

    run = _mapping_or_attr(context, "run")
    tags = _mapping_or_attr(run, "tags") if run is not None else None
    return tags if isinstance(tags, Mapping) else {}


def _validated_failed_node_from_event(
    requested_failed_node: str,
    event_asset_key: str | None,
) -> str:
    if event_asset_key is None:
        msg = "dbt failure event asset_key metadata is required"
        raise ValueError(msg)

    if event_asset_key != requested_failed_node:
        msg = (
            "dbt failure event asset_key does not match failed_asset_key: "
            f"event={event_asset_key} requested={requested_failed_node}"
        )
        raise ValueError(msg)

    return event_asset_key


def _dbt_test_failure_gate_event(event: object) -> _DbtGateEvent:
    if event is None:
        raise TypeError("dbt test failure event is required")

    return _DbtGateEvent(
        failure_class=FailureClass.TASK_LEVEL,
        dbt_node_name=_extract_dbt_node_name(event),
        asset_key=_extract_asset_key(event),
    )


def _extract_dbt_node_name(event: object) -> str | None:
    for value in _candidate_values(
        event,
        (
            "dbt_node_name",
            "node_name",
            "unique_id",
            "dbt_unique_id",
            "check_name",
            "test_name",
        ),
    ):
        if isinstance(value, str) and value:
            return value

    metadata = _event_metadata(event)
    for key in ("dbt_node_info", "node_info"):
        node_info = metadata.get(key)
        if isinstance(node_info, Mapping):
            for field in ("node_name", "unique_id", "name"):
                value = node_info.get(field)
                if isinstance(value, str) and value:
                    return value

    for key in (
        "dbt_node_name",
        "node_name",
        "unique_id",
        "dbt_unique_id",
        "dagster_dbt/unique_id",
        "check_name",
        "test_name",
    ):
        value = metadata.get(key)
        if isinstance(value, str) and value:
            return value

    return None


def _extract_asset_key(event: object) -> str | None:
    for value in _candidate_values(event, ("asset_key", "asset_key_path")):
        try:
            return _stable_asset_key_string(value)
        except (TypeError, ValueError):
            continue

    metadata = _event_metadata(event)
    for key in ("asset_key", "asset_key_path"):
        if key not in metadata:
            continue
        try:
            return _stable_asset_key_string(metadata[key])
        except (TypeError, ValueError):
            continue

    return None


def _event_metadata(event: object) -> Mapping[str, object]:
    metadata = _mapping_or_attr(event, "metadata")
    if isinstance(metadata, Mapping):
        return {
            str(key): _plain_metadata_value(value)
            for key, value in metadata.items()
        }

    event_specific_data = _mapping_or_attr(event, "event_specific_data")
    if event_specific_data is not None:
        nested_metadata = _mapping_or_attr(event_specific_data, "metadata")
        if isinstance(nested_metadata, Mapping):
            return {
                str(key): _plain_metadata_value(value)
                for key, value in nested_metadata.items()
            }

    return {}


def _candidate_values(event: object, names: Sequence[str]) -> tuple[object, ...]:
    values: list[object] = []
    for name in names:
        value = _mapping_or_attr(event, name)
        if value is not None:
            values.append(value)

    dagster_event = _mapping_or_attr(event, "dagster_event")
    if dagster_event is not None:
        for name in names:
            value = _mapping_or_attr(dagster_event, name)
            if value is not None:
                values.append(value)

    return tuple(values)


def _mapping_or_attr(value: object, name: str) -> object | None:
    if isinstance(value, Mapping):
        return _plain_metadata_value(value.get(name))
    return _plain_metadata_value(getattr(value, name, None))


def _plain_metadata_value(value: object) -> object:
    if isinstance(value, (str, int, float, bool, list, tuple, dict)) or value is None:
        return value
    for attribute_name in ("value", "text", "data", "path"):
        attribute = getattr(value, attribute_name, None)
        if isinstance(attribute, (str, int, float, bool, list, tuple, dict)):
            return attribute
    return value


def _stable_asset_key_string(asset_key: object) -> str:
    if isinstance(asset_key, str):
        if not asset_key:
            raise ValueError("failed_asset_key is required")
        return asset_key

    to_user_string = getattr(asset_key, "to_user_string", None)
    if callable(to_user_string):
        value = to_user_string()
        if isinstance(value, str) and value:
            return value

    path = _asset_key_path(asset_key)
    if path:
        return "/".join(path)

    to_string = getattr(asset_key, "to_string", None)
    if callable(to_string):
        value = to_string()
        if isinstance(value, str) and value:
            return value

    raise TypeError("asset key must be a string or expose a stable string form")


def _asset_key_path(asset_key: object) -> tuple[str, ...]:
    if isinstance(asset_key, Mapping):
        value = asset_key.get("path")
    else:
        value = getattr(asset_key, "path", None)

    if isinstance(value, str):
        return (value,) if value else ()
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        path = tuple(str(part) for part in value if str(part))
        if path:
            return path
    if isinstance(asset_key, Sequence) and not isinstance(
        asset_key,
        (bytes, bytearray, str),
    ):
        path = tuple(str(part) for part in asset_key if str(part))
        if path:
            return path
    return ()


__all__ = [
    "DbtGateHandlingResult",
    "classify_dbt_test_failure",
    "handle_dbt_test_failures",
    "plan_dbt_test_partial_rerun",
    "stream_dbt_events_with_gate_handling",
]
