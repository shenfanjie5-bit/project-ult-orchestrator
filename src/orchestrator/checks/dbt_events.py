"""Adapters for dbt test failure events emitted through Dagster."""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.decision_handler import dispatch_gate_decision_alert
from orchestrator.checks.models import GateDecision
from orchestrator.cli.rerun import DEFAULT_REQUEST_DIR
from orchestrator.policy import FailureClass, GateAction, GatePolicyProfile, PhaseEnum
from orchestrator.rerun import (
    PartialRerunPlan,
    RunHistorySnapshot,
    compute_partial_rerun_plan,
)

_RERUN_REQUEST_DIR_ENV = "ORCHESTRATOR_RERUN_REQUEST_DIR"
_FAILED_DBT_TEST_STATUSES = frozenset({"fail", "error"})
_DBT_ASSET_RESOURCE_TYPES = frozenset({"model", "seed", "snapshot", "source"})


@dataclass(frozen=True, slots=True)
class DbtFailureHandlingResult:
    """Side effects produced for a classified dbt test failure."""

    decision: GateDecision
    partial_rerun_plan: PartialRerunPlan | None
    rerun_request_path: Path | None


@dataclass(frozen=True, slots=True)
class _DbtGateEvent:
    failure_class: FailureClass
    dbt_node_name: str | None = None
    asset_key: str | None = None


def stream_dbt_build_events(
    *,
    context: object,
    dbt_invocation: object,
    policy: GatePolicyProfile,
    manifest_path: str | Path | None = None,
    request_dir: str | Path | None = None,
) -> Iterator[object]:
    """Stream dbt events and handle failed dbt tests through the gate pipeline."""

    failure_event: object | None = None
    try:
        for event in _stream_dbt_invocation(dbt_invocation):
            if failure_event is None:
                failure_event = _dbt_test_failure_from_stream_event(event)
            yield event
    except Exception:
        failure_event = failure_event or _dbt_test_failure_from_artifacts(
            dbt_invocation,
            manifest_path=manifest_path,
        )
        if failure_event is not None:
            handle_dbt_test_failure(
                context=context,
                event=failure_event,
                policy=policy,
                request_dir=request_dir,
            )
        raise

    failure_event = failure_event or _dbt_test_failure_from_artifacts(
        dbt_invocation,
        manifest_path=manifest_path,
    )
    if failure_event is not None:
        handle_dbt_test_failure(
            context=context,
            event=failure_event,
            policy=policy,
            request_dir=request_dir,
        )


def handle_dbt_test_failure(
    *,
    context: object,
    event: object,
    policy: GatePolicyProfile,
    request_dir: str | Path | None = None,
) -> DbtFailureHandlingResult:
    """Classify a dbt test failure and produce alert/rerun side effects."""

    decision = classify_dbt_test_failure(event, policy)
    failed_node = _required_event_asset_key(event)
    partial_rerun_plan: PartialRerunPlan | None = None
    rerun_request_path: Path | None = None

    if decision.action is GateAction.PARTIAL_RERUN:
        partial_rerun_plan = plan_dbt_test_partial_rerun(
            _run_id_from_context(context),
            failed_node,
            event,
            policy,
        )
        rerun_request_path = write_dbt_partial_rerun_request(
            partial_rerun_plan,
            request_dir=request_dir,
        )

    summary = decision.reason or _dbt_failure_summary(event, failed_node)
    dispatch_gate_decision_alert(
        decision,
        cycle_id=_cycle_id_from_context(context),
        failed_node=failed_node,
        summary=summary,
        channels=policy.alert_channels,
    )
    _emit_gate_decision_observation(
        context=context,
        decision=decision,
        failed_node=failed_node,
        event=event,
        partial_rerun_plan=partial_rerun_plan,
        rerun_request_path=rerun_request_path,
    )

    return DbtFailureHandlingResult(
        decision=decision,
        partial_rerun_plan=partial_rerun_plan,
        rerun_request_path=rerun_request_path,
    )


def write_dbt_partial_rerun_request(
    plan: PartialRerunPlan,
    *,
    request_dir: str | Path | None = None,
) -> Path:
    """Write a manual-rerun request for the manual rerun sensor."""

    target_dir = _rerun_request_dir(request_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    request_path = target_dir / _request_filename(plan.run_id, plan.failed_node)
    payload = json.dumps(_request_from_plan(plan), indent=2, sort_keys=True) + "\n"
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=target_dir,
            prefix=f".{request_path.stem}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            temp_file.write(payload)
            temp_file.flush()
            os.fsync(temp_file.fileno())

        os.replace(temp_path, request_path)
        _fsync_directory(target_dir)
    except Exception:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise

    return request_path


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


def _stream_dbt_invocation(dbt_invocation: object) -> Iterator[object]:
    stream = getattr(dbt_invocation, "stream", None)
    if not callable(stream):
        raise TypeError("dbt_invocation must expose stream()")

    yield from stream()


def _dbt_test_failure_from_stream_event(event: object) -> object | None:
    if not _is_failed_asset_check_event(event):
        return None

    return event if _extract_asset_key(event) is not None else None


def _dbt_test_failure_from_artifacts(
    dbt_invocation: object,
    *,
    manifest_path: str | Path | None,
) -> object | None:
    run_results = _dbt_artifact(dbt_invocation, "run_results.json")
    if not isinstance(run_results, Mapping):
        return None

    manifest = _dbt_artifact(
        dbt_invocation,
        "manifest.json",
        fallback_path=manifest_path,
    )
    if not isinstance(manifest, Mapping):
        return None

    results = run_results.get("results")
    if not isinstance(results, Sequence) or isinstance(results, (bytes, str)):
        return None

    for result in results:
        if not _is_failed_dbt_test_result(result, manifest):
            continue

        event = _gate_event_from_dbt_result(result, manifest)
        if event is not None:
            return event

    return None


def _dbt_artifact(
    dbt_invocation: object,
    artifact_name: str,
    *,
    fallback_path: str | Path | None = None,
) -> object | None:
    get_artifact = getattr(dbt_invocation, "get_artifact", None)
    if callable(get_artifact):
        try:
            return get_artifact(artifact_name)
        except Exception:
            pass

    if artifact_name == "manifest.json" and fallback_path is not None:
        try:
            return json.loads(Path(fallback_path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    return None


def _is_failed_asset_check_event(event: object) -> bool:
    passed = _asset_check_passed_value(event)
    if passed is not False:
        return False

    if _extract_dbt_node_name(event) is not None:
        return True

    metadata = _event_metadata(event)
    return any(key in metadata for key in ("dbt_node_info", "node_info", "unique_id"))


def _asset_check_passed_value(event: object) -> bool | None:
    for value in _candidate_values(event, ("passed",)):
        if isinstance(value, bool):
            return value

    event_specific_data = _mapping_or_attr(event, "event_specific_data")
    if event_specific_data is not None:
        for nested_name in ("asset_check_evaluation", "evaluation"):
            evaluation = _mapping_or_attr(event_specific_data, nested_name)
            if evaluation is None:
                continue
            passed = _mapping_or_attr(evaluation, "passed")
            if isinstance(passed, bool):
                return passed

    return None


def _is_failed_dbt_test_result(result: object, manifest: Mapping[str, object]) -> bool:
    if not isinstance(result, Mapping):
        return False

    status = result.get("status")
    if status not in _FAILED_DBT_TEST_STATUSES:
        return False

    unique_id = _dbt_result_unique_id(result)
    if unique_id is None:
        return False

    node = _manifest_node(manifest, unique_id)
    if node is not None:
        return node.get("resource_type") == "test"

    return unique_id.startswith("test.")


def _gate_event_from_dbt_result(
    result: Mapping[str, object],
    manifest: Mapping[str, object],
) -> Mapping[str, object] | None:
    unique_id = _dbt_result_unique_id(result)
    if unique_id is None:
        return None

    asset_key = _dbt_asset_key_for_failed_test(unique_id, manifest)
    if asset_key is None:
        return None

    node_name = _dbt_test_node_name(unique_id, result, manifest)
    return {
        "asset_key": asset_key,
        "metadata": {
            "node_info": {
                "node_name": node_name,
                "unique_id": unique_id,
            },
            "dbt_status": result.get("status"),
            "dbt_message": result.get("message"),
        },
    }


def _dbt_result_unique_id(result: Mapping[str, object]) -> str | None:
    unique_id = result.get("unique_id")
    if isinstance(unique_id, str) and unique_id:
        return unique_id

    node = result.get("node")
    if isinstance(node, Mapping):
        node_unique_id = node.get("unique_id")
        if isinstance(node_unique_id, str) and node_unique_id:
            return node_unique_id

    return None


def _dbt_test_node_name(
    unique_id: str,
    result: Mapping[str, object],
    manifest: Mapping[str, object],
) -> str:
    for value in (result.get("node_name"), result.get("name")):
        if isinstance(value, str) and value:
            return value

    node = _manifest_node(manifest, unique_id)
    if node is not None:
        name = node.get("name")
        if isinstance(name, str) and name:
            return name

    return unique_id


def _dbt_asset_key_for_failed_test(
    unique_id: str,
    manifest: Mapping[str, object],
) -> str | None:
    test_node = _manifest_node(manifest, unique_id)
    for dependency_unique_id in _dbt_test_dependencies(test_node):
        dependency_node = _manifest_node(manifest, dependency_unique_id)
        if dependency_node is None:
            continue
        if dependency_node.get("resource_type") not in _DBT_ASSET_RESOURCE_TYPES:
            continue

        asset_key = _dbt_asset_key_for_node(dependency_node)
        if asset_key is not None:
            return asset_key

    return None


def _dbt_test_dependencies(test_node: Mapping[str, object] | None) -> tuple[str, ...]:
    if test_node is None:
        return ()

    depends_on = test_node.get("depends_on")
    if not isinstance(depends_on, Mapping):
        return ()

    nodes = depends_on.get("nodes")
    if not isinstance(nodes, Sequence) or isinstance(nodes, (bytes, str)):
        return ()

    return tuple(node for node in nodes if isinstance(node, str) and node)


def _dbt_asset_key_for_node(node: Mapping[str, object]) -> str | None:
    dagster_meta = _dagster_meta_for_node(node)
    for key in ("asset_key", "asset_key_path"):
        if key not in dagster_meta:
            continue
        try:
            return _stable_asset_key_string(dagster_meta[key])
        except (TypeError, ValueError):
            continue

    resource_type = node.get("resource_type")
    if resource_type == "source":
        source_name = node.get("source_name")
        name = node.get("name")
        if (
            isinstance(source_name, str)
            and source_name
            and isinstance(name, str)
            and name
        ):
            return f"{source_name}/{name}"
        return None

    name = node.get("name")
    if isinstance(name, str) and name:
        return name

    return None


def _dagster_meta_for_node(node: Mapping[str, object]) -> Mapping[str, object]:
    for parent_key in ("meta", "config"):
        parent = node.get(parent_key)
        if not isinstance(parent, Mapping):
            continue

        meta = parent.get("meta") if parent_key == "config" else parent
        if not isinstance(meta, Mapping):
            continue

        dagster_meta = meta.get("dagster")
        if isinstance(dagster_meta, Mapping):
            return dagster_meta

    return {}


def _manifest_node(
    manifest: Mapping[str, object],
    unique_id: str,
) -> Mapping[str, object] | None:
    for key in ("nodes", "sources"):
        section = manifest.get(key)
        if not isinstance(section, Mapping):
            continue

        node = section.get(unique_id)
        if isinstance(node, Mapping):
            return node

    return None


def _required_event_asset_key(event: object) -> str:
    asset_key = _extract_asset_key(event)
    if asset_key is None:
        raise ValueError("dbt failure event asset_key metadata is required")

    return asset_key


def _run_id_from_context(context: object) -> str:
    run_id = _mapping_or_attr(context, "run_id")
    if isinstance(run_id, str) and run_id:
        return run_id

    run = _mapping_or_attr(context, "run")
    run_id = _mapping_or_attr(run, "run_id") if run is not None else None
    if isinstance(run_id, str) and run_id:
        return run_id

    raise ValueError("Dagster context run_id is required for dbt partial rerun")


def _cycle_id_from_context(context: object) -> str:
    tags = _context_tags(context)
    cycle_id = tags.get("cycle_id")
    if isinstance(cycle_id, str) and cycle_id:
        return cycle_id

    return _run_id_from_context(context)


def _context_tags(context: object) -> Mapping[str, object]:
    tags = _mapping_or_attr(context, "run_tags")
    if isinstance(tags, Mapping):
        return tags

    run = _mapping_or_attr(context, "run")
    tags = _mapping_or_attr(run, "tags") if run is not None else None
    if isinstance(tags, Mapping):
        return tags

    return {}


def _dbt_failure_summary(event: object, failed_node: str) -> str:
    metadata = _event_metadata(event)
    message = metadata.get("dbt_message")
    if isinstance(message, str) and message:
        return message

    node_name = _extract_dbt_node_name(event)
    if node_name:
        return f"dbt test failed for {node_name}"

    return f"dbt test failed for asset {failed_node}"


def _emit_gate_decision_observation(
    *,
    context: object,
    decision: GateDecision,
    failed_node: str,
    event: object,
    partial_rerun_plan: PartialRerunPlan | None,
    rerun_request_path: Path | None,
) -> None:
    log_event = getattr(context, "log_event", None)
    if not callable(log_event):
        return

    try:
        from dagster import AssetKey, AssetObservation
    except ModuleNotFoundError:
        return

    metadata = _gate_decision_metadata(
        decision=decision,
        event=event,
        partial_rerun_plan=partial_rerun_plan,
        rerun_request_path=rerun_request_path,
    )
    log_event(
        AssetObservation(
            asset_key=AssetKey.from_user_string(failed_node),
            description="dbt test failure gate decision",
            metadata=metadata,
        ),
    )


def _gate_decision_metadata(
    *,
    decision: GateDecision,
    event: object,
    partial_rerun_plan: PartialRerunPlan | None,
    rerun_request_path: Path | None,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "phase": decision.phase.value,
        "action": decision.action.value,
        "failure_class": (
            decision.failure_class.value if decision.failure_class is not None else ""
        ),
        "reason": decision.reason or "",
        "dbt_node_name": _extract_dbt_node_name(event) or "",
    }
    if partial_rerun_plan is not None:
        metadata["rerun_mode"] = partial_rerun_plan.rerun_mode
        metadata["rerun_selection"] = json.dumps(
            list(partial_rerun_plan.rerun_selection),
        )
        metadata["requires_manual_ack"] = partial_rerun_plan.requires_manual_ack
    if rerun_request_path is not None:
        metadata["rerun_request_path"] = str(rerun_request_path)

    return metadata


def _request_from_plan(plan: PartialRerunPlan) -> dict[str, Any]:
    return {
        "run_id": plan.run_id,
        "failed_node": plan.failed_node,
        "rerun_selection": list(plan.rerun_selection),
        "requires_manual_ack": plan.requires_manual_ack,
        "rerun_mode": plan.rerun_mode,
        "generated_at": plan.generated_at.isoformat(),
    }


def _request_filename(run_id: str, failed_node: str) -> str:
    return (
        f"{_safe_filename_part(run_id)}-"
        f"{_safe_filename_part(failed_node)}.json"
    )


def _safe_filename_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value).strip("._") or "request"


def _rerun_request_dir(request_dir: str | Path | None) -> Path:
    if request_dir is not None:
        return Path(request_dir)

    return Path(os.environ.get(_RERUN_REQUEST_DIR_ENV, DEFAULT_REQUEST_DIR))


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return

    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


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
    for value in _candidate_values(event, ("dbt_node_name", "node_name", "unique_id")):
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
        return metadata

    event_specific_data = _mapping_or_attr(event, "event_specific_data")
    if event_specific_data is not None:
        nested_metadata = _mapping_or_attr(event_specific_data, "metadata")
        if isinstance(nested_metadata, Mapping):
            return nested_metadata

        for nested_name in ("asset_check_evaluation", "evaluation"):
            evaluation = _mapping_or_attr(event_specific_data, nested_name)
            if evaluation is None:
                continue
            nested_metadata = _mapping_or_attr(evaluation, "metadata")
            if isinstance(nested_metadata, Mapping):
                return nested_metadata

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
        return value.get(name)
    return getattr(value, name, None)


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
    "DbtFailureHandlingResult",
    "classify_dbt_test_failure",
    "handle_dbt_test_failure",
    "plan_dbt_test_partial_rerun",
    "stream_dbt_build_events",
    "write_dbt_partial_rerun_request",
]
