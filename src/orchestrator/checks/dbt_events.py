"""Adapters for dbt test failure events emitted through Dagster."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.models import GateDecision
from orchestrator.policy import FailureClass, GatePolicyProfile, PhaseEnum
from orchestrator.rerun import (
    PartialRerunPlan,
    RunHistorySnapshot,
    compute_partial_rerun_plan,
)


@dataclass(frozen=True, slots=True)
class _DbtGateEvent:
    failure_class: FailureClass
    dbt_node_name: str | None = None
    asset_key: str | None = None


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


def _validated_failed_node_from_event(
    requested_failed_node: str,
    event_asset_key: str | None,
) -> str:
    if event_asset_key is None:
        if _is_validated_dbt_asset_or_group_key(requested_failed_node):
            return requested_failed_node
        msg = (
            "dbt failure event asset_key metadata is required unless "
            "failed_asset_key is a validated dbt asset/group key"
        )
        raise ValueError(msg)

    if event_asset_key != requested_failed_node:
        msg = (
            "dbt failure event asset_key does not match failed_asset_key: "
            f"event={event_asset_key} requested={requested_failed_node}"
        )
        raise ValueError(msg)

    return event_asset_key


def _is_validated_dbt_asset_or_group_key(failed_node: str) -> bool:
    return failed_node.startswith("dbt_") or failed_node.startswith("dbt/")


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
    "classify_dbt_test_failure",
    "plan_dbt_test_partial_rerun",
]
