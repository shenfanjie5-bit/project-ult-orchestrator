"""Read-only run diagnostics assembled from Dagster logs and rerun requests."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from orchestrator.alerting import runbook_url_for
from orchestrator.rerun_request import DEFAULT_REQUEST_DIR


class RunDiagnosticNotFound(LookupError):
    """Raised when no Dagster run can be found for a cycle id."""


@dataclass(frozen=True, slots=True)
class GateDecisionDiagnostic:
    run_id: str | None
    phase: str
    failure_class: str | None
    action: str
    scenario_id: str | None
    failed_node: str | None
    summary: str | None
    runbook_url: str | None


@dataclass(frozen=True, slots=True)
class RerunPlanDiagnostic:
    run_id: str
    failed_node: str
    rerun_selection: tuple[str, ...]
    requires_manual_ack: bool
    rerun_mode: str
    generated_at: str
    scenario_id: str | None = None


@dataclass(frozen=True, slots=True)
class _RunRecordDiagnostic:
    run_id: str
    job_name: str | None
    status: str | None
    tags: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class RunDiagnosticSummary:
    cycle_id: str
    runs: tuple[_RunRecordDiagnostic, ...]
    gate_decisions: tuple[GateDecisionDiagnostic, ...]
    rerun_plans: tuple[RerunPlanDiagnostic, ...]
    generated_at: str


def collect_run_diagnostic(
    cycle_id: str,
    *,
    instance: object | None = None,
    request_dir: str | Path = DEFAULT_REQUEST_DIR,
) -> RunDiagnosticSummary:
    """Collect a JSON-serializable diagnostic summary for one cycle id."""

    normalized_cycle_id = cycle_id.strip()
    if not normalized_cycle_id:
        raise ValueError("cycle_id is required")

    if instance is None:
        instance = _default_dagster_instance()

    raw_runs = _runs_for_cycle(instance, normalized_cycle_id)
    if not raw_runs:
        raise RunDiagnosticNotFound(
            f"no Dagster runs found for cycle_id={normalized_cycle_id}",
        )

    run_records = tuple(_run_record_diagnostic(run) for run in raw_runs)
    run_ids = frozenset(run.run_id for run in run_records)
    gate_decisions = _gate_decisions_for_runs(instance, raw_runs)
    rerun_plans = _rerun_plans_from_request_dir(request_dir, run_ids)

    return RunDiagnosticSummary(
        cycle_id=normalized_cycle_id,
        runs=run_records,
        gate_decisions=gate_decisions,
        rerun_plans=rerun_plans,
        generated_at=_now_iso(),
    )


def run_diagnostic_to_dict(summary: RunDiagnosticSummary) -> dict[str, object]:
    """Serialize a run diagnostic summary using only JSON-native containers."""

    return {
        "cycle_id": summary.cycle_id,
        "runs": [
            {
                "run_id": run.run_id,
                "job_name": run.job_name,
                "status": run.status,
                "tags": _json_safe(dict(run.tags)),
            }
            for run in summary.runs
        ],
        "gate_decisions": [
            {
                "run_id": decision.run_id,
                "phase": decision.phase,
                "failure_class": decision.failure_class,
                "action": decision.action,
                "scenario_id": decision.scenario_id,
                "failed_node": decision.failed_node,
                "summary": decision.summary,
                "runbook_url": decision.runbook_url,
            }
            for decision in summary.gate_decisions
        ],
        "rerun_plans": [
            {
                "run_id": plan.run_id,
                "failed_node": plan.failed_node,
                "rerun_selection": list(plan.rerun_selection),
                "requires_manual_ack": plan.requires_manual_ack,
                "rerun_mode": plan.rerun_mode,
                "generated_at": plan.generated_at,
                "scenario_id": plan.scenario_id,
            }
            for plan in summary.rerun_plans
        ],
        "generated_at": summary.generated_at,
    }


def _default_dagster_instance() -> object:
    try:
        from dagster import DagsterInstance
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "dagster is required when no diagnostic instance is supplied",
        ) from exc

    return DagsterInstance.get()


def _runs_for_cycle(instance: object, cycle_id: str) -> tuple[object, ...]:
    runs = _query_runs(instance, cycle_id)
    return tuple(
        run
        for run in runs
        if _run_tags(_dagster_run(run)).get("cycle_id") == cycle_id
    )


def _query_runs(instance: object, cycle_id: str) -> tuple[object, ...]:
    filters = _runs_filter(cycle_id)
    for method_name in ("get_runs", "get_run_records"):
        method = getattr(instance, method_name, None)
        if not callable(method):
            continue

        if filters is not None:
            for keyword in ("filters", "run_filter"):
                try:
                    return _as_tuple(method(**{keyword: filters}))
                except TypeError:
                    continue

        try:
            return _as_tuple(method())
        except TypeError:
            continue

    return ()


def _runs_filter(cycle_id: str) -> object | None:
    try:
        from dagster import RunsFilter
    except Exception:
        return None

    try:
        return RunsFilter(tags={"cycle_id": cycle_id})
    except TypeError:
        return None


def _run_record_diagnostic(raw_run: object) -> _RunRecordDiagnostic:
    run = _dagster_run(raw_run)
    run_id = _required_string(_mapping_or_attr(run, "run_id"), "run_id")
    return _RunRecordDiagnostic(
        run_id=run_id,
        job_name=_optional_string(
            _first_present_attr(run, ("job_name", "pipeline_name")),
        ),
        status=_status_string(_mapping_or_attr(run, "status")),
        tags=_json_safe_mapping(_run_tags(run)),
    )


def _dagster_run(raw_run: object) -> object:
    return _mapping_or_attr(raw_run, "dagster_run") or raw_run


def _run_tags(run: object) -> Mapping[str, object]:
    tags = _mapping_or_attr(run, "tags")
    return tags if isinstance(tags, Mapping) else {}


def _gate_decisions_for_runs(
    instance: object,
    raw_runs: Sequence[object],
) -> tuple[GateDecisionDiagnostic, ...]:
    decisions: list[GateDecisionDiagnostic] = []
    seen: set[tuple[object, ...]] = set()

    for raw_run in raw_runs:
        run = _dagster_run(raw_run)
        run_id = _required_string(_mapping_or_attr(run, "run_id"), "run_id")
        run_tags = _run_tags(run)
        for record in _event_records_for_run(instance, run_id):
            for decision in _gate_decisions_from_record(record, run_id, run_tags):
                key = (
                    decision.run_id,
                    decision.phase,
                    decision.failure_class,
                    decision.action,
                    decision.scenario_id,
                    decision.failed_node,
                    decision.summary,
                )
                if key in seen:
                    continue
                seen.add(key)
                decisions.append(decision)

    return tuple(decisions)


def _event_records_for_run(instance: object, run_id: str) -> tuple[object, ...]:
    all_logs = getattr(instance, "all_logs", None)
    if callable(all_logs):
        return _as_tuple(all_logs(run_id))

    get_records_for_run = getattr(instance, "get_records_for_run", None)
    if callable(get_records_for_run):
        try:
            return _records_from_connection(
                get_records_for_run(run_id, ascending=True),
            )
        except TypeError:
            return _records_from_connection(get_records_for_run(run_id))

    return ()


def _records_from_connection(value: object) -> tuple[object, ...]:
    records = _mapping_or_attr(value, "records")
    if records is not None:
        return _as_tuple(records)
    return _as_tuple(value)


def _gate_decisions_from_record(
    record: object,
    fallback_run_id: str,
    run_tags: Mapping[str, object],
) -> tuple[GateDecisionDiagnostic, ...]:
    entry = _event_log_entry(record)
    event = _dagster_event(entry)
    run_id = (
        _optional_string(_mapping_or_attr(entry, "run_id"))
        or _optional_string(_mapping_or_attr(record, "run_id"))
        or fallback_run_id
    )

    decisions: list[GateDecisionDiagnostic] = []
    for owner in _gate_metadata_owners(event):
        metadata = _metadata(owner)
        action = _optional_string(metadata.get("action"))
        if not action:
            continue

        failure_class = _optional_string(metadata.get("failure_class"))
        scenario_id = _optional_string(metadata.get("scenario_id"))
        failed_node = _failed_node(metadata, owner, event)
        phase = (
            _optional_string(metadata.get("phase"))
            or _optional_string(run_tags.get("phase"))
            or _infer_phase(failed_node, owner, event)
        )
        summary = _summary(metadata, owner, entry, phase=phase, action=action)
        runbook_url = _diagnostic_runbook_url(
            metadata,
            phase=phase,
            failure_class=failure_class,
            action=action,
            scenario_id=scenario_id,
        )

        decisions.append(
            GateDecisionDiagnostic(
                run_id=run_id,
                phase=phase,
                failure_class=failure_class,
                action=action,
                scenario_id=scenario_id,
                failed_node=failed_node,
                summary=summary,
                runbook_url=runbook_url,
            ),
        )

    return tuple(decisions)


def _event_log_entry(record: object) -> object:
    return (
        _mapping_or_attr(record, "event_log_entry")
        or _mapping_or_attr(record, "event_log_entry_data")
        or record
    )


def _dagster_event(entry: object) -> object:
    return _mapping_or_attr(entry, "dagster_event") or entry


def _gate_metadata_owners(event: object) -> tuple[object, ...]:
    owners: list[object] = []
    event_specific_data = _mapping_or_attr(event, "event_specific_data")

    for candidate in (
        _mapping_or_attr(event_specific_data, "asset_observation"),
        _mapping_or_attr(event_specific_data, "observation"),
        _mapping_or_attr(event, "asset_observation"),
        _mapping_or_attr(event, "observation"),
    ):
        if candidate is not None:
            owners.append(candidate)

    for candidate in (
        _mapping_or_attr(event_specific_data, "asset_check_evaluation"),
        _mapping_or_attr(event_specific_data, "evaluation"),
        _mapping_or_attr(event, "asset_check_evaluation"),
        _mapping_or_attr(event, "evaluation"),
    ):
        if candidate is not None:
            owners.append(candidate)

    if _has_metadata(event_specific_data):
        owners.append(event_specific_data)
    if _has_metadata(event):
        owners.append(event)

    return tuple(owner for owner in owners if _metadata(owner))


def _metadata(owner: object) -> dict[str, object]:
    raw_metadata = _mapping_or_attr(owner, "metadata")
    if not isinstance(raw_metadata, Mapping):
        return {}
    return {
        str(key): _metadata_value(value)
        for key, value in raw_metadata.items()
    }


def _metadata_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _metadata_value(nested_value)
            for key, nested_value in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return [_metadata_value(item) for item in value]

    to_user_string = getattr(value, "to_user_string", None)
    if callable(to_user_string):
        return to_user_string()

    for attribute_name in ("value", "text", "data", "path", "url", "md_str"):
        attribute_value = getattr(value, attribute_name, None)
        if attribute_value is not None:
            return _metadata_value(attribute_value)

    return str(value)


def _failed_node(
    metadata: Mapping[str, object],
    owner: object,
    event: object,
) -> str | None:
    for key in ("failed_node", "failed_nodes", "asset_key", "dbt_node_name"):
        value = _optional_string(metadata.get(key))
        if value:
            return value

    check_name = _check_name(owner)
    if check_name:
        return check_name

    for candidate in (
        _mapping_or_attr(owner, "asset_key"),
        _mapping_or_attr(event, "asset_key"),
    ):
        asset_key = _asset_key_string(candidate)
        if asset_key:
            return asset_key

    return None


def _check_name(owner: object) -> str | None:
    check_name = _optional_string(_mapping_or_attr(owner, "check_name"))
    if check_name:
        return check_name

    check_key = _mapping_or_attr(owner, "check_key")
    return _optional_string(_mapping_or_attr(check_key, "name"))


def _asset_key_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        parts = [str(part) for part in value if str(part)]
        return "/".join(parts) if parts else None

    to_user_string = getattr(value, "to_user_string", None)
    if callable(to_user_string):
        return _optional_string(to_user_string())

    path = getattr(value, "path", None)
    if isinstance(path, Sequence) and not isinstance(path, (str, bytes, bytearray)):
        parts = [str(part) for part in path if str(part)]
        return "/".join(parts) if parts else None

    return _optional_string(str(value))


def _infer_phase(failed_node: str | None, owner: object, event: object) -> str:
    candidates = [
        failed_node,
        _check_name(owner),
        _asset_key_string(_mapping_or_attr(owner, "asset_key")),
        _asset_key_string(_mapping_or_attr(event, "asset_key")),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        lowered = candidate.lower()
        for phase in ("phase0", "phase1", "phase2", "phase3"):
            if phase in lowered:
                return phase
        if "llm_health" in lowered or "dbt" in lowered:
            return "phase0"
        if "graph_" in lowered:
            return "phase1"
        if "formal" in lowered or "manifest" in lowered:
            return "phase3"
    return "unknown"


def _summary(
    metadata: Mapping[str, object],
    owner: object,
    entry: object,
    *,
    phase: str,
    action: str,
) -> str | None:
    for key in ("summary", "reason", "message", "error", "description"):
        value = _optional_string(metadata.get(key))
        if value:
            return value

    for value in (
        _mapping_or_attr(owner, "description"),
        _mapping_or_attr(entry, "message"),
        _mapping_or_attr(entry, "user_message"),
    ):
        summary = _optional_string(value)
        if summary:
            return summary

    return f"{phase} gate decision: {action}"


def _diagnostic_runbook_url(
    metadata: Mapping[str, object],
    *,
    phase: str,
    failure_class: str | None,
    action: str,
    scenario_id: str | None,
) -> str | None:
    explicit = _optional_string(metadata.get("runbook_url"))
    if explicit:
        return explicit
    if action == "continue" or phase == "unknown":
        return None
    return runbook_url_for(phase, failure_class, action, scenario_id=scenario_id)


def _rerun_plans_from_request_dir(
    request_dir: str | Path,
    run_ids: frozenset[str],
) -> tuple[RerunPlanDiagnostic, ...]:
    path = Path(request_dir)
    if not path.exists() or not path.is_dir():
        return ()

    plans: list[RerunPlanDiagnostic] = []
    for request_path in sorted(path.glob("*.json")):
        plan = _rerun_plan_from_request_path(request_path)
        if plan is None or plan.run_id not in run_ids:
            continue
        plans.append(plan)
    return tuple(plans)


def _rerun_plan_from_request_path(path: Path) -> RerunPlanDiagnostic | None:
    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None

    if not isinstance(raw_payload, Mapping):
        return None

    try:
        return RerunPlanDiagnostic(
            run_id=_required_string(raw_payload.get("run_id"), "run_id"),
            failed_node=_required_string(
                raw_payload.get("failed_node"),
                "failed_node",
            ),
            rerun_selection=_required_string_tuple(
                raw_payload.get("rerun_selection"),
                "rerun_selection",
            ),
            requires_manual_ack=_required_bool(
                raw_payload.get("requires_manual_ack"),
                "requires_manual_ack",
            ),
            rerun_mode=_required_string(raw_payload.get("rerun_mode"), "rerun_mode"),
            generated_at=_required_string(
                raw_payload.get("generated_at"),
                "generated_at",
            ),
            scenario_id=_optional_string(raw_payload.get("scenario_id")),
        )
    except (TypeError, ValueError):
        return None


def _mapping_or_attr(value: object, key: str) -> object | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return value.get(key)
    return getattr(value, key, None)


def _first_present_attr(value: object, keys: Iterable[str]) -> object | None:
    for key in keys:
        candidate = _mapping_or_attr(value, key)
        if candidate is not None:
            return candidate
    return None


def _has_metadata(value: object) -> bool:
    raw_metadata = _mapping_or_attr(value, "metadata")
    return isinstance(raw_metadata, Mapping) and bool(raw_metadata)


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float)):
        return str(value)
    return str(value).strip() or None


def _required_string(value: object, key: str) -> str:
    string_value = _optional_string(value)
    if string_value is None:
        raise ValueError(f"{key} is required")
    return string_value


def _required_string_tuple(value: object, key: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{key} must be a string sequence")
    output = tuple(_required_string(item, key) for item in value)
    if not output:
        raise ValueError(f"{key} must not be empty")
    return output


def _required_bool(value: object, key: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be a boolean")
    return value


def _status_string(status: object) -> str | None:
    if status is None:
        return None
    value = getattr(status, "value", None)
    if isinstance(value, str):
        return value
    name = getattr(status, "name", None)
    if isinstance(name, str):
        return name
    return str(status)


def _json_safe_mapping(mapping: Mapping[str, object]) -> Mapping[str, object]:
    return {str(key): _json_safe(value) for key, value in mapping.items()}


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(nested) for key, nested in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return [_json_safe(item) for item in value]
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return str(value)


def _as_tuple(value: object) -> tuple[object, ...]:
    if value is None:
        return ()
    if isinstance(value, tuple):
        return value
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(value)
    return (value,)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


__all__ = [
    "GateDecisionDiagnostic",
    "RerunPlanDiagnostic",
    "RunDiagnosticNotFound",
    "RunDiagnosticSummary",
    "collect_run_diagnostic",
    "run_diagnostic_to_dict",
]
