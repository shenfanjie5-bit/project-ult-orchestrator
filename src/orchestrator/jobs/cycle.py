"""Daily cycle Dagster job definitions."""

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dagster import AssetSelection, define_asset_job

from orchestrator.checks.decision_handler import dispatch_gate_decision_alert
from orchestrator.checks.phase1 import build_phase1_graph_failure_gate_hook
from orchestrator.checks.phase3 import (
    ManifestWriteFailureEvent,
    classify_manifest_write_failure,
    plan_manifest_repair_rerun,
)
from orchestrator.jobs.audit import AUDIT_EVAL_GROUP_NAME
from orchestrator.jobs.phase0_constants import PHASE0_GROUP_NAME
from orchestrator.jobs.phase1 import PHASE1_GROUP_NAME
from orchestrator.jobs.phase2 import PHASE2_GROUP_NAME
from orchestrator.jobs.phase3 import PHASE3_GROUP_NAME, PHASE3_MANIFEST_ASSET_KEY
from orchestrator.policy import GateAction, GatePolicyProfile
from orchestrator.rerun_request import DEFAULT_REQUEST_DIR, write_rerun_request

_MANIFEST_REPAIR_ASSET_KEY_ENV = "ORCHESTRATOR_MANIFEST_REPAIR_ASSET_KEY"
_DEFAULT_MANIFEST_REPAIR_ASSET_KEY = "repair_cycle_publish_manifest"
_RERUN_REQUEST_DIR_ENV = "ORCHESTRATOR_RERUN_REQUEST_DIR"


def _build_phase3_manifest_failure_gate_hook() -> object:
    """Build a Dagster failure hook for Phase 3 manifest write failures."""

    from dagster import failure_hook

    @failure_hook(required_resource_keys={"gate_policy"})
    def phase3_manifest_failure_gate(context: object) -> None:
        event = _manifest_failure_event_from_hook_context(context)
        if event is None:
            return

        policy = _policy_from_hook_context(context)
        decision = classify_manifest_write_failure(event, policy)
        request_path: Path | None = None
        plan_error: str | None = None
        write_error: str | None = None

        if decision.action is GateAction.REPAIR_MANIFEST:
            try:
                plan = plan_manifest_repair_rerun(
                    _run_id_from_hook_context(context),
                    event,
                    policy,
                )
            except Exception as exc:
                plan_error = _exception_summary(exc)
            else:
                try:
                    request_path = write_rerun_request(plan, _request_dir())
                except OSError as exc:
                    write_error = _exception_summary(exc)

        dispatch_gate_decision_alert(
            decision,
            cycle_id=_cycle_id_from_hook_context(context),
            failed_node=event.failed_node,
            summary=_manifest_alert_summary(
                event,
                decision_reason=decision.reason,
                request_path=request_path,
                plan_error=plan_error,
                write_error=write_error,
            ),
            channels=policy.alert_channels,
        )

    return phase3_manifest_failure_gate


def _manifest_failure_event_from_hook_context(
    context: object,
) -> ManifestWriteFailureEvent | None:
    if _failed_node_from_hook_context(context) != PHASE3_MANIFEST_ASSET_KEY:
        return None

    return ManifestWriteFailureEvent(
        repair_node=_manifest_repair_asset_key(),
        reason=_reason_from_hook_context(context),
    )


def _failed_node_from_hook_context(context: object) -> str | None:
    op = getattr(context, "op", None)
    for value in (
        getattr(op, "name", None),
        getattr(context, "op_name", None),
        getattr(context, "step_key", None),
    ):
        if value == PHASE3_MANIFEST_ASSET_KEY:
            return PHASE3_MANIFEST_ASSET_KEY
    return None


def _manifest_repair_asset_key() -> str:
    return os.environ.get(
        _MANIFEST_REPAIR_ASSET_KEY_ENV,
        _DEFAULT_MANIFEST_REPAIR_ASSET_KEY,
    ).strip()


def _request_dir() -> Path:
    return Path(os.environ.get(_RERUN_REQUEST_DIR_ENV, DEFAULT_REQUEST_DIR))


def _reason_from_hook_context(context: object) -> str | None:
    exception = getattr(context, "op_exception", None)
    if exception is None:
        return None

    return _exception_summary(exception)


def _exception_summary(exception: BaseException) -> str:
    message = str(exception)
    if message:
        return f"{type(exception).__name__}: {message}"
    return type(exception).__name__


def _policy_from_hook_context(context: object) -> GatePolicyProfile:
    resources = getattr(context, "resources", None)
    gate_policy_resource = getattr(resources, "gate_policy", None)
    policy = getattr(gate_policy_resource, "policy", gate_policy_resource)
    if not isinstance(policy, GatePolicyProfile):
        msg = "gate_policy resource must expose a GatePolicyProfile policy"
        raise TypeError(msg)
    return policy


def _cycle_id_from_hook_context(context: object) -> str:
    tags = _hook_context_tags(context)
    cycle_id = tags.get("cycle_id")
    if isinstance(cycle_id, str) and cycle_id:
        return cycle_id

    return _run_id_from_hook_context(context)


def _run_id_from_hook_context(context: object) -> str:
    run_id = getattr(context, "run_id", None)
    if isinstance(run_id, str) and run_id:
        return run_id

    run = getattr(context, "run", None)
    run_id = getattr(run, "run_id", None) if run is not None else None
    if isinstance(run_id, str) and run_id:
        return run_id

    return "unknown-daily-cycle-run"


def _hook_context_tags(context: object) -> Mapping[str, Any]:
    for container in (
        context,
        getattr(context, "run", None),
        getattr(context, "dagster_run", None),
    ):
        if container is None:
            continue
        for attribute_name in ("run_tags", "tags"):
            tags = getattr(container, attribute_name, None)
            if isinstance(tags, Mapping):
                return tags
    return {}


def _manifest_alert_summary(
    event: ManifestWriteFailureEvent,
    *,
    decision_reason: str | None,
    request_path: Path | None,
    plan_error: str | None,
    write_error: str | None,
) -> str:
    summary = event.reason or decision_reason or "Phase 3 manifest write failed"
    details: list[str] = []
    if request_path is not None:
        details.append(f"rerun request: {request_path}")
    if plan_error:
        details.append(f"repair rerun plan failed: {plan_error}")
    if write_error:
        details.append(f"repair rerun request write failed: {write_error}")
    if details:
        return f"{summary} ({'; '.join(details)})"
    return summary


daily_cycle_job = define_asset_job(
    name="daily_cycle_job",
    selection=AssetSelection.groups(
        PHASE0_GROUP_NAME,
        PHASE1_GROUP_NAME,
        PHASE2_GROUP_NAME,
        PHASE3_GROUP_NAME,
        AUDIT_EVAL_GROUP_NAME,
    ),
    hooks={
        build_phase1_graph_failure_gate_hook(),
        _build_phase3_manifest_failure_gate_hook(),
    },
)


def build_daily_cycle_jobs(
    phase_config: Mapping[str, Any] | None,
) -> tuple[object, ...]:
    """Build daily cycle jobs from the phase configuration facade."""

    return (daily_cycle_job,)


__all__ = [
    "build_daily_cycle_jobs",
    "daily_cycle_job",
]
