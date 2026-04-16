"""Runbook URL helpers for gate decision alerts."""

from __future__ import annotations

from dataclasses import replace
from typing import Final

from orchestrator.alerting.payload import AlertPayload

RUNBOOK_P5_PATH: Final = "docs/RUNBOOK_P5.md"

_SCENARIO_RUNBOOK_ANCHORS: Final[dict[str, str]] = {
    "phase0_data_readiness_delayed": "phase0-data_quality-fail_run",
    "phase0_llm_health_check_failed": "phase0-infra-fail_run",
    "phase0_dbt_test_failed": "phase0-task_level-partial_rerun",
    "phase1_graph_promotion_snapshot_failed": "phase1-publish-fail_run",
    "phase2_single_stock_task_failed": "phase2-task_level-mark_inconclusive",
    "phase2_pool_failure_rate_exceeded": "phase2-data_quality-fail_run",
    "phase3_formal_commit_failed": "phase3-publish-fail_run",
    "phase3_manifest_write_failed": "phase3-infra-repair_manifest",
    "infra_unavailable_hard_stop": "phase2-infra-fail_run",
}


def runbook_url_for(
    phase: str,
    failure_class: str | None,
    action: str,
    *,
    scenario_id: str | None = None,
) -> str:
    """Return the repository-relative P5 runbook URL for a gate outcome."""

    if scenario_id is not None:
        scenario_anchor = _SCENARIO_RUNBOOK_ANCHORS.get(scenario_id.strip())
        if scenario_anchor is not None:
            return _runbook_url(scenario_anchor)

    return _runbook_url(_anchor_for(phase, failure_class, action))


def with_runbook_url(
    payload: AlertPayload,
    *,
    scenario_id: str | None = None,
) -> AlertPayload:
    """Return an alert payload with a runbook URL filled when absent."""

    if payload.runbook_url:
        return payload

    return replace(
        payload,
        runbook_url=runbook_url_for(
            payload.phase,
            payload.failure_class,
            payload.action,
            scenario_id=scenario_id,
        ),
    )


def _anchor_for(phase: str, failure_class: str | None, action: str) -> str:
    failure_anchor = _normalize_part(failure_class) if failure_class else "unknown"
    return "-".join(
        (
            _normalize_part(phase),
            failure_anchor,
            _normalize_part(action),
        ),
    )


def _normalize_part(value: str) -> str:
    return value.strip().lower().replace(" ", "_")


def _runbook_url(anchor: str) -> str:
    return f"{RUNBOOK_P5_PATH}#{anchor}"


__all__ = ["runbook_url_for", "with_runbook_url"]
