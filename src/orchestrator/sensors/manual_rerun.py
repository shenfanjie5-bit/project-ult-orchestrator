"""Manual partial-rerun request sensor."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from dagster import AssetKey, RunRequest, SkipReason, sensor

from orchestrator.cli.rerun import DEFAULT_REQUEST_DIR
from orchestrator.jobs.cycle import daily_cycle_job
from orchestrator.jobs.phase3 import PHASE3_FORMAL_COMMIT_ASSET_KEY

_REQUEST_DIR_ENV = "ORCHESTRATOR_RERUN_REQUEST_DIR"
_FAILED_DIR_NAME = ".failed"
_ALLOWED_RERUN_MODES = frozenset({"repair_only", "asset_only", "phase_only"})


@sensor(job=daily_cycle_job, name="manual_rerun_sensor")
def manual_rerun_sensor() -> RunRequest | list[RunRequest] | SkipReason:
    request_dir = Path(os.environ.get(_REQUEST_DIR_ENV, DEFAULT_REQUEST_DIR))
    return evaluate_manual_rerun_requests(request_dir)


def evaluate_manual_rerun_requests(request_dir: Path) -> RunRequest | list[RunRequest] | SkipReason:
    """Read pending request JSON files and return Dagster run requests."""

    if not request_dir.exists():
        return SkipReason(f"manual rerun request dir does not exist: {request_dir}")
    if not request_dir.is_dir():
        return SkipReason(f"manual rerun request path is not a directory: {request_dir}")

    request_files = tuple(sorted(request_dir.glob("*.json")))
    if not request_files:
        return SkipReason(f"no manual rerun requests found in {request_dir}")

    diagnostics: list[str] = []
    run_requests: list[RunRequest] = []
    for request_path in request_files:
        payload, error = _load_request(request_path)
        if error is not None:
            diagnostics.append(error)
            diagnostics.extend(_quarantine_invalid_request(request_dir, request_path, error))
            continue

        assert payload is not None
        try:
            run_requests.append(_build_run_request(payload))
        except Exception as exc:
            error = f"invalid manual rerun request {request_path.name}: {exc}"
            diagnostics.append(error)
            diagnostics.extend(_quarantine_invalid_request(request_dir, request_path, error))

    if len(run_requests) == 1:
        return run_requests[0]
    if run_requests:
        return run_requests

    return SkipReason("; ".join(diagnostics))


def _load_request(request_path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        raw_payload = json.loads(request_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return None, f"invalid manual rerun request JSON {request_path.name}: {exc}"
    except OSError as exc:
        return None, f"failed to read manual rerun request {request_path.name}: {exc}"

    if not isinstance(raw_payload, dict):
        return None, f"invalid manual rerun request {request_path.name}: expected object"

    return _validate_request(raw_payload, request_path.name)


def _validate_request(
    payload: dict[str, Any],
    request_name: str,
) -> tuple[dict[str, Any] | None, str | None]:
    run_id = payload.get("run_id")
    if not _is_non_empty_str(run_id):
        return None, f"invalid manual rerun request {request_name}: run_id is required"

    failed_node = payload.get("failed_node")
    if not _is_non_empty_str(failed_node):
        return (
            None,
            f"invalid manual rerun request {request_name}: failed_node is required",
        )

    rerun_selection = payload.get("rerun_selection")
    if (
        not isinstance(rerun_selection, list)
        or not rerun_selection
        or not all(_is_non_empty_str(item) for item in rerun_selection)
    ):
        return (
            None,
            f"invalid manual rerun request {request_name}: "
            "rerun_selection must be a non-empty string list",
        )

    requires_manual_ack = payload.get("requires_manual_ack")
    if not isinstance(requires_manual_ack, bool):
        return (
            None,
            f"invalid manual rerun request {request_name}: "
            "requires_manual_ack must be a boolean",
        )

    rerun_mode = payload.get("rerun_mode")
    if not _is_non_empty_str(rerun_mode):
        return (
            None,
            f"invalid manual rerun request {request_name}: rerun_mode is required",
        )
    if rerun_mode not in _ALLOWED_RERUN_MODES:
        return (
            None,
            f"unknown rerun_mode in manual rerun request {request_name}: "
            f"{rerun_mode}",
        )

    repair_only_error = _validate_repair_only_selection(
        request_name,
        failed_node=failed_node,
        rerun_mode=rerun_mode,
        rerun_selection=rerun_selection,
        requires_manual_ack=requires_manual_ack,
    )
    if repair_only_error is not None:
        return None, repair_only_error

    generated_at = payload.get("generated_at")
    if not _is_non_empty_str(generated_at):
        return (
            None,
            f"invalid manual rerun request {request_name}: generated_at is required",
        )

    return {
        "run_id": run_id,
        "failed_node": failed_node,
        "rerun_selection": list(rerun_selection),
        "requires_manual_ack": requires_manual_ack,
        "rerun_mode": rerun_mode,
        "generated_at": generated_at,
    }, None


def _validate_repair_only_selection(
    request_name: str,
    *,
    failed_node: str,
    rerun_mode: object,
    rerun_selection: list[object],
    requires_manual_ack: bool,
) -> str | None:
    if rerun_mode != "repair_only":
        return None

    if not requires_manual_ack:
        return (
            f"invalid manual rerun request {request_name}: "
            "repair_only requests require manual acknowledgment"
        )
    if len(rerun_selection) != 1:
        return (
            f"invalid manual rerun request {request_name}: "
            "repair_only rerun_selection must contain exactly one repair asset"
        )
    selected_node = rerun_selection[0]
    if selected_node == failed_node:
        return (
            f"invalid manual rerun request {request_name}: "
            "repair_only rerun_selection must target a repair asset, not "
            "the failed node"
        )
    if selected_node == PHASE3_FORMAL_COMMIT_ASSET_KEY:
        return (
            f"invalid manual rerun request {request_name}: "
            "repair_only rerun_selection must not include formal_objects_commit"
        )

    return None


def _build_run_request(payload: dict[str, Any]) -> RunRequest:
    tags = {
        "rerun_of": payload["run_id"],
        "failed_node": payload["failed_node"],
        "rerun_mode": payload["rerun_mode"],
    }
    run_key = (
        "manual-rerun:"
        f"{payload['run_id']}:"
        f"{payload['failed_node']}:"
        f"{payload['generated_at']}"
    )

    asset_selection = list(payload["rerun_selection"])
    try:
        return RunRequest(
            job_name="daily_cycle_job",
            run_key=run_key,
            asset_selection=asset_selection,
            tags=tags,
        )
    except Exception:
        return RunRequest(
            job_name="daily_cycle_job",
            run_key=run_key,
            asset_selection=[
                AssetKey.from_user_string(asset_key)
                for asset_key in asset_selection
            ],
            tags=tags,
        )


def _quarantine_invalid_request(
    request_dir: Path,
    request_path: Path,
    error: str,
) -> list[str]:
    failed_dir = request_dir / _FAILED_DIR_NAME
    try:
        failed_dir.mkdir(exist_ok=True)
        request_path.replace(failed_dir / request_path.name)
        (failed_dir / f"{request_path.name}.error.txt").write_text(
            error + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        return [
            f"failed to quarantine invalid manual rerun request "
            f"{request_path.name}: {exc}",
        ]
    return []


def _is_non_empty_str(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


__all__ = ["evaluate_manual_rerun_requests", "manual_rerun_sensor"]
