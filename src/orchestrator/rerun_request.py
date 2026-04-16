"""Shared manual rerun request file helpers."""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Final

from orchestrator.rerun import PartialRerunPlan

DEFAULT_REQUEST_DIR: Final[str] = ".orchestrator/rerun_requests"


def request_from_plan(plan: PartialRerunPlan) -> dict[str, Any]:
    payload = {
        "run_id": plan.run_id,
        "failed_node": plan.failed_node,
        "rerun_selection": list(plan.rerun_selection),
        "requires_manual_ack": plan.requires_manual_ack,
        "rerun_mode": plan.rerun_mode,
        "generated_at": plan.generated_at.isoformat(),
    }
    if plan.scenario_id is not None:
        payload["scenario_id"] = plan.scenario_id
    return payload


def request_filename(run_id: str, failed_node: str) -> str:
    return f"{safe_filename_part(run_id)}-{safe_filename_part(failed_node)}.json"


def safe_filename_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value).strip("._") or "request"


def write_rerun_request(plan: PartialRerunPlan, request_dir: str | Path) -> Path:
    target_dir = Path(request_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    request_path = target_dir / request_filename(plan.run_id, plan.failed_node)
    payload = json.dumps(request_from_plan(plan), indent=2, sort_keys=True) + "\n"
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
