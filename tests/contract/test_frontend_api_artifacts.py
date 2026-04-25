from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ARTIFACT_ROOT = Path(__file__).resolve().parents[2] / "artifacts" / "frontend-api"


def test_frontend_api_orchestrator_artifacts_exist() -> None:
    required_paths = [
        ARTIFACT_ROOT / "runs.json",
        ARTIFACT_ROOT / "runs" / "RUN_API4A_001.json",
    ]

    missing = [str(path) for path in required_paths if not path.exists()]

    assert missing == []


def test_frontend_api_orchestrator_runs_index_shape() -> None:
    payload = _load_json(ARTIFACT_ROOT / "runs.json")
    items = payload["items"]

    assert isinstance(items, list)
    assert items[0]["run_id"] == "RUN_API4A_001"
    assert items[0]["cycle_id"] == "CYCLE_20260424"
    assert items[0]["status"] == "success"
    assert isinstance(items[0]["phases"], list)
    assert isinstance(payload["metadata"], dict)


def test_frontend_api_orchestrator_run_detail_shape() -> None:
    payload = _load_json(ARTIFACT_ROOT / "runs" / "RUN_API4A_001.json")

    assert payload["run_id"] == "RUN_API4A_001"
    assert payload["status"] == "success"
    assert isinstance(payload["steps"], list)
    assert payload["steps"][0]["step_id"] == "formal_snapshot_read"
    assert isinstance(payload["metadata"], dict)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload
