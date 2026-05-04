from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ARTIFACT_ROOT = Path(__file__).resolve().parents[2] / "artifacts" / "frontend-api"


def test_frontend_api_orchestrator_artifacts_exist() -> None:
    required_paths = [
        ARTIFACT_ROOT / "runs.json",
        ARTIFACT_ROOT / "runs" / "RUN_API4A_001.json",
        ARTIFACT_ROOT / "ex3-graph-signals" / "CYCLE_20260416.json",
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


def test_frontend_api_ex3_graph_signal_artifact_shape() -> None:
    payload = _load_json_value(
        ARTIFACT_ROOT / "ex3-graph-signals" / "CYCLE_20260416.json"
    )
    allowed_signal_keys = {
        "cycle_id",
        "candidate_id",
        "delta_id",
        "delta_type",
        "selection_ref",
        "source_node",
        "target_node",
        "relation_type",
        "properties",
        "evidence_refs",
    }

    assert isinstance(payload, list)
    assert payload
    for signal in payload:
        assert isinstance(signal, dict)
        assert set(signal) == allowed_signal_keys
        assert signal["cycle_id"] == "CYCLE_20260416"
        assert isinstance(signal["candidate_id"], int)
        assert isinstance(signal["properties"], dict)
        assert isinstance(signal["evidence_refs"], list)
        assert not _has_forbidden_property_key(signal["properties"])


def _load_json(path: Path) -> dict[str, Any]:
    payload = _load_json_value(path)
    assert isinstance(payload, dict)
    return payload


def _load_json_value(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _has_forbidden_property_key(value: object) -> bool:
    forbidden_exact = {
        "candidate_queue_id",
        "ingest_seq",
        "log",
        "logs",
        "metadata",
        "private_id",
        "provider",
        "queue_id",
        "raw_text",
        "secret",
        "source",
        "traceback",
    }
    forbidden_tokens = {"log", "logs", "provider", "queue", "raw", "secret", "source"}
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).strip().lower()
            key_tokens = {
                token
                for token in normalized.replace("-", "_").replace(".", "_").split("_")
                if token
            }
            if normalized in forbidden_exact or key_tokens & forbidden_tokens:
                return True
            if _has_forbidden_property_key(item):
                return True
    if isinstance(value, list):
        return any(_has_forbidden_property_key(item) for item in value)
    return False
