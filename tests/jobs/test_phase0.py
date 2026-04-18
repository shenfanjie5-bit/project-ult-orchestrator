import ast
import os
import sys
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DBT_PROJECT_DIR = Path(
    os.environ.get("ORCHESTRATOR_DBT_PROJECT_DIR", _REPO_ROOT / "dbt_stub")
).expanduser()
_DBT_MANIFEST_PATH = _DBT_PROJECT_DIR / "target" / "manifest.json"


@pytest.fixture
def asset_key_type() -> Any:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    return dagster.AssetKey


@pytest.fixture
def phase0_module() -> Any:
    pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")
    if not _DBT_MANIFEST_PATH.exists():
        pytest.skip("dbt manifest is not compiled; run make dbt-compile")

    from orchestrator.jobs import phase0

    return phase0


@pytest.fixture
def phase0_readiness_ping_asset(phase0_module: Any) -> Any:
    phase0_readiness_ping = phase0_module.phase0_readiness_ping

    return phase0_readiness_ping


def test_phase0_group_constants(phase0_module: Any) -> None:
    assert phase0_module.PHASE0_GROUP_NAME == "phase0"
    assert phase0_module.PHASE0_REQUIRED_ASSET_KEYS == (
        "phase0_readiness_ping",
        "candidate_freeze",
        "graph_status",
    )
    assert phase0_module.PHASE0_GRAPH_CONSISTENCY_CHECK_NAME == (
        "neo4j_graph_consistency_check"
    )


def test_phase0_readiness_ping_asset_exists(
    phase0_readiness_ping_asset: Any,
) -> None:
    phase0_readiness_ping = phase0_readiness_ping_asset

    assert phase0_readiness_ping is not None


def test_phase0_readiness_ping_group_name(
    asset_key_type: Any,
    phase0_readiness_ping_asset: Any,
) -> None:
    phase0_readiness_ping = phase0_readiness_ping_asset
    asset_key = asset_key_type(["phase0_readiness_ping"])

    assert phase0_readiness_ping.group_names_by_key[asset_key] == "phase0"


def test_phase0_readiness_ping_key_is_addressable(
    asset_key_type: Any,
    phase0_readiness_ping_asset: Any,
) -> None:
    phase0_readiness_ping = phase0_readiness_ping_asset
    asset_key = asset_key_type(["phase0_readiness_ping"])

    assert phase0_readiness_ping.key == asset_key
    assert asset_key in phase0_readiness_ping.keys


def test_dbt_phase0_assets_context_is_unannotated_for_dagster_19() -> None:
    phase0_path = _REPO_ROOT / "src" / "orchestrator" / "jobs" / "phase0.py"
    tree = ast.parse(phase0_path.read_text())
    dbt_asset_function = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "dbt_phase0_assets"
    )
    context_arg = dbt_asset_function.args.args[0]

    assert context_arg.arg == "context"
    assert context_arg.annotation is None


def test_dbt_phase0_assets_imports_under_supported_dagster_range(
    phase0_module: Any,
) -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    defs = dagster.Definitions(assets=[phase0_module.dbt_phase0_assets])

    assert isinstance(defs, dagster.Definitions)


def test_missing_dbt_manifest_fails_with_prepare_hint(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")
    monkeypatch.setenv("ORCHESTRATOR_DBT_PROJECT_DIR", str(tmp_path))
    _clear_phase0_imports()

    try:
        with pytest.raises(FileNotFoundError, match="dbt compile|prepare"):
            __import__("orchestrator.jobs.phase0", fromlist=["phase0"])
    finally:
        _clear_phase0_imports()


def _clear_phase0_imports() -> None:
    for module_name in list(sys.modules):
        if module_name == "orchestrator.jobs":
            sys.modules.pop(module_name, None)
        elif module_name.startswith("orchestrator.jobs."):
            sys.modules.pop(module_name, None)
