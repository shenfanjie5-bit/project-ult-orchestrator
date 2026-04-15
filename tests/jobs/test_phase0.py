import os
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
def phase0_readiness_ping_asset() -> Any:
    pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")
    if not _DBT_MANIFEST_PATH.exists():
        pytest.skip("dbt manifest is not compiled; run make dbt-compile")

    from orchestrator.jobs import phase0_readiness_ping

    return phase0_readiness_ping


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
