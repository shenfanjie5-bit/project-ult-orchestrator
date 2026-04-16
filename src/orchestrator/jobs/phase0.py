"""Phase 0 Dagster asset wiring."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

from dagster import AssetExecutionContext, ResourceParam, asset
from dagster_dbt import DbtCliResource, dbt_assets

from orchestrator.checks.dbt_events import stream_dbt_events_with_gate_handling
from orchestrator.checks.resources import GatePolicyResource
from orchestrator.jobs.phase0_constants import (
    PHASE0_CANDIDATE_FREEZE_ASSET_KEY,
    PHASE0_GROUP_NAME,
    PHASE0_READINESS_ASSET_KEY,
    PHASE0_REQUIRED_ASSET_KEYS,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DBT_PROJECT_DIR_ENV = os.environ.get("ORCHESTRATOR_DBT_PROJECT_DIR")
DBT_PROJECT_DIR = (
    Path(_DBT_PROJECT_DIR_ENV) if _DBT_PROJECT_DIR_ENV else _REPO_ROOT / "dbt_stub"
).expanduser()
DBT_PROFILES_DIR = DBT_PROJECT_DIR
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


def _require_dbt_manifest(manifest_path: Path) -> Path:
    if not manifest_path.exists():
        msg = (
            "dbt manifest is missing at "
            f"{manifest_path}. Run `dbt compile --project-dir "
            f"{DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}` or prepare "
            "the dbt fixture before importing orchestrator.definitions."
        )
        raise FileNotFoundError(msg)
    return manifest_path


@asset(group_name=PHASE0_GROUP_NAME)
def phase0_readiness_ping() -> str:
    return "ok"


@dbt_assets(manifest=_require_dbt_manifest(DBT_MANIFEST_PATH))
def dbt_phase0_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    gate_policy: ResourceParam[GatePolicyResource],
) -> Iterator[object]:
    dbt_invocation = dbt.cli(
        [
            "build",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROFILES_DIR),
        ],
        context=context,
    )
    yield from stream_dbt_events_with_gate_handling(
        context=context,
        dbt_invocation=dbt_invocation,
        policy=gate_policy.policy,
    )


__all__ = [
    "DBT_MANIFEST_PATH",
    "DBT_PROFILES_DIR",
    "DBT_PROJECT_DIR",
    "PHASE0_CANDIDATE_FREEZE_ASSET_KEY",
    "PHASE0_GROUP_NAME",
    "PHASE0_READINESS_ASSET_KEY",
    "PHASE0_REQUIRED_ASSET_KEYS",
    "dbt_phase0_assets",
    "phase0_readiness_ping",
]
