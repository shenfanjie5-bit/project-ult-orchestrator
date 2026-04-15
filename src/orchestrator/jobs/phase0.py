"""Phase 0 Dagster asset wiring."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Iterator

from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets


logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DBT_PROJECT_DIR_ENV = os.environ.get("ORCHESTRATOR_DBT_PROJECT_DIR")
DBT_PROJECT_DIR = (
    Path(_DBT_PROJECT_DIR_ENV) if _DBT_PROJECT_DIR_ENV else _REPO_ROOT / "dbt_stub"
).expanduser()
DBT_PROFILES_DIR = DBT_PROJECT_DIR
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"
DBT_PROJECT = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)
try:
    DBT_PROJECT.prepare_if_dev()
except Exception as exc:  # pragma: no cover - depends on local dbt adapter setup
    logger.warning("Could not prepare dbt project for dev: %s", exc)


@asset(group_name="phase0")
def phase0_readiness_ping() -> str:
    return "ok"


if DBT_PROJECT.manifest_path.exists():

    @dbt_assets(manifest=DBT_PROJECT.manifest_path)
    def dbt_phase0_assets(
        context: AssetExecutionContext,
        dbt: DbtCliResource,
    ) -> Iterator[object]:
        yield from dbt.cli(
            [
                "build",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROFILES_DIR),
            ],
            context=context,
        ).stream()

else:

    @asset(name="heartbeat", group_name="phase0", compute_kind="dbt")
    def dbt_phase0_assets() -> int:
        return 1
