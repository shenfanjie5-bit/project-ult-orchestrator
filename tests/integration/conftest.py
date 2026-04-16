from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Iterator
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DBT_STUB_DIR = _REPO_ROOT / "dbt_stub"


def _clear_phase0_imports() -> None:
    for module_name in list(sys.modules):
        if module_name == "orchestrator.definitions":
            sys.modules.pop(module_name, None)
        elif module_name == "orchestrator.jobs":
            sys.modules.pop(module_name, None)
        elif module_name.startswith("orchestrator.jobs."):
            sys.modules.pop(module_name, None)


@pytest.fixture
def dagster_module(integration_toolchain_preflight: None) -> ModuleType:
    return require_integration_module("dagster", "dagster")


@pytest.fixture
def dagster_dbt_module(integration_toolchain_preflight: None) -> ModuleType:
    return require_integration_module("dagster_dbt", "dagster-dbt")


@pytest.fixture(scope="session")
def integration_toolchain_preflight() -> None:
    require_integration_module("dagster", "dagster")
    require_integration_module("dagster_dbt", "dagster-dbt")


@pytest.fixture
def dagster_instance(dagster_module: ModuleType) -> Iterator[object]:
    dagster = dagster_module

    with dagster.DagsterInstance.ephemeral() as instance:
        yield instance


@pytest.fixture
def stub_policy_path() -> str:
    return str(_REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml")


@pytest.fixture(scope="session")
def _compiled_dbt_project(
    tmp_path_factory: pytest.TempPathFactory,
    integration_toolchain_preflight: None,
) -> Path:
    dbt_executable = shutil.which("dbt")
    if dbt_executable is None:
        pytest.skip("dbt CLI is not installed; install the project dev dependencies")

    project_dir = tmp_path_factory.mktemp("orchestrator_dbt") / "dbt_stub"
    shutil.copytree(_DBT_STUB_DIR, project_dir)
    shutil.rmtree(project_dir / "target", ignore_errors=True)
    shutil.rmtree(project_dir / "dbt_packages", ignore_errors=True)
    (project_dir / "dagster_home").mkdir(exist_ok=True)

    completed = subprocess.run(
        [
            dbt_executable,
            "compile",
            "--profiles-dir",
            ".",
            "--project-dir",
            ".",
        ],
        cwd=project_dir,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=45,
    )
    if completed.returncode != 0:
        pytest.fail(f"dbt compile failed:\n{completed.stdout}")

    manifest_path = project_dir / "target" / "manifest.json"
    if not manifest_path.exists():
        pytest.fail(f"dbt compile did not produce {manifest_path}")

    return project_dir


@pytest.fixture
def tmp_dbt_project(
    _compiled_dbt_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    monkeypatch.setenv("ORCHESTRATOR_DBT_PROJECT_DIR", str(_compiled_dbt_project))
    _clear_phase0_imports()

    yield _compiled_dbt_project

    _clear_phase0_imports()


def require_integration_module(module_name: str, package_name: str) -> ModuleType:
    try:
        return import_module(module_name)
    except ModuleNotFoundError as exc:
        pytest.fail(
            f"{package_name} is required for tests/integration; "
            "install the project dev dependencies before running this target. "
            f"Original import error: {exc}",
            pytrace=False,
        )
    except Exception as exc:
        pytest.fail(
            f"{package_name} could not be imported for tests/integration; "
            "install a compatible integration toolchain before running this target. "
            f"Original import error: {type(exc).__name__}: {exc}",
            pytrace=False,
        )


def asset_materialization_keys(result: object) -> set[object]:
    keys: set[object] = set()
    for event in _result_events(result):
        if not (
            getattr(event, "is_step_materialization", False)
            or getattr(event, "event_type_value", None) == "ASSET_MATERIALIZATION"
        ):
            continue

        asset_key = getattr(event, "asset_key", None)
        if asset_key is None:
            event_specific_data = getattr(event, "event_specific_data", None)
            materialization = getattr(event_specific_data, "materialization", None)
            asset_key = getattr(materialization, "asset_key", None)
        if asset_key is not None:
            keys.add(asset_key)
    return keys


def materialization_order(result: object) -> list[object]:
    keys: list[object] = []
    for event in _result_events(result):
        if not (
            getattr(event, "is_step_materialization", False)
            or getattr(event, "event_type_value", None) == "ASSET_MATERIALIZATION"
        ):
            continue

        asset_key = getattr(event, "asset_key", None)
        if asset_key is None:
            event_specific_data = getattr(event, "event_specific_data", None)
            materialization = getattr(event_specific_data, "materialization", None)
            asset_key = getattr(materialization, "asset_key", None)
        if asset_key is not None:
            keys.append(asset_key)
    return keys


def asset_check_evaluations(result: object) -> list[object]:
    evaluations: list[object] = []
    for event in _result_events(result):
        if not (
            getattr(event, "is_asset_check_evaluation", False)
            or getattr(event, "event_type_value", None) == "ASSET_CHECK_EVALUATION"
        ):
            continue

        event_specific_data = getattr(event, "event_specific_data", None)
        evaluation = getattr(event_specific_data, "asset_check_evaluation", None)
        if evaluation is None:
            evaluation = getattr(event_specific_data, "evaluation", None)
        if evaluation is None and _looks_like_asset_check_evaluation(
            event_specific_data,
        ):
            evaluation = event_specific_data
        if evaluation is None:
            evaluation = getattr(event, "asset_check_evaluation", None)
        if evaluation is not None:
            evaluations.append(evaluation)
    return evaluations


def metadata_value(evaluation: object, key: str) -> object:
    metadata = getattr(evaluation, "metadata", {}) or {}
    value = metadata[key]
    return getattr(value, "value", getattr(value, "text", value))


def _result_events(result: object) -> tuple[Any, ...]:
    return tuple(getattr(result, "all_events", ()))


def _looks_like_asset_check_evaluation(value: object) -> bool:
    return value is not None and (
        hasattr(value, "check_name") or hasattr(value, "check_key")
    )
