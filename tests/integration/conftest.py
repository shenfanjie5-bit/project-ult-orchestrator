from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

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
def dagster_instance() -> Iterator[object]:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    with dagster.DagsterInstance.ephemeral() as instance:
        yield instance


@pytest.fixture
def stub_policy_path() -> str:
    return str(_REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml")


@pytest.fixture(scope="session")
def _compiled_dbt_project(tmp_path_factory: pytest.TempPathFactory) -> Path:
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
