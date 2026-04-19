"""Tests for ``orchestrator min-cycle`` (assembly e2e public CLI).

These guard the argv contract and the OrchestratorCycleReport schema
shape consumed by ``assembly.tests.e2e.runner``. The actual cycle
execution is a placeholder in this stage; richer Phase 0-3 wiring is
covered by the orchestrator stage 2 milestone.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from orchestrator.cli.main import main as orchestrator_main
from orchestrator.cli.min_cycle import main as min_cycle_main


def _write_minimal_cycle_manifest(
    path: Path,
    *,
    scenario_id: str = "minimal-daily-cycle-placeholder",
    expected_phases: list[str] | None = None,
    required_artifacts: list[str] | None = None,
) -> Path:
    expected_phases = expected_phases or [
        "resolve-profile",
        "load-fixture",
        "execute-minimal-cycle",
        "write-report",
    ]
    required_artifacts = required_artifacts or ["cycle_summary"]
    payload = {
        "scenario_id": scenario_id,
        "expected_phases": expected_phases,
        "required_artifacts": required_artifacts,
        "orchestrator_args": [
            "min-cycle",
            "--profile",
            "{profile_id}",
            "--fixture",
            "{fixture_manifest}",
            "--run-artifacts-dir",
            "{run_dir}",
            "--report",
            "{orchestrator_report_path}",
        ],
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _build_argv(*, fixture: Path, run_dir: Path, report: Path) -> list[str]:
    return [
        "--profile",
        "lite-local",
        "--fixture",
        str(fixture),
        "--run-artifacts-dir",
        str(run_dir),
        "--report",
        str(report),
    ]


class TestArgvContract:
    def test_min_cycle_main_succeeds_on_valid_manifest(self, tmp_path: Path) -> None:
        manifest = _write_minimal_cycle_manifest(tmp_path / "manifest.yaml")
        report = tmp_path / "report.json"

        rc = min_cycle_main(
            _build_argv(
                fixture=manifest,
                run_dir=tmp_path / "run",
                report=report,
            )
        )

        assert rc == 0
        assert report.is_file()

    def test_top_level_orchestrator_dispatches_min_cycle(self, tmp_path: Path) -> None:
        manifest = _write_minimal_cycle_manifest(tmp_path / "manifest.yaml")
        report = tmp_path / "report.json"

        rc = orchestrator_main(
            ["min-cycle", *_build_argv(
                fixture=manifest,
                run_dir=tmp_path / "run",
                report=report,
            )]
        )

        assert rc == 0
        assert report.is_file()

    @pytest.mark.parametrize(
        "missing_arg",
        ["--profile", "--fixture", "--run-artifacts-dir", "--report"],
    )
    def test_missing_required_arg_returns_nonzero(
        self, tmp_path: Path, missing_arg: str
    ) -> None:
        manifest = _write_minimal_cycle_manifest(tmp_path / "manifest.yaml")
        argv = _build_argv(
            fixture=manifest,
            run_dir=tmp_path / "run",
            report=tmp_path / "report.json",
        )
        # remove the flag and its following value
        idx = argv.index(missing_arg)
        del argv[idx : idx + 2]

        rc = min_cycle_main(argv)

        assert rc != 0


class TestReportSchema:
    """OrchestratorCycleReport (assembly.tests.e2e.schema) compatibility.

    Fields under ConfigDict(extra="forbid"): profile_id, phases, artifacts,
    status (Literal["success","failed","partial"]), failure_reason.
    """

    def test_success_report_has_required_fields(self, tmp_path: Path) -> None:
        manifest = _write_minimal_cycle_manifest(tmp_path / "manifest.yaml")
        report_path = tmp_path / "report.json"

        rc = min_cycle_main(
            _build_argv(
                fixture=manifest,
                run_dir=tmp_path / "run",
                report=report_path,
            )
        )

        assert rc == 0
        report = json.loads(report_path.read_text())
        assert set(report.keys()) == {
            "profile_id",
            "phases",
            "artifacts",
            "status",
            "failure_reason",
        }

    def test_success_report_status_is_literal_success(self, tmp_path: Path) -> None:
        manifest = _write_minimal_cycle_manifest(tmp_path / "manifest.yaml")
        report_path = tmp_path / "report.json"

        min_cycle_main(
            _build_argv(
                fixture=manifest,
                run_dir=tmp_path / "run",
                report=report_path,
            )
        )

        report = json.loads(report_path.read_text())
        assert report["status"] == "success"
        assert report["failure_reason"] is None

    def test_success_report_phases_match_manifest(self, tmp_path: Path) -> None:
        custom_phases = ["phase-a", "phase-b", "phase-c"]
        manifest = _write_minimal_cycle_manifest(
            tmp_path / "manifest.yaml", expected_phases=custom_phases
        )
        report_path = tmp_path / "report.json"

        min_cycle_main(
            _build_argv(
                fixture=manifest,
                run_dir=tmp_path / "run",
                report=report_path,
            )
        )

        report = json.loads(report_path.read_text())
        assert report["phases"] == custom_phases

    def test_success_report_includes_required_artifacts(self, tmp_path: Path) -> None:
        manifest = _write_minimal_cycle_manifest(
            tmp_path / "manifest.yaml",
            required_artifacts=["cycle_summary", "extra_artifact"],
        )
        report_path = tmp_path / "report.json"

        min_cycle_main(
            _build_argv(
                fixture=manifest,
                run_dir=tmp_path / "run",
                report=report_path,
            )
        )

        report = json.loads(report_path.read_text())
        assert set(report["artifacts"].keys()) == {"cycle_summary", "extra_artifact"}
        for path_str in report["artifacts"].values():
            assert Path(path_str).is_file()

    def test_failure_report_emitted_on_missing_manifest(self, tmp_path: Path) -> None:
        report_path = tmp_path / "report.json"

        rc = min_cycle_main(
            _build_argv(
                fixture=tmp_path / "no-such.yaml",
                run_dir=tmp_path / "run",
                report=report_path,
            )
        )

        assert rc != 0
        assert report_path.is_file()
        report = json.loads(report_path.read_text())
        assert report["status"] == "failed"
        assert "fixture manifest not found" in report["failure_reason"]

    def test_failure_report_emitted_on_invalid_yaml(self, tmp_path: Path) -> None:
        manifest = tmp_path / "broken.yaml"
        manifest.write_text("scenario_id: broken\nexpected_phases: [unbalanced",  encoding="utf-8")
        report_path = tmp_path / "report.json"

        rc = min_cycle_main(
            _build_argv(
                fixture=manifest,
                run_dir=tmp_path / "run",
                report=report_path,
            )
        )

        assert rc != 0
        report = json.loads(report_path.read_text())
        assert report["status"] == "failed"


class TestPlaceholderArtifacts:
    def test_each_required_artifact_emits_placeholder_file(
        self, tmp_path: Path
    ) -> None:
        manifest = _write_minimal_cycle_manifest(
            tmp_path / "manifest.yaml",
            required_artifacts=["cycle_summary"],
            scenario_id="my-scenario",
        )
        run_dir = tmp_path / "run"
        report_path = tmp_path / "report.json"

        min_cycle_main(
            _build_argv(fixture=manifest, run_dir=run_dir, report=report_path)
        )

        artifact = run_dir / "cycle_summary.placeholder.json"
        assert artifact.is_file()
        payload = json.loads(artifact.read_text())
        assert payload["kind"] == "cycle_summary"
        assert payload["scenario_id"] == "my-scenario"
        assert payload["profile_id"] == "lite-local"
        assert payload["real_phase_execution"] is False


class TestAgainstAssemblyRealFixture:
    """End-to-end against the actual assembly fixture sitting in workspace.

    Only runs when the assembly repo is available next to orchestrator.
    Skips otherwise so this test file remains useful in isolated CI.
    """

    REAL_MANIFEST = (
        Path(__file__).resolve().parents[3]
        / "assembly"
        / "src"
        / "assembly"
        / "tests"
        / "e2e"
        / "fixtures"
        / "minimal_cycle"
        / "manifest.yaml"
    )

    def test_real_assembly_manifest_runs_through(self, tmp_path: Path) -> None:
        if not self.REAL_MANIFEST.is_file():
            pytest.skip(f"assembly fixture not found at {self.REAL_MANIFEST}")

        report_path = tmp_path / "report.json"
        rc = min_cycle_main(
            _build_argv(
                fixture=self.REAL_MANIFEST,
                run_dir=tmp_path / "run",
                report=report_path,
            )
        )

        assert rc == 0, report_path.read_text() if report_path.is_file() else "no report"
        report = json.loads(report_path.read_text())
        assert report["status"] == "success"
        assert "execute-minimal-cycle" in report["phases"]
        assert "cycle_summary" in report["artifacts"]
