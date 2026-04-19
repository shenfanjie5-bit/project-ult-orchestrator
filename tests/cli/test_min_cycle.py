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


class TestRuntimeArtifacts:
    """Per-artifact runtime payload contract (post-upgrade). The
    placeholder mode has been removed in the
    `upgrade-min-cycle-real-execution` issue; artifacts now carry real
    Phase 0-3 assembly metadata.
    """

    def test_each_required_artifact_emits_runtime_file(
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

        # Filename is <kind>.json (no `.placeholder.` suffix).
        artifact = run_dir / "cycle_summary.json"
        assert artifact.is_file()
        payload = json.loads(artifact.read_text())
        assert payload["kind"] == "cycle_summary"
        assert payload["scenario_id"] == "my-scenario"
        assert payload["profile_id"] == "lite-local"
        # Post-upgrade: real_phase_execution flips to True.
        assert payload["real_phase_execution"] is True
        # produced_by string drops the placeholder suffix.
        assert payload["produced_by"] == "orchestrator.cli.min_cycle"

    @pytest.mark.skip(
        reason=(
            "Placeholder mode has been removed in the "
            "upgrade-min-cycle-real-execution issue (codex stage 2.4 "
            "follow-up). This skip-marker stays as a tombstone so a "
            "future revival is intentional, not accidental."
        )
    )
    def test_legacy_placeholder_mode_removed_marker(self) -> None:
        """Tombstone for the old placeholder mode.

        Marked legacy_placeholder so CI grep can find it; skipped so it
        doesn't run. If anyone needs to revive placeholder mode, they
        should consciously delete this skip and add the corresponding
        runtime branch.
        """


class TestRealPhaseExecution:
    """Assembly e2e contract: post-upgrade min-cycle artifact payloads
    must signal real Phase 0-3 assembly, not a placeholder run.

    Per stage 2.5 plan + assembly OrchestratorCycleReport schema
    (`extra="forbid"`, only 5 top-level fields), the real-execution
    signal lives in the artifact payload — not in the report top
    level. assembly's e2e runner reads the artifact paths from the
    report's `artifacts` dict and parses each JSON to verify these
    fields.
    """

    def test_artifact_carries_real_phase_execution_true(self, tmp_path: Path) -> None:
        manifest = _write_minimal_cycle_manifest(
            tmp_path / "manifest.yaml",
            required_artifacts=["cycle_summary"],
            scenario_id="real-exec-scenario",
        )
        run_dir = tmp_path / "run"
        report_path = tmp_path / "report.json"

        min_cycle_main(
            _build_argv(fixture=manifest, run_dir=run_dir, report=report_path)
        )

        report = json.loads(report_path.read_text())
        artifact_path = Path(report["artifacts"]["cycle_summary"])
        payload = json.loads(artifact_path.read_text())
        # Codex stage 2.5 review #1 fix: real_phase_execution is now
        # observed-not-asserted. In a dev venv (dagster present) this
        # MUST be True AND the assembled_job_names list must be
        # non-empty (proves real Dagster job builder ran, not a stub).
        assert payload["real_phase_execution"] is True, (
            f"real_phase_execution should be True when dagster is "
            f"installed; assembly_error={payload.get('assembly_error')!r}"
        )
        assembled = payload["assembled_job_names"]
        assert isinstance(assembled, list) and assembled, (
            f"assembled_job_names should be non-empty list when assembly "
            f"succeeds; got {assembled!r}"
        )
        assert payload["assembly_error"] is None

    def test_artifact_records_assembly_failure_when_dagster_missing(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """Negative test: when build_daily_cycle_jobs raises (proxy for
        dagster missing in dev venv), the artifact must record
        real_phase_execution=False + a non-None assembly_error.

        Codex stage 2.5 review #1: silent claims of execution must be
        impossible. Monkeypatch the assembly probe to force the failure
        path without uninstalling dagster.
        """
        from orchestrator.cli import min_cycle as min_cycle_module

        def _fake_failure() -> tuple[list[str], str | None]:
            return [], "build_daily_cycle_jobs import failed: simulated"

        monkeypatch.setattr(
            min_cycle_module, "_try_assemble_dagster_jobs", _fake_failure
        )

        manifest = _write_minimal_cycle_manifest(
            tmp_path / "manifest.yaml",
            required_artifacts=["cycle_summary"],
            scenario_id="dagster-missing-scenario",
        )
        run_dir = tmp_path / "run"
        report_path = tmp_path / "report.json"

        rc = min_cycle_main(
            _build_argv(fixture=manifest, run_dir=run_dir, report=report_path)
        )
        # min-cycle returns 0 (report written) regardless — assembly
        # e2e reads artifact payload to surface the real signal. exit
        # code 0 means "report written", not "execution succeeded".
        assert rc == 0

        report = json.loads(report_path.read_text())
        artifact_path = Path(report["artifacts"]["cycle_summary"])
        payload = json.loads(artifact_path.read_text())
        assert payload["real_phase_execution"] is False
        assert payload["assembled_job_names"] == []
        assert "simulated" in payload["assembly_error"]

    def test_artifact_carries_non_empty_cycle_publish_manifest_id(
        self, tmp_path: Path
    ) -> None:
        manifest = _write_minimal_cycle_manifest(
            tmp_path / "manifest.yaml",
            required_artifacts=["cycle_summary"],
            scenario_id="manifest-id-scenario",
        )
        run_dir = tmp_path / "run"
        report_path = tmp_path / "report.json"

        min_cycle_main(
            _build_argv(fixture=manifest, run_dir=run_dir, report=report_path)
        )

        report = json.loads(report_path.read_text())
        artifact_path = Path(report["artifacts"]["cycle_summary"])
        payload = json.loads(artifact_path.read_text())
        manifest_id = payload["cycle_publish_manifest_id"]
        assert isinstance(manifest_id, str) and manifest_id, (
            f"cycle_publish_manifest_id must be non-empty str, got {manifest_id!r}"
        )
        # Stable derivation: same scenario_id always yields same id
        # (lets assembly e2e assert on a known-shape value).
        assert "manifest_id_scenario" in manifest_id, manifest_id

    def test_artifact_phases_executed_matches_manifest_expected_phases(
        self, tmp_path: Path
    ) -> None:
        custom_phases = ["phase-a", "phase-b", "phase-c"]
        manifest = _write_minimal_cycle_manifest(
            tmp_path / "manifest.yaml",
            expected_phases=custom_phases,
            required_artifacts=["cycle_summary"],
        )
        run_dir = tmp_path / "run"
        report_path = tmp_path / "report.json"

        min_cycle_main(
            _build_argv(fixture=manifest, run_dir=run_dir, report=report_path)
        )

        report = json.loads(report_path.read_text())
        artifact_path = Path(report["artifacts"]["cycle_summary"])
        payload = json.loads(artifact_path.read_text())
        assert payload["phases_executed"] == custom_phases

    def test_report_top_level_schema_unchanged(self, tmp_path: Path) -> None:
        """The upgrade MUST NOT add fields to the report top level —
        assembly's OrchestratorCycleReport schema is `extra="forbid"`
        and only accepts profile_id / phases / artifacts / status /
        failure_reason. Any new field at the top level would break
        assembly e2e. New signals belong in the artifact payload only.
        """
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
        assert set(report.keys()) == {
            "profile_id",
            "phases",
            "artifacts",
            "status",
            "failure_reason",
        }, (
            f"report top-level keys drift: got {set(report.keys())}; "
            "OrchestratorCycleReport in assembly is extra='forbid'"
        )


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
