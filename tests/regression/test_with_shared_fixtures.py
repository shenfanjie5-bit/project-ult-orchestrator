"""Regression tests consuming the shared ``audit_eval_fixtures`` package.

Per SUBPROJECT_TESTING_STANDARD.md §10 ``orchestrator`` heavy-uses
``minimal_cycle`` for cycle-orchestration assembly invariants.

This module:

1. Walks every minimal_cycle case and asserts the manifest declares 4
   formal-object snapshots orchestrator coordinates publishing for.
2. **Really exercises a runtime function**: drives the
   ``orchestrator.cli.min_cycle.main`` end-to-end with each fixture's
   *manifest* (the assembly e2e fixture, separate from audit-eval
   shared fixtures), then asserts the artifact payload encodes the
   real-phase-execution contract (iron rule #5).
3. Business-expectation regression: the case_001 cycle's expected 4
   phase_results all "ok" maps to the runtime artifact's
   `phases_executed` matching expected_phases (iron rule #5 sub-rule
   from main-core stage 2.3 follow-up #2).

**Hard-import on purpose** (iron rule #1): ImportError bubbles to
pytest collection so ``make regression`` / the regression CI lane fail
loud when shared-fixtures extra is not installed.
"""

from __future__ import annotations

# Hard import — fail collection if shared-fixtures extra not installed.
from audit_eval_fixtures import (  # noqa: F401
    Case,
    CaseRef,
    iter_cases,
    list_packs,
    load_case,
)

# Hard-import the runtime function we exercise so a refactor that drops
# min_cycle from the public path fails at collection (iron rule #5).
from orchestrator.cli.min_cycle import main as min_cycle_main  # noqa: F401


class TestSharedFixturesAreReachable:
    def test_minimal_cycle_pack_present(self) -> None:
        assert "minimal_cycle" in list_packs()


class TestManifestDeclaresFourFormalTables:
    """Every minimal_cycle case's expected.cycle_publish_manifest must
    declare the four formal-object table slots orchestrator coordinates.
    """

    REQUIRED_TABLES = {
        "world_state_snapshot",
        "official_alpha_pool",
        "alpha_result_snapshot",
        "recommendation_snapshot",
    }

    def test_every_case_declares_four_tables(self) -> None:
        for ref in iter_cases("minimal_cycle"):
            case = load_case(ref.pack_name, ref.case_id)
            tables = case.expected.get("cycle_publish_manifest", {}).get(
                "tables", {}
            )
            missing = self.REQUIRED_TABLES - set(tables.keys())
            assert not missing, (
                f"{ref.case_id}: tables missing {missing}"
            )


class TestRuntimeMinCycleAssemblesArtifactsForFixture:
    """**Real-runtime regression** (iron rule #5).

    For each minimal_cycle case, drive ``orchestrator.cli.min_cycle.main``
    with a synthetic fixture manifest (assembly-style; min-cycle only
    needs scenario_id + expected_phases + required_artifacts +
    orchestrator_args). Assert the produced artifacts file the upgraded
    contract: real_phase_execution=True, non-empty
    cycle_publish_manifest_id, phases_executed matches.

    The fixture's ``case.expected.phase_results`` keys (4 phase names)
    drive ``expected_phases`` so a fixture extension automatically
    flows in (codex non-blocking pattern from data-platform follow-up).
    """

    @staticmethod
    def _write_manifest(tmp_path, case) -> "Path":
        import yaml
        from pathlib import Path

        # Use the fixture's expected phase_results keys as the
        # expected_phases — derived, not hard-coded.
        phase_results = case.expected.get("phase_results", {})
        expected_phases = list(phase_results.keys())
        manifest_path = tmp_path / "manifest.yaml"
        manifest_path.write_text(
            yaml.safe_dump(
                {
                    "scenario_id": f"regression-{case.case_id}",
                    "expected_phases": expected_phases,
                    "required_artifacts": ["cycle_summary"],
                    "orchestrator_args": [
                        "min-cycle",
                        "--profile", "{profile_id}",
                        "--fixture", "{fixture_manifest}",
                        "--run-artifacts-dir", "{run_dir}",
                        "--report", "{orchestrator_report_path}",
                    ],
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return manifest_path

    def test_min_cycle_artifacts_signal_real_phase_execution(self, tmp_path) -> None:
        import json
        from pathlib import Path

        exercised_at_least_one = False
        for ref in iter_cases("minimal_cycle"):
            case = load_case(ref.pack_name, ref.case_id)
            phase_results = case.expected.get("phase_results", {})
            if not phase_results:
                continue
            exercised_at_least_one = True

            case_dir = tmp_path / ref.case_id
            case_dir.mkdir(exist_ok=True)
            manifest_path = self._write_manifest(case_dir, case)
            run_dir = case_dir / "run"
            report_path = case_dir / "report.json"

            rc = min_cycle_main(
                [
                    "--profile", "lite-local",
                    "--fixture", str(manifest_path),
                    "--run-artifacts-dir", str(run_dir),
                    "--report", str(report_path),
                ]
            )
            assert rc == 0, f"{ref.case_id}: min-cycle rc={rc}"

            report = json.loads(report_path.read_text())
            assert report["status"] == "success"

            artifact_path = Path(report["artifacts"]["cycle_summary"])
            payload = json.loads(artifact_path.read_text())

            # Real-phase-execution contract.
            assert payload["real_phase_execution"] is True, ref.case_id
            assert payload["cycle_publish_manifest_id"], ref.case_id
            # Business-expectation: phases_executed equals the fixture's
            # phase_results keys (drives downstream report generation).
            assert payload["phases_executed"] == list(phase_results.keys()), (
                f"{ref.case_id}: phases_executed drift; got "
                f"{payload['phases_executed']}, want "
                f"{list(phase_results.keys())}"
            )

        assert exercised_at_least_one, (
            "expected at least one minimal_cycle case with phase_results"
        )

    def test_case_001_artifact_produces_stable_manifest_id(self, tmp_path) -> None:
        """Business-expectation regression for case_001: the manifest_id
        is a stable derivation of scenario_id, so reruns of the same
        fixture yield the same id — assembly e2e relies on this for
        idempotent assertion.
        """
        import json
        from pathlib import Path

        case = load_case("minimal_cycle", "case_001_one_stock_one_cycle")

        # Run twice with the same scenario_id; manifest_id must match.
        observed_ids = []
        for run_idx in (1, 2):
            case_dir = tmp_path / f"run_{run_idx}"
            case_dir.mkdir(exist_ok=True)
            manifest_path = self._write_manifest(case_dir, case)
            run_dir = case_dir / "run"
            report_path = case_dir / "report.json"

            rc = min_cycle_main(
                [
                    "--profile", "lite-local",
                    "--fixture", str(manifest_path),
                    "--run-artifacts-dir", str(run_dir),
                    "--report", str(report_path),
                ]
            )
            assert rc == 0
            report = json.loads(report_path.read_text())
            artifact_path = Path(report["artifacts"]["cycle_summary"])
            payload = json.loads(artifact_path.read_text())
            observed_ids.append(payload["cycle_publish_manifest_id"])

        assert observed_ids[0] == observed_ids[1], (
            f"manifest_id should be stable across reruns of same "
            f"scenario_id; got {observed_ids}"
        )
