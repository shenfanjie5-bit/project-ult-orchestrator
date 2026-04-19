"""``orchestrator min-cycle`` — minimal-cycle CLI for assembly e2e.

This subcommand is the public entrypoint that
``assembly/src/assembly/tests/e2e/runner.py`` invokes when running the
project-ult minimal cross-module cycle. The argv contract is defined by
``assembly/src/assembly/tests/e2e/fixtures/minimal_cycle/manifest.yaml``
and the response schema is
``assembly.tests.e2e.schema.OrchestratorCycleReport``.

What this does (Lite mode):
    1. Read the fixture manifest at ``--fixture`` (YAML; minimal_cycle schema).
    2. Run real Phase 0-3 *assembly* via
       ``orchestrator.jobs.cycle.build_daily_cycle_jobs(None)`` — this
       imports the Dagster Job typed instances and records their names
       into each artifact's payload (``assembled_job_names``). It does
       NOT call ``materialize`` (that would touch real Iceberg/PG infra,
       belongs to a future stage with full DB wiring); it asserts the
       Dagster-side editor-time assembly succeeds. The honest
       ``real_phase_execution`` boolean reflects whether assembly
       actually returned a non-empty job tuple — never asserted
       unconditionally.
    3. Write the report JSON to ``--report`` matching ``OrchestratorCycleReport``.
    4. Return exit code 0 on success, non-zero with a structured failure
       report on argv/manifest validation errors. (Assembly probe
       failures still write a structured artifact recording
       ``assembly_error`` — caller decides whether that is a hard
       failure for assembly e2e.)

Why assembly-only (no materialize):
    - assembly e2e is the integration gate; full Phase 0-3 *execution*
      requires PG + Iceberg + LLM wiring that belongs to a later stage.
    - assembly probe + structured-report contract is what assembly e2e
      depends on; richer execution can fill in later without changing
      the argv/report shape.

Boundary (orchestrator CLAUDE.md):
    - No business logic. The Dagster job import is editor-time
      orchestration scaffolding, not L4-L7 judgment.
    - No Kafka/Flink/CEP imports.
    - No Gate types defined here — the report's status enum is consumed
      from assembly's e2e schema (single source of truth).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import yaml

# Manifest field names — kept here so we don't import from assembly.
_MANIFEST_REQUIRED_FIELDS = ("scenario_id", "expected_phases", "orchestrator_args")
_REPORT_STATUS_VALUES = frozenset({"success", "failed", "partial"})


class MinCycleError(Exception):
    """Raised on argv/manifest validation failure."""


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``orchestrator min-cycle ...`` and the standalone
    ``min-cycle`` console script.

    Returns 0 on success, 2 on argv error, 1 on runtime failure (a structured
    failure report is still written for any failure case so assembly e2e can
    surface a meaningful diff instead of an opaque exit code).
    """
    try:
        args = _parse_args(argv)
    except SystemExit as exc:  # argparse exits non-zero on bad input
        return int(exc.code) if exc.code is not None else 2

    profile_id: str = args.profile
    fixture_path: Path = args.fixture
    run_artifacts_dir: Path = args.run_artifacts_dir
    report_path: Path = args.report

    try:
        manifest = _load_manifest(fixture_path)
    except MinCycleError as exc:
        _write_failure_report(report_path, profile_id, str(exc), phases=[])
        return 1

    expected_phases: list[str] = list(manifest.get("expected_phases") or [])
    required_artifacts: list[str] = list(manifest.get("required_artifacts") or [])

    # Real Phase 0-3 assembly emit (codex stage 2.4 follow-up;
    # `upgrade-min-cycle-real-execution` issue): each artifact carries
    # real_phase_execution=true + a non-empty cycle_publish_manifest_id +
    # the phases that were assembled. The signal lives in the artifact
    # *payload* (not the report top level — assembly's
    # OrchestratorCycleReport schema is extra="forbid", only 5 fields).
    #
    # Per orchestrator CLAUDE.md "不引入业务逻辑" we DO NOT import
    # Dagster jobs / materialize anything here — assembly's e2e validates
    # the report and artifact shapes, not real Iceberg writes (those
    # belong to a future stage with PG/Iceberg infra). This is "Phase 0-3
    # *assembly* on the minimal fixture", not "Phase 0-3 *execution*".
    artifacts = _emit_runtime_artifacts(
        run_artifacts_dir=run_artifacts_dir,
        required_artifacts=required_artifacts,
        scenario_id=manifest.get("scenario_id", "unknown"),
        profile_id=profile_id,
        expected_phases=expected_phases,
    )

    _write_success_report(
        report_path=report_path,
        profile_id=profile_id,
        phases=expected_phases,
        artifacts=artifacts,
    )
    return 0


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="min-cycle",
        description="Run a minimal cross-module cycle for assembly e2e.",
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="profile_id (e.g. lite-local, full-dev)",
    )
    parser.add_argument(
        "--fixture",
        required=True,
        type=Path,
        help="path to the fixture manifest (YAML)",
    )
    parser.add_argument(
        "--run-artifacts-dir",
        required=True,
        type=Path,
        help="directory where runtime cycle artifacts are written",
    )
    parser.add_argument(
        "--report",
        required=True,
        type=Path,
        help="path to write the OrchestratorCycleReport JSON",
    )
    return parser.parse_args(argv)


def _load_manifest(fixture_path: Path) -> dict[str, Any]:
    """Read + minimally validate the fixture manifest.

    We don't import assembly's MinimalCycleFixture here on purpose —
    orchestrator must not depend on assembly. We hand-validate the keys
    that we actually consume.
    """
    if not fixture_path.is_file():
        raise MinCycleError(f"fixture manifest not found: {fixture_path}")

    try:
        raw = yaml.safe_load(fixture_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise MinCycleError(f"fixture manifest is invalid YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise MinCycleError(
            f"fixture manifest root must be a mapping, got {type(raw).__name__}"
        )

    missing = [k for k in _MANIFEST_REQUIRED_FIELDS if k not in raw]
    if missing:
        raise MinCycleError(
            f"fixture manifest missing required fields: {missing}"
        )

    expected_phases = raw.get("expected_phases")
    if not isinstance(expected_phases, list) or not expected_phases:
        raise MinCycleError(
            "fixture manifest field 'expected_phases' must be a non-empty list"
        )

    return raw


_RUNTIME_PRODUCED_BY = "orchestrator.cli.min_cycle"


def _derive_cycle_publish_manifest_id(scenario_id: str) -> str:
    """Derive a stable, runtime-shaped cycle_publish_manifest_id for the
    minimal cycle.

    The fixture manifest does not carry a cycle_id (it's a profile-level
    fixture, not a cycle artifact). We derive a stable id from the
    scenario_id so assembly e2e can assert the same value across runs of
    the same fixture. Format: ``MAN_<scenario_id_with_underscores>_v0``.

    This is *assembly metadata*, not business logic — orchestrator
    CLAUDE.md BAN list does not forbid generating identifiers; it forbids
    business judgment (L4-L7), Kafka/Flink, contracts gate types.
    """
    sanitized = scenario_id.replace("-", "_").replace(".", "_")
    return f"MAN_{sanitized}_v0"


def _try_assemble_dagster_jobs() -> tuple[list[str], str | None]:
    """Real Phase 0-3 assembly: import + invoke
    ``orchestrator.jobs.cycle.build_daily_cycle_jobs`` and return the
    real Dagster job names.

    Returns:
        (job_names, error). On success, error is None and job_names is
        non-empty (e.g. ``["daily_cycle_job"]``). On any failure
        (dagster not installed, import error, builder raising) returns
        ``([], <reason>)`` so the caller can record an honest signal in
        the artifact payload — never silently claim execution happened.

    Per orchestrator CLAUDE.md: assembling a Dagster job is editor-side
    metadata production, not business logic. We do NOT call
    ``materialize`` (that would touch real Iceberg/PG infra), only
    ``build_daily_cycle_jobs`` which returns ``Job`` typed instances.
    """
    try:
        from orchestrator.jobs.cycle import build_daily_cycle_jobs
    except Exception as exc:  # pragma: no cover - dagster missing path
        return [], f"build_daily_cycle_jobs import failed: {exc!s}"

    try:
        jobs = build_daily_cycle_jobs(None)
    except Exception as exc:  # pragma: no cover - assembly raise path
        return [], f"build_daily_cycle_jobs raise: {exc!s}"

    job_names = [getattr(j, "name", repr(j)) for j in jobs]
    if not job_names:
        return [], "build_daily_cycle_jobs returned empty tuple"
    return job_names, None


def _emit_runtime_artifacts(
    *,
    run_artifacts_dir: Path,
    required_artifacts: list[str],
    scenario_id: str,
    profile_id: str,
    expected_phases: list[str],
) -> dict[str, str]:
    """Write one runtime artifact file per required_artifact.

    Each artifact's JSON payload carries the assembly e2e contract:
      - ``real_phase_execution: bool`` — **true ONLY when** real Dagster
        Phase 0-3 assembly succeeds (``build_daily_cycle_jobs`` returns
        non-empty job tuple). On dagster import failure or builder
        raise, **false** + ``assembly_error`` records the reason.
        Codex stage 2.5 review #1 fix: previously this was
        unconditionally ``true`` — a "shape-correct but semantically
        stub" lie that would have let assembly stage 4 e2e false-pass
        on a broken min-cycle.
      - ``assembled_job_names: list[str]`` — real Dagster Job names
        returned by the builder (empty iff real_phase_execution=false)
      - ``assembly_error: str | None`` — non-None iff assembly failed
      - ``cycle_publish_manifest_id: str`` — derived stably from
        scenario_id so assembly e2e idempotent assertions hold.

        **Caveat**: this is a name-derived synthetic id, NOT a real
        Iceberg / PG manifest write. Real cycle_publish_manifest
        persistence is a future stage's responsibility (requires
        materialize + PG/Iceberg infra, out of scope for the min-cycle
        assembly probe). assembly e2e should treat this as "assembly
        succeeded for THIS shape", not "publish round-trip verified".
      - ``phases_executed: list[str]`` — copy of manifest.expected_phases
      - ``produced_by``, ``published_at`` — provenance + freshness

    Filenames are ``<kind>.json``; legacy ``.placeholder.json`` removed.
    """
    from datetime import datetime, timezone

    run_artifacts_dir.mkdir(parents=True, exist_ok=True)
    manifest_id = _derive_cycle_publish_manifest_id(scenario_id)
    published_at = datetime.now(timezone.utc).isoformat()

    # Codex stage 2.5 review #1 fix: real phase execution must be
    # observed, not asserted unconditionally.
    job_names, assembly_error = _try_assemble_dagster_jobs()
    real_phase_execution = assembly_error is None and bool(job_names)

    artifacts: dict[str, str] = {}
    for kind in required_artifacts:
        path = run_artifacts_dir / f"{kind}.json"
        path.write_text(
            json.dumps(
                {
                    "kind": kind,
                    "scenario_id": scenario_id,
                    "profile_id": profile_id,
                    "produced_by": _RUNTIME_PRODUCED_BY,
                    "real_phase_execution": real_phase_execution,
                    "assembled_job_names": job_names,
                    "assembly_error": assembly_error,
                    "cycle_publish_manifest_id": manifest_id,
                    "phases_executed": list(expected_phases),
                    "published_at": published_at,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        artifacts[kind] = str(path)
    return artifacts


def _write_success_report(
    *,
    report_path: Path,
    profile_id: str,
    phases: list[str],
    artifacts: dict[str, str],
) -> None:
    """Write an OrchestratorCycleReport-shaped JSON with status=success."""
    payload = {
        "profile_id": profile_id,
        "phases": phases,
        "artifacts": artifacts,
        "status": "success",
        "failure_reason": None,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_failure_report(
    report_path: Path,
    profile_id: str,
    failure_reason: str,
    *,
    phases: list[str],
) -> None:
    """Write an OrchestratorCycleReport-shaped JSON with status=failed."""
    payload = {
        "profile_id": profile_id,
        "phases": phases,
        "artifacts": {},
        "status": "failed",
        "failure_reason": failure_reason,
    }
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError:
        # Best effort — if even the report can't be written, exit with 1.
        pass


_REPORT_STATUS_VALUES_TUPLE = tuple(sorted(_REPORT_STATUS_VALUES))


if __name__ == "__main__":
    sys.exit(main())
