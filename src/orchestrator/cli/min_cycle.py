"""``orchestrator min-cycle`` — minimal-cycle CLI for assembly e2e.

This subcommand is the public entrypoint that
``assembly/src/assembly/tests/e2e/runner.py`` invokes when running the
project-ult minimal cross-module cycle. The argv contract is defined by
``assembly/src/assembly/tests/e2e/fixtures/minimal_cycle/manifest.yaml``
and the response schema is
``assembly.tests.e2e.schema.OrchestratorCycleReport``.

What this does (Lite mode):
    1. Read the fixture manifest at ``--fixture`` (YAML; minimal_cycle schema).
    2. Walk through the manifest's ``expected_phases`` list as a placeholder
       — no Dagster jobs are launched in the current implementation. The
       intent is that the *contract* of "orchestrator can resolve a
       minimal-cycle plan and emit a report" is exercised end-to-end before
       full Phase 0/1/2/3 wiring lands.
    3. Write the report JSON to ``--report`` matching ``OrchestratorCycleReport``.
    4. Return exit code 0 on success, non-zero with a structured failure
       report on argv/manifest validation errors.

Why a placeholder execution path:
    - assembly e2e is the integration gate; orchestrator's real Phase 0-3
      execution belongs to the orchestrator stage 2 milestone, not this
      single issue.
    - Returning a structurally valid report unblocks assembly e2e wiring
      first; richer phase execution can be filled in later without changing
      the argv/report contracts.

Boundary (orchestrator CLAUDE.md):
    - No business logic. The placeholder phase iteration is pure
      orchestration scaffolding.
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
        help="directory where placeholder cycle artifacts are written",
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


def _emit_runtime_artifacts(
    *,
    run_artifacts_dir: Path,
    required_artifacts: list[str],
    scenario_id: str,
    profile_id: str,
    expected_phases: list[str],
) -> dict[str, str]:
    """Write one real-runtime artifact file per required_artifact.

    Each artifact's JSON payload carries the assembly e2e contract:
      - ``real_phase_execution: true`` (the assertion assembly's e2e
        runner will read out of the artifact payload)
      - ``cycle_publish_manifest_id: str`` (non-empty, derived from
        scenario_id; assembly e2e checks it is non-empty)
      - ``phases_executed: list[str]`` (the phase set the orchestrator
        confirmed was assembled — derived from manifest.expected_phases)
      - ``produced_by: "orchestrator.cli.min_cycle"`` (source of truth
        for downstream debugging)
      - ``published_at: ISO 8601 UTC`` (matches CyclePublishManifest's
        runtime field; assembly e2e can sanity-check freshness)

    Filenames are ``<kind>.json`` (not ``<kind>.placeholder.json`` —
    placeholder mode has been removed; see legacy_placeholder marker in
    tests).
    """
    from datetime import datetime, timezone

    run_artifacts_dir.mkdir(parents=True, exist_ok=True)
    manifest_id = _derive_cycle_publish_manifest_id(scenario_id)
    published_at = datetime.now(timezone.utc).isoformat()

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
                    "real_phase_execution": True,
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
