"""Public integration entrypoints for assembly compatibility checks.

Mirrors the audit-eval / contracts / reasoner-runtime / main-core /
data-platform public.py templates. Five module-level singletons
referenced by ``assembly/module-registry.yaml`` ``module_id:
orchestrator``:

- ``health_probe``  — verifies the orchestrator package boundary loads
  and the four public-API namespaces (cli / definitions / rerun /
  diagnostics) are importable
- ``smoke_hook``    — verifies the ``min-cycle`` CLI entrypoint exists
  and the orchestrator __version__ surfaces (no Dagster job material-
  ization, no PG/Iceberg call — stays under the smoke budget)
- ``init_hook``     — no-op (Dagster resources / PG sessions are owned
  by Dagster's own RunRequest cycle, not by orchestrator bootstrap)
- ``version_declaration`` — returns module + contract version
- ``cli``           — argparse-based dispatcher with a ``version``
  subcommand (separate from ``orchestrator`` console script which
  routes to diag / min-cycle / rerun)

Boundary (orchestrator CLAUDE.md):
- This module does NOT define any L4-L7 business logic
- This module does NOT pull in Kafka / Flink / CEP at import time
- This module does NOT define gate types — those come from `contracts`
- This module does NOT make any LLM call
"""

from __future__ import annotations

import argparse
import time
from typing import Any

from orchestrator import __version__ as _MODULE_VERSION

_MODULE_ID = "orchestrator"
# Stage 4 §4.1.5: contract_version is the canonical contracts schema version
# this module is bound against (NOT this module's own package version, which
# stays in module_version). Harmonized to v0.1.3 across all 11 active
# subsystem modules so assembly's ContractsVersionCheck (strict equality vs
# matrix.contract_version) succeeds at the cross-project compat audit
# (assembly/scripts/stage_3_compat_audit.py + Stage 4 §4.1 registry).
_CONTRACT_VERSION = "v0.1.3"
_COMPATIBLE_CONTRACT_RANGE = ">=0.1.0,<0.2.0"


class _HealthProbe:
    """Health probe — confirms the orchestrator package is importable
    and the four public-API namespaces load cleanly. No Dagster
    materialization, no infrastructure call.
    """

    _PROBE_NAME = "orchestrator.import"

    def check(self, *, timeout_sec: float) -> dict[str, Any]:
        start = time.monotonic()
        details: dict[str, Any] = {"timeout_sec": timeout_sec}
        try:
            import orchestrator.cli.main  # noqa: F401
            import orchestrator.cli.min_cycle  # noqa: F401
            import orchestrator.cli.diag  # noqa: F401

            details["public_namespaces"] = [
                "orchestrator.cli.main",
                "orchestrator.cli.min_cycle",
                "orchestrator.cli.diag",
            ]
            status = "healthy"
            message = "orchestrator package import healthy"
        except Exception as exc:  # pragma: no cover - degraded path
            status = "degraded"
            message = f"orchestrator import degraded: {exc!s}"
            details["error_type"] = type(exc).__name__
        latency_ms = (time.monotonic() - start) * 1000.0
        return {
            "module_id": _MODULE_ID,
            "probe_name": self._PROBE_NAME,
            "status": status,
            "latency_ms": latency_ms,
            "message": message,
            "details": details,
        }


class _SmokeHook:
    """Smoke hook — exercises the ``min-cycle`` CLI entrypoint exists
    and is callable. Does NOT invoke a real cycle (that requires a
    fixture path). Stays under 1-second smoke budget.
    """

    _HOOK_NAME = "orchestrator.cli-smoke"

    def run(self, *, profile_id: str) -> dict[str, Any]:
        start = time.monotonic()
        try:
            from orchestrator.cli.main import main as orchestrator_main
            from orchestrator.cli.min_cycle import main as min_cycle_main

            assert callable(orchestrator_main), "orchestrator main not callable"
            assert callable(min_cycle_main), "min-cycle main not callable"

            duration_ms = (time.monotonic() - start) * 1000.0
            return {
                "module_id": _MODULE_ID,
                "hook_name": self._HOOK_NAME,
                "passed": True,
                "duration_ms": duration_ms,
                "failure_reason": None,
                "details": {
                    "profile_id": profile_id,
                    "cli_entrypoints_checked": 2,
                },
            }
        except Exception as exc:
            duration_ms = (time.monotonic() - start) * 1000.0
            return {
                "module_id": _MODULE_ID,
                "hook_name": self._HOOK_NAME,
                "passed": False,
                "duration_ms": duration_ms,
                "failure_reason": f"orchestrator smoke failed: {exc!s}",
                "details": {"profile_id": profile_id},
            }


class _InitHook:
    """Init hook — no-op.

    Dagster owns its own RunRequest / instance / resource lifecycle.
    orchestrator does not bootstrap PG / Iceberg sessions — those are
    Dagster resource concerns instantiated lazily inside Dagster jobs.
    """

    def initialize(self, *, resolved_env: dict[str, str]) -> None:
        _ = resolved_env  # explicit unused-binding to silence linters
        return None


class _VersionDeclaration:
    """Version declaration — single source of truth for module + contract version."""

    def declare(self) -> dict[str, Any]:
        return {
            "module_id": _MODULE_ID,
            "module_version": _MODULE_VERSION,
            "contract_version": _CONTRACT_VERSION,
            "compatible_contract_range": _COMPATIBLE_CONTRACT_RANGE,
        }


class _Cli:
    """CLI entrypoint — argparse dispatcher for assembly-facing
    subcommands.

    Subcommands:
      * ``version`` — print module/contract version (default).
      * ``min-cycle`` — delegate to ``orchestrator.cli.min_cycle.main``,
        the minimal-cycle CLI assembly e2e (``run_min_cycle_e2e``)
        invokes via the public ``cli`` entrypoint registered in
        ``assembly/module-registry.yaml``. Stage 4 §4.3 prerequisite:
        without this dispatch the assembly e2e fails with
        ``orchestrator: error: argument subcommand: invalid choice:
        'min-cycle'`` and the §4.2 positive regression cannot reach
        ``status="success"``. Remaining argv after ``min-cycle`` is
        forwarded verbatim (``--profile``, ``--fixture``, etc.).

    Returns POSIX exit codes (0 ok, 2 invalid usage). The argv
    parameter is positional-or-keyword to match the assembly
    ``CliEntrypoint`` protocol.

    NOT to be confused with ``orchestrator.cli.main:main`` (the full
    operational CLI dispatching diag / min-cycle / rerun via Click);
    this one is the assembly-integration public-protocol entrypoint.
    """

    _PROG = "orchestrator"

    def invoke(self, argv: list[str]) -> int:
        # ``min-cycle`` has its own argparse parser — pre-dispatch
        # before the outer parser tries to validate ``--profile`` etc.
        # as outer flags.
        if argv and argv[0] == "min-cycle":
            from orchestrator.cli.min_cycle import main as min_cycle_main

            return int(min_cycle_main(argv[1:]) or 0)

        parser = argparse.ArgumentParser(
            prog=self._PROG,
            description="orchestrator public CLI (assembly integration)",
        )
        parser.add_argument(
            "subcommand",
            nargs="?",
            default="version",
            choices=("version", "min-cycle"),
            help="subcommand to run (default: version)",
        )
        try:
            args = parser.parse_args(argv)
        except SystemExit as exc:
            return int(exc.code) if exc.code is not None else 2

        if args.subcommand == "version":
            info = _VersionDeclaration().declare()
            print(
                f"{info['module_id']} {info['module_version']} "
                f"(contract {info['contract_version']})"
            )
            return 0
        return 2


# Module-level singletons — names referenced by
# assembly/module-registry.yaml ("orchestrator.public:health_probe", ...).
health_probe: _HealthProbe = _HealthProbe()
smoke_hook: _SmokeHook = _SmokeHook()
init_hook: _InitHook = _InitHook()
version_declaration: _VersionDeclaration = _VersionDeclaration()
cli: _Cli = _Cli()


__all__ = [
    "cli",
    "health_probe",
    "init_hook",
    "smoke_hook",
    "version_declaration",
]
