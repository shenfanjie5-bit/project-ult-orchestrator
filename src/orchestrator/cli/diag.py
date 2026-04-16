"""Run diagnostic CLI."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from orchestrator.diagnostics import (
    RunDiagnosticNotFound,
    collect_run_diagnostic,
    run_diagnostic_to_dict,
)
from orchestrator.rerun_request import DEFAULT_REQUEST_DIR


def main(argv: Sequence[str] | None = None) -> int:
    """Collect read-only diagnostics for an orchestrator run."""

    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        return _run(args)

    parser.print_help(sys.stderr)
    return 2


def _run(args: argparse.Namespace) -> int:
    try:
        summary = collect_run_diagnostic(
            args.cycle_id,
            request_dir=args.request_dir,
        )
    except RunDiagnosticNotFound as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except Exception as exc:
        print(f"failed to collect run diagnostic: {exc}", file=sys.stderr)
        return 2

    payload = run_diagnostic_to_dict(summary)
    indent = None if args.json else 2
    print(json.dumps(payload, indent=indent, sort_keys=True))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="orchestrator diag",
        description="Collect read-only orchestrator run diagnostics.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run",
        help="summarize Dagster runs, gate decisions, and rerun requests",
    )
    run_parser.add_argument("cycle_id")
    run_parser.add_argument(
        "--request-dir",
        default=DEFAULT_REQUEST_DIR,
        help=f"directory for rerun request JSON files (default: {DEFAULT_REQUEST_DIR})",
    )
    run_parser.add_argument(
        "--json",
        action="store_true",
        help="emit compact JSON output",
    )

    return parser


if __name__ == "__main__":
    raise SystemExit(main())
