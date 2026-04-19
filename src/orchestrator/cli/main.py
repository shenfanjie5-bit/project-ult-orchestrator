"""Top-level orchestrator CLI dispatcher."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch orchestrator CLI subcommands."""

    parser = _build_parser()
    args, remaining = parser.parse_known_args(argv)

    if args.command == "diag":
        from orchestrator.cli import diag

        return diag.main(remaining)

    if args.command == "min-cycle":
        from orchestrator.cli import min_cycle

        return min_cycle.main(remaining)

    parser.print_help(sys.stderr)
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="orchestrator",
        description="Orchestrator operational commands.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("diag", help="collect run diagnostics")
    subparsers.add_parser(
        "min-cycle",
        help="run a minimal cross-module cycle for assembly e2e",
    )
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
