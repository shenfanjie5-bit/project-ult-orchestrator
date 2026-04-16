"""Manual partial-rerun request CLI."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from orchestrator.rerun import plan_partial_rerun
from orchestrator.rerun_request import (
    DEFAULT_REQUEST_DIR,
    request_from_plan,
    write_rerun_request,
)


def main(argv: Sequence[str] | None = None) -> int:
    """Generate a manual rerun request for the Dagster sensor to consume."""

    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        plan = plan_partial_rerun(args.run_id, args.failed_node)
    except Exception as exc:
        print(f"failed to plan partial rerun: {exc}", file=sys.stderr)
        return 2

    request = request_from_plan(plan)
    if args.dry_run:
        print(json.dumps(request, sort_keys=True))
        return 0

    try:
        request_path = write_rerun_request(plan, args.request_dir)
    except OSError as exc:
        print(f"failed to write rerun request: {exc}", file=sys.stderr)
        return 1

    print(str(request_path))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="orchestrator-rerun",
        description="Create a manual partial-rerun request for orchestrator.",
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--failed-node", required=True)
    parser.add_argument(
        "--request-dir",
        default=DEFAULT_REQUEST_DIR,
        help=f"directory for rerun request JSON files (default: {DEFAULT_REQUEST_DIR})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the request JSON without writing a request file",
    )
    return parser

if __name__ == "__main__":
    raise SystemExit(main())
