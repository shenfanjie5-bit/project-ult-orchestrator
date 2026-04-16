"""Manual partial-rerun request CLI."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from orchestrator.rerun import PartialRerunPlan, plan_partial_rerun

DEFAULT_REQUEST_DIR = ".orchestrator/rerun_requests"


def main(argv: Sequence[str] | None = None) -> int:
    """Generate a manual rerun request for the Dagster sensor to consume."""

    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        plan = plan_partial_rerun(args.run_id, args.failed_node)
    except Exception as exc:
        print(f"failed to plan partial rerun: {exc}", file=sys.stderr)
        return 2

    request = _request_from_plan(plan)
    if args.dry_run:
        print(json.dumps(request, sort_keys=True))
        return 0

    request_dir = Path(args.request_dir)
    request_path = request_dir / _request_filename(plan.run_id, plan.failed_node)
    try:
        request_dir.mkdir(parents=True, exist_ok=True)
        request_path.write_text(
            json.dumps(request, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
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


def _request_from_plan(plan: PartialRerunPlan) -> dict[str, Any]:
    return {
        "run_id": plan.run_id,
        "failed_node": plan.failed_node,
        "rerun_selection": list(plan.rerun_selection),
        "requires_manual_ack": plan.requires_manual_ack,
        "rerun_mode": plan.rerun_mode,
        "generated_at": plan.generated_at.isoformat(),
    }


def _request_filename(run_id: str, failed_node: str) -> str:
    safe_run_id = _safe_filename_part(run_id)
    safe_failed_node = _safe_filename_part(failed_node)
    return f"{safe_run_id}-{safe_failed_node}.json"


def _safe_filename_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value).strip("._") or "request"


if __name__ == "__main__":
    raise SystemExit(main())

