"""Boundary tests for orchestrator red lines (per §10 STANDARD + CLAUDE.md).

Three red-line checks:

1. **No business logic in min-cycle** — orchestrator CLAUDE.md:
   "不引入业务逻辑". min_cycle.py must not import any L4-L7 / business
   module (main_core, data_platform business runtime, reasoner_runtime
   client builders, etc.) at module-import time.
2. **No Kafka / Flink / CEP** — orchestrator CLAUDE.md: Temporal /
   stream-layer is P11+ extension; current default path must not pull
   in those imports anywhere in the package.
3. **public.py 边界** — subprocess-isolated import deny scan
   (iron rule #2): no business module / no LLM / no Dagster heavy import
   at public.py load time.
"""

from __future__ import annotations

import ast
import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


# ── #1 min-cycle 不引入业务逻辑 ────────────────────────────────


class TestMinCycleNoBusinessImports:
    """AST-scan min_cycle.py: must not statically import any business
    runtime module. If a future refactor pulls in main_core or
    data_platform.serving, this catches it.
    """

    SRC_FILE = Path(__file__).resolve().parents[2] / "src" / "orchestrator" / "cli" / "min_cycle.py"
    FORBIDDEN_PREFIXES = (
        "main_core",
        "data_platform.serving",
        "data_platform.queue",
        "data_platform.cycle",
        "graph_engine",
        "audit_eval",
        "entity_registry",
        "reasoner_runtime",
        "subsystem_sdk",
        "subsystem_announcement",
        "subsystem_news",
    )

    def test_min_cycle_does_not_import_business_runtime(self) -> None:
        violations: list[str] = []
        tree = ast.parse(self.SRC_FILE.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for prefix in self.FORBIDDEN_PREFIXES:
                    if module == prefix or module.startswith(prefix + "."):
                        violations.append(
                            f"line {node.lineno}: from {module}"
                        )
            if isinstance(node, ast.Import):
                for alias in node.names:
                    for prefix in self.FORBIDDEN_PREFIXES:
                        if alias.name == prefix or alias.name.startswith(prefix + "."):
                            violations.append(
                                f"line {node.lineno}: import {alias.name}"
                            )
        assert not violations, (
            "min_cycle.py must not import business runtime modules "
            "(orchestrator CLAUDE.md '不引入业务逻辑'):\n" + "\n".join(violations)
        )


# ── #2 No Kafka / Flink / CEP anywhere ────────────────────────


class TestNoStreamLayerImports:
    """Stream-layer is P11+. Current orchestrator default path must
    have zero static imports of Kafka / Flink / CEP. AST scan over the
    entire src/orchestrator package.
    """

    SRC_DIR = Path(__file__).resolve().parents[2] / "src" / "orchestrator"
    STREAM_LAYER_PREFIXES = (
        "kafka",
        "confluent_kafka",
        "pyflink",
        "flink",
    )

    def test_no_kafka_or_flink_imports(self) -> None:
        violations: list[str] = []
        for py_path in self.SRC_DIR.rglob("*.py"):
            tree = ast.parse(py_path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    for prefix in self.STREAM_LAYER_PREFIXES:
                        if module == prefix or module.startswith(prefix + "."):
                            violations.append(
                                f"{py_path.name}:{node.lineno}: from {module}"
                            )
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        for prefix in self.STREAM_LAYER_PREFIXES:
                            if alias.name == prefix or alias.name.startswith(prefix + "."):
                                violations.append(
                                    f"{py_path.name}:{node.lineno}: import {alias.name}"
                                )
        assert not violations, (
            "Kafka / Flink / CEP belongs to stream-layer (P11+); "
            "orchestrator must not import them yet:\n" + "\n".join(violations)
        )


# ── #3 public.py 边界（subprocess-isolated）─────────────────

_BUSINESS_DOWNSTREAMS = (
    "main_core", "data_platform", "graph_engine", "audit_eval",
    "entity_registry", "reasoner_runtime",
    "subsystem_sdk", "subsystem_announcement", "subsystem_news",
    "assembly", "feature_store", "stream_layer",
)
_HEAVY_RUNTIME_PREFIXES = (
    "psycopg", "pyiceberg", "neo4j",
    "litellm", "openai", "anthropic",
    "torch", "tensorflow",
    "dagster",  # dagster import is heavy; public.py must stay light
)
_PROBE_SCRIPT = textwrap.dedent(
    """
    import json, sys
    sys.path.insert(0, {src_dir!r})
    import orchestrator.public  # noqa: F401
    print(json.dumps(sorted(sys.modules.keys())))
    """
).strip()


@pytest.fixture(scope="module")
def loaded_modules_in_clean_subprocess() -> frozenset[str]:
    src_dir = str(Path(__file__).resolve().parents[2] / "src")
    result = subprocess.run(
        [sys.executable, "-c", _PROBE_SCRIPT.format(src_dir=src_dir)],
        check=False, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise AssertionError("subprocess probe failed; stderr:\n" + result.stderr)
    return frozenset(json.loads(result.stdout))


class TestPublicNoBusinessImports:
    def test_public_pulls_in_no_business_module(
        self, loaded_modules_in_clean_subprocess: frozenset[str]
    ) -> None:
        offenders = sorted(
            mod for mod in loaded_modules_in_clean_subprocess
            if any(mod == p or mod.startswith(p + ".") for p in _BUSINESS_DOWNSTREAMS)
        )
        assert not offenders, f"public pulled in business module(s): {offenders}"

    def test_public_pulls_in_no_heavy_infra(
        self, loaded_modules_in_clean_subprocess: frozenset[str]
    ) -> None:
        offenders = sorted(
            mod for mod in loaded_modules_in_clean_subprocess
            if any(mod == p or mod.startswith(p + ".") for p in _HEAVY_RUNTIME_PREFIXES)
        )
        assert not offenders, f"public pulled in heavy infra module(s): {offenders}"
