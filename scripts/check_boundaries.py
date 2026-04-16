from __future__ import annotations

import argparse
import ast
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path


FORBIDDEN_RUNTIME_IMPORT_ROOTS: tuple[str, ...] = (
    "data_platform",
    "graph_engine",
    "main_core",
    "audit_eval",
    "stream_layer",
    "kafka",
    "flink",
)

BUSINESS_ALGORITHM_KEYWORDS: tuple[str, ...] = (
    "pagerank",
    "embedding",
    "feature_",
    "alpha_",
    "factor_",
    "rank_score",
    "recommendation_score",
)

DEFAULT_SIBLING_MODULES: tuple[str, ...] = (
    "data-platform",
    "graph-engine",
    "main-core",
    "audit-eval",
    "reasoner-runtime",
    "stream-layer",
)


@dataclass(frozen=True, slots=True)
class BoundaryViolation:
    path: Path
    line: int
    code: str
    message: str


def scan_orchestrator_boundaries(root: Path) -> list[BoundaryViolation]:
    source_root = root / "src" / "orchestrator"
    violations: list[BoundaryViolation] = []

    for path, tree in _parse_python_files(source_root):
        violations.extend(_scan_forbidden_imports(path, tree))
        violations.extend(_scan_business_keywords(path, tree))

    return violations


def scan_reverse_imports(
    workspace_root: Path,
    module_names: Iterable[str],
) -> list[BoundaryViolation]:
    violations: list[BoundaryViolation] = []

    for module_name in module_names:
        source_root = workspace_root / module_name / "src"
        if not source_root.exists():
            continue

        for path, tree in _parse_python_files(source_root):
            for imported_module, line in _absolute_imports(tree):
                if _matches_module_root(imported_module, ("orchestrator",)):
                    violations.append(
                        BoundaryViolation(
                            path=path,
                            line=line,
                            code="reverse-import",
                            message=(
                                f"sibling module {module_name!r} imports "
                                f"orchestrator runtime module {imported_module!r}"
                            ),
                        )
                    )

    return violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check orchestrator module boundary rules.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Current orchestrator repository root. Defaults to cwd.",
    )
    args = parser.parse_args(argv)

    root = args.root.resolve()
    workspace_root = root.parent
    violations = [
        *scan_orchestrator_boundaries(root),
        *scan_reverse_imports(workspace_root, DEFAULT_SIBLING_MODULES),
    ]

    for violation in violations:
        print(_format_violation(violation, (root, workspace_root)))

    return 1 if violations else 0


def _parse_python_files(source_root: Path) -> Iterator[tuple[Path, ast.AST]]:
    if not source_root.exists():
        return

    for path in sorted(source_root.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        yield path, ast.parse(source, filename=str(path))


def _scan_forbidden_imports(
    path: Path,
    tree: ast.AST,
) -> list[BoundaryViolation]:
    violations: list[BoundaryViolation] = []

    for module_name, line in _absolute_imports(tree):
        if _matches_module_root(module_name, FORBIDDEN_RUNTIME_IMPORT_ROOTS):
            violations.append(
                BoundaryViolation(
                    path=path,
                    line=line,
                    code="forbidden-import",
                    message=f"forbidden runtime import {module_name!r}",
                )
            )

    return violations


def _scan_business_keywords(
    path: Path,
    tree: ast.AST,
) -> list[BoundaryViolation]:
    violations: list[BoundaryViolation] = []

    for value, line, subject in _business_keyword_subjects(tree):
        if keyword := _matched_business_keyword(value):
            violations.append(
                BoundaryViolation(
                    path=path,
                    line=line,
                    code="business-keyword",
                    message=(
                        f"business algorithm keyword {keyword!r} found in "
                        f"{subject} {value!r}"
                    ),
                )
            )

    return violations


def _absolute_imports(tree: ast.AST) -> Iterator[tuple[str, int]]:
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name, node.lineno
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            yield node.module, node.lineno


def _business_keyword_subjects(tree: ast.AST) -> Iterator[tuple[str, int, str]]:
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node.name, node.lineno, "function name"
        elif isinstance(node, ast.ClassDef):
            yield node.name, node.lineno, "class name"
        elif isinstance(node, ast.arg):
            yield node.arg, node.lineno, "argument name"
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            yield node.id, node.lineno, "variable name"
        elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Store):
            yield node.attr, node.lineno, "attribute name"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name.rsplit(".", maxsplit=1)[-1], node.lineno, "import name"
                if alias.asname:
                    yield alias.asname, node.lineno, "import alias"
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                yield alias.name, node.lineno, "import name"
                if alias.asname:
                    yield alias.asname, node.lineno, "import alias"
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.value, node.lineno, "string constant"


def _matched_business_keyword(value: str) -> str | None:
    lowered = value.lower()
    for keyword in BUSINESS_ALGORITHM_KEYWORDS:
        if keyword in lowered:
            return keyword
    return None


def _matches_module_root(module_name: str, roots: Iterable[str]) -> bool:
    return any(
        module_name == root or module_name.startswith(f"{root}.")
        for root in roots
    )


def _format_violation(violation: BoundaryViolation, roots: Iterable[Path]) -> str:
    path = violation.path
    for root in roots:
        try:
            path = violation.path.relative_to(root)
            break
        except ValueError:
            continue
    return f"{path}:{violation.line}: {violation.code}: {violation.message}"


if __name__ == "__main__":
    raise SystemExit(main())
