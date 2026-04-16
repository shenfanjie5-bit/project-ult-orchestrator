from __future__ import annotations

from pathlib import Path

from scripts.check_boundaries import (
    DEFAULT_SIBLING_MODULES,
    BoundaryViolation,
    scan_orchestrator_boundaries,
    scan_reverse_imports,
)


_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_current_workspace_has_no_boundary_violations() -> None:
    violations = [
        *scan_orchestrator_boundaries(_REPO_ROOT),
        *scan_reverse_imports(_REPO_ROOT.parent, DEFAULT_SIBLING_MODULES),
    ]

    assert violations == []


def test_business_keyword_violation_in_orchestrator_source(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "orchestrator"
    source_path = repo_root / "src" / "orchestrator" / "business.py"
    _write_source(
        source_path,
        "def compute_pagerank() -> str:\n"
        "    return 'delegated'\n",
    )

    violations = scan_orchestrator_boundaries(repo_root)

    assert _violation_codes(violations) == {"business-keyword"}
    assert violations[0].path == source_path
    assert violations[0].line == 1
    assert "pagerank" in violations[0].message


def test_forbidden_runtime_import_violation_in_orchestrator_source(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "orchestrator"
    source_path = repo_root / "src" / "orchestrator" / "imports.py"
    _write_source(source_path, "import main_core.foo\n")

    violations = scan_orchestrator_boundaries(repo_root)

    assert _violation_codes(violations) == {"forbidden-import"}
    assert violations[0].path == source_path
    assert violations[0].line == 1
    assert "main_core.foo" in violations[0].message


def test_reverse_import_violation_in_sibling_source(tmp_path: Path) -> None:
    source_path = tmp_path / "main-core" / "src" / "main_core" / "flow.py"
    _write_source(
        source_path,
        "from orchestrator.definitions import build_definitions\n",
    )

    violations = scan_reverse_imports(tmp_path, ("main-core",))

    assert _violation_codes(violations) == {"reverse-import"}
    assert violations[0].path == source_path
    assert violations[0].line == 1
    assert "orchestrator.definitions" in violations[0].message


def test_missing_sibling_directory_is_skipped(tmp_path: Path) -> None:
    assert scan_reverse_imports(tmp_path, ("main-core",)) == []


def _write_source(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def _violation_codes(violations: list[BoundaryViolation]) -> set[str]:
    return {violation.code for violation in violations}
