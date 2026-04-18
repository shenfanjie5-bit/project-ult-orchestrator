import ast
from pathlib import Path


_PHASE0_PATH = (
    Path(__file__).resolve().parents[2] / "src" / "orchestrator" / "jobs" / "phase0.py"
)


def _phase0_module_ast() -> ast.Module:
    return ast.parse(_PHASE0_PATH.read_text(encoding="utf-8"))


def test_dbt_assets_context_parameter_is_unannotated() -> None:
    module = _phase0_module_ast()
    functions = [node for node in module.body if isinstance(node, ast.FunctionDef)]
    dbt_phase0_assets = next(
        node for node in functions if node.name == "dbt_phase0_assets"
    )
    context_arg = dbt_phase0_assets.args.args[0]

    assert context_arg.arg == "context"
    assert context_arg.annotation is None


def test_phase0_does_not_import_asset_execution_context() -> None:
    module = _phase0_module_ast()
    dagster_imports = [
        alias.name
        for node in module.body
        if isinstance(node, ast.ImportFrom) and node.module == "dagster"
        for alias in node.names
    ]

    assert "AssetExecutionContext" not in dagster_imports
