from __future__ import annotations

import sys
from types import ModuleType

import pytest

_ORIGINAL_IMPORTORSKIP = pytest.importorskip
_DAGSTER_TOOLCHAIN_MODULES = frozenset({"dagster", "dagster_dbt"})


def _patch_pendulum_compat() -> None:
    try:
        import pendulum
    except ModuleNotFoundError:
        return

    if not hasattr(pendulum, "Pendulum") and hasattr(pendulum, "DateTime"):
        pendulum.Pendulum = pendulum.DateTime  # type: ignore[attr-defined]


_patch_pendulum_compat()


def _clear_partial_import(module_name: str) -> None:
    for loaded_name in tuple(sys.modules):
        if loaded_name == module_name or loaded_name.startswith(f"{module_name}."):
            sys.modules.pop(loaded_name, None)


def _importorskip_with_dagster_toolchain_check(
    modname: str,
    minversion: str | None = None,
    reason: str | None = None,
    *,
    exc_type: type[ImportError] | None = None,
) -> ModuleType:
    try:
        return _ORIGINAL_IMPORTORSKIP(
            modname,
            minversion=minversion,
            reason=reason,
            exc_type=exc_type,
        )
    except Exception as exc:
        root_module = modname.partition(".")[0]
        if root_module not in _DAGSTER_TOOLCHAIN_MODULES:
            raise
        _clear_partial_import(root_module)
        pytest.skip(
            f"{modname} could not be imported; install a compatible Dagster "
            f"toolchain for this Python runtime. Original import error: "
            f"{type(exc).__name__}: {exc}",
        )


pytest.importorskip = _importorskip_with_dagster_toolchain_check  # type: ignore[method-assign]


@pytest.fixture
def empty_fixture() -> None:
    return None
