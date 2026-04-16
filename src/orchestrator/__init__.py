"""Dagster orchestration package for project-ult."""

__version__ = "0.1.0"

_PUBLIC_API_IMPORTS = {
    "build_daily_cycle_jobs": ("orchestrator.jobs.cycle", "build_daily_cycle_jobs"),
    "build_definitions": ("orchestrator.definitions", "build_definitions"),
    "build_resource_bundle": ("orchestrator.resources", "build_resource_bundle"),
    "classify_gate_result": ("orchestrator.checks", "classify_gate_result"),
    "load_gate_policy": ("orchestrator.policy", "load_gate_policy"),
    "plan_partial_rerun": ("orchestrator.rerun", "plan_partial_rerun"),
}


def __getattr__(name: str) -> object:
    if name in _PUBLIC_API_IMPORTS:
        from importlib import import_module

        module_name, attribute_name = _PUBLIC_API_IMPORTS[name]
        return getattr(import_module(module_name), attribute_name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "build_daily_cycle_jobs",
    "build_definitions",
    "build_resource_bundle",
    "classify_gate_result",
    "load_gate_policy",
    "plan_partial_rerun",
]
