"""Dagster orchestration package for project-ult."""

__version__ = "0.1.1"

_PUBLIC_API_IMPORTS = {
    "PartialRerunNotAllowed": ("orchestrator.rerun", "PartialRerunNotAllowed"),
    "PartialRerunPlan": ("orchestrator.rerun", "PartialRerunPlan"),
    "RunHistorySnapshot": ("orchestrator.rerun", "RunHistorySnapshot"),
    "UnknownFailedNode": ("orchestrator.rerun", "UnknownFailedNode"),
    "build_daily_cycle_jobs": ("orchestrator.jobs.cycle", "build_daily_cycle_jobs"),
    "build_definitions": ("orchestrator.definitions", "build_definitions"),
    "build_resource_bundle": ("orchestrator.resources", "build_resource_bundle"),
    "classify_gate_result": ("orchestrator.checks", "classify_gate_result"),
    "compute_partial_rerun_plan": (
        "orchestrator.rerun",
        "compute_partial_rerun_plan",
    ),
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
    "PartialRerunNotAllowed",
    "PartialRerunPlan",
    "RunHistorySnapshot",
    "UnknownFailedNode",
    "build_daily_cycle_jobs",
    "build_definitions",
    "build_resource_bundle",
    "classify_gate_result",
    "compute_partial_rerun_plan",
    "load_gate_policy",
    "plan_partial_rerun",
]
