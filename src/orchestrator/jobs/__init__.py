"""Dagster job and asset group exports."""

_PUBLIC_JOB_IMPORTS = {
    "AUDIT_EVAL_GROUP_NAME": ("orchestrator.jobs.audit", "AUDIT_EVAL_GROUP_NAME"),
    "RETROSPECTIVE_HOOK_ASSET_KEY": (
        "orchestrator.jobs.audit",
        "RETROSPECTIVE_HOOK_ASSET_KEY",
    ),
    "build_daily_cycle_jobs": (
        "orchestrator.jobs.cycle",
        "build_daily_cycle_jobs",
    ),
    "daily_cycle_job": ("orchestrator.jobs.cycle", "daily_cycle_job"),
    "daily_cycle_phase0_job": (
        "orchestrator.jobs.cycle",
        "daily_cycle_phase0_job",
    ),
    "PHASE0_CANDIDATE_FREEZE_ASSET_KEY": (
        "orchestrator.jobs.phase0",
        "PHASE0_CANDIDATE_FREEZE_ASSET_KEY",
    ),
    "PHASE0_GRAPH_CONSISTENCY_CHECK_NAME": (
        "orchestrator.jobs.phase0",
        "PHASE0_GRAPH_CONSISTENCY_CHECK_NAME",
    ),
    "PHASE0_GRAPH_STATUS_ASSET_KEY": (
        "orchestrator.jobs.phase0",
        "PHASE0_GRAPH_STATUS_ASSET_KEY",
    ),
    "PHASE0_GROUP_NAME": ("orchestrator.jobs.phase0", "PHASE0_GROUP_NAME"),
    "PHASE0_READINESS_ASSET_KEY": (
        "orchestrator.jobs.phase0",
        "PHASE0_READINESS_ASSET_KEY",
    ),
    "PHASE0_REQUIRED_ASSET_KEYS": (
        "orchestrator.jobs.phase0",
        "PHASE0_REQUIRED_ASSET_KEYS",
    ),
    "dbt_phase0_assets": ("orchestrator.jobs.phase0", "dbt_phase0_assets"),
    "phase0_readiness_ping": (
        "orchestrator.jobs.phase0",
        "phase0_readiness_ping",
    ),
    "PHASE1_GRAPH_PROMOTION_ASSET_KEY": (
        "orchestrator.jobs.phase1",
        "PHASE1_GRAPH_PROMOTION_ASSET_KEY",
    ),
    "PHASE1_GRAPH_SNAPSHOT_ASSET_KEY": (
        "orchestrator.jobs.phase1",
        "PHASE1_GRAPH_SNAPSHOT_ASSET_KEY",
    ),
    "PHASE1_GROUP_NAME": ("orchestrator.jobs.phase1", "PHASE1_GROUP_NAME"),
    "PHASE2_GROUP_NAME": ("orchestrator.jobs.phase2", "PHASE2_GROUP_NAME"),
    "PHASE2_STAGE_KEYS": ("orchestrator.jobs.phase2", "PHASE2_STAGE_KEYS"),
    "PHASE3_FORMAL_COMMIT_ASSET_KEY": (
        "orchestrator.jobs.phase3",
        "PHASE3_FORMAL_COMMIT_ASSET_KEY",
    ),
    "PHASE3_GROUP_NAME": ("orchestrator.jobs.phase3", "PHASE3_GROUP_NAME"),
    "PHASE3_MANIFEST_ASSET_KEY": (
        "orchestrator.jobs.phase3",
        "PHASE3_MANIFEST_ASSET_KEY",
    ),
}


def __getattr__(name: str) -> object:
    if name in _PUBLIC_JOB_IMPORTS:
        from importlib import import_module

        module_name, attribute_name = _PUBLIC_JOB_IMPORTS[name]
        return getattr(import_module(module_name), attribute_name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "AUDIT_EVAL_GROUP_NAME",
    "PHASE0_CANDIDATE_FREEZE_ASSET_KEY",
    "PHASE0_GRAPH_CONSISTENCY_CHECK_NAME",
    "PHASE0_GRAPH_STATUS_ASSET_KEY",
    "PHASE0_GROUP_NAME",
    "PHASE0_READINESS_ASSET_KEY",
    "PHASE0_REQUIRED_ASSET_KEYS",
    "PHASE1_GRAPH_PROMOTION_ASSET_KEY",
    "PHASE1_GRAPH_SNAPSHOT_ASSET_KEY",
    "PHASE1_GROUP_NAME",
    "PHASE2_GROUP_NAME",
    "PHASE2_STAGE_KEYS",
    "PHASE3_FORMAL_COMMIT_ASSET_KEY",
    "PHASE3_GROUP_NAME",
    "PHASE3_MANIFEST_ASSET_KEY",
    "RETROSPECTIVE_HOOK_ASSET_KEY",
    "build_daily_cycle_jobs",
    "daily_cycle_job",
    "daily_cycle_phase0_job",
    "dbt_phase0_assets",
    "phase0_readiness_ping",
]
