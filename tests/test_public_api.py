import orchestrator


def test_package_exports_contract_facades() -> None:
    assert set(orchestrator.__all__) == {
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
    }


def test_pure_contract_facades_are_importable_from_package_root() -> None:
    from orchestrator import (
        PartialRerunPlan,
        RunHistorySnapshot,
        classify_gate_result,
        compute_partial_rerun_plan,
        load_gate_policy,
        plan_partial_rerun,
    )

    assert PartialRerunPlan.__name__ == "PartialRerunPlan"
    assert RunHistorySnapshot.__name__ == "RunHistorySnapshot"
    assert callable(classify_gate_result)
    assert callable(compute_partial_rerun_plan)
    assert callable(load_gate_policy)
    assert callable(plan_partial_rerun)
