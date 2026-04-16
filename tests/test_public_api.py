import orchestrator


def test_package_exports_contract_facades() -> None:
    assert set(orchestrator.__all__) == {
        "build_daily_cycle_jobs",
        "build_definitions",
        "build_resource_bundle",
        "classify_gate_result",
        "load_gate_policy",
        "plan_partial_rerun",
    }


def test_pure_contract_facades_are_importable_from_package_root() -> None:
    from orchestrator import (
        classify_gate_result,
        load_gate_policy,
        plan_partial_rerun,
    )

    assert callable(classify_gate_result)
    assert callable(load_gate_policy)
    assert callable(plan_partial_rerun)
