from __future__ import annotations

from pathlib import Path

import pytest

from orchestrator.policy import REQUIRED_GATE_MATRIX_SCENARIOS
from tests.integration.conftest import metadata_value
from tests.integration.failure_injection import (
    PHASE2_DOWNSTREAM_PUBLISH_ASSET_KEY,
    PHASE1_GRAPH_PROMOTION_ASSET_KEY,
    PHASE2_STAGE_KEYS,
    PHASE3_FORMAL_COMMIT_ASSET_KEY,
    PHASE3_MANIFEST_ASSET_KEY,
    REPAIR_MANIFEST_ASSET_KEY,
    FailureInjectionCase,
    asset_selection_strings,
    gate_matrix_failure_cases,
)


@pytest.fixture(
    params=REQUIRED_GATE_MATRIX_SCENARIOS,
    ids=REQUIRED_GATE_MATRIX_SCENARIOS,
)
def case(
    request: pytest.FixtureRequest,
    dagster_module: object,
    stub_policy_path: str,
    tmp_path: Path,
    tmp_dbt_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> FailureInjectionCase:
    cases = gate_matrix_failure_cases(
        dagster_module,
        stub_policy_path=stub_policy_path,
        tmp_path=tmp_path,
        tmp_dbt_project=tmp_dbt_project,
        monkeypatch=monkeypatch,
    )
    cases_by_id = {matrix_case.scenario_id: matrix_case for matrix_case in cases}
    return cases_by_id[str(request.param)]


def test_gate_matrix_failure_cases_cover_required_scenarios(
    dagster_module: object,
    stub_policy_path: str,
    tmp_path: Path,
    tmp_dbt_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cases = gate_matrix_failure_cases(
        dagster_module,
        stub_policy_path=stub_policy_path,
        tmp_path=tmp_path,
        tmp_dbt_project=tmp_dbt_project,
        monkeypatch=monkeypatch,
    )

    assert {case.scenario_id for case in cases} == set(
        REQUIRED_GATE_MATRIX_SCENARIOS,
    )


def test_gate_matrix_failure_case(
    case: FailureInjectionCase,
    dagster_module: object,
    dagster_instance: object,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dagster = dagster_module
    caplog.clear()

    outcome = case.build(dagster_instance=dagster_instance, caplog=caplog)
    expected_failed_node = case.expected_failed_node or outcome.failed_node

    assert outcome.action == case.expected_action
    assert outcome.failure_class == case.expected_failure_class
    assert outcome.success is case.expected_success
    assert outcome.failed_node == expected_failed_node

    _assert_required_alert_payload(outcome, case, expected_failed_node)
    _assert_materialization_expectations(dagster, outcome)
    _assert_scenario_specifics(case.scenario_id, dagster, outcome)


def _assert_required_alert_payload(
    outcome: object,
    case: FailureInjectionCase,
    expected_failed_node: str | None,
) -> None:
    alert = _single_alert_for_failed_node(outcome.alert_payloads, expected_failed_node)

    assert alert["phase"] == case.phase
    assert alert["action"] == case.expected_action
    assert alert["failure_class"] == case.expected_failure_class
    assert alert["failed_node"] == expected_failed_node
    assert isinstance(alert["runbook_url"], str)
    assert alert["runbook_url"]


def _assert_materialization_expectations(
    dagster: object,
    outcome: object,
) -> None:
    required_asset_keys = tuple(
        outcome.extras.get("required_materialized_asset_keys", ()),
    )
    blocked_asset_keys = tuple(outcome.extras.get("blocked_asset_keys", ()))

    for asset_key in required_asset_keys:
        assert dagster.AssetKey([asset_key]) in outcome.materialized_keys
    for asset_key in blocked_asset_keys:
        assert dagster.AssetKey([asset_key]) not in outcome.materialized_keys


def _assert_scenario_specifics(
    scenario_id: str,
    dagster: object,
    outcome: object,
) -> None:
    if scenario_id == "phase0_data_readiness_delayed":
        assert isinstance(outcome.sensor_result, dagster.SkipReason)
        assert not isinstance(outcome.sensor_result, dagster.RunRequest)
        return

    if scenario_id == "phase0_llm_health_check_failed":
        evaluation = outcome.evaluations[0]
        assert getattr(evaluation, "passed", None) is False
        assert metadata_value(evaluation, "action") == "fail_run"
        assert metadata_value(evaluation, "failure_class") == "infra"
        return

    if scenario_id == "phase0_dbt_test_failed":
        _assert_dbt_failure_request(outcome)
        return

    if scenario_id == "phase1_graph_promotion_snapshot_failed":
        assert dagster.AssetKey([PHASE1_GRAPH_PROMOTION_ASSET_KEY]) in (
            outcome.materialized_keys
        )
        return

    if scenario_id == "phase2_single_stock_task_failed":
        evaluation = outcome.evaluations[0]
        assert outcome.success is True
        assert getattr(evaluation, "passed", None) is False
        assert metadata_value(evaluation, "action") == "mark_inconclusive"
        assert metadata_value(evaluation, "stock_id") == "AAPL"
        assert metadata_value(evaluation, "failure_rate") == 0.1
        return

    if scenario_id == "phase2_pool_failure_rate_exceeded":
        evaluation = outcome.evaluations[0]
        assert getattr(evaluation, "passed", None) is False
        assert metadata_value(evaluation, "action") == "fail_run"
        assert metadata_value(evaluation, "failure_rate") == 0.4
        assert dagster.AssetKey([PHASE2_STAGE_KEYS[-1]]) in outcome.materialized_keys
        assert dagster.AssetKey([PHASE2_DOWNSTREAM_PUBLISH_ASSET_KEY]) not in (
            outcome.materialized_keys
        )
        return

    if scenario_id == "phase3_formal_commit_failed":
        evaluation = outcome.evaluations[0]
        assert getattr(evaluation, "passed", None) is False
        assert metadata_value(evaluation, "action") == "fail_run"
        assert dagster.AssetKey([PHASE3_FORMAL_COMMIT_ASSET_KEY]) in (
            outcome.materialized_keys
        )
        assert dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY]) not in (
            outcome.materialized_keys
        )
        return

    if scenario_id == "phase3_manifest_write_failed":
        _assert_manifest_repair_request(dagster, outcome)
        return

    if scenario_id == "infra_unavailable_hard_stop":
        assert dagster.AssetKey([PHASE2_DOWNSTREAM_PUBLISH_ASSET_KEY]) not in (
            outcome.materialized_keys
        )
        return

    pytest.fail(f"unhandled scenario_id={scenario_id}")


def _assert_dbt_failure_request(outcome: object) -> None:
    gate_observation = outcome.extras["gate_observation"]
    request = _single_rerun_request(outcome)
    expected_request = outcome.extras["expected_request"]

    assert metadata_value(gate_observation, "action") == "partial_rerun"
    assert metadata_value(gate_observation, "failure_class") == "task_level"
    assert metadata_value(gate_observation, "failed_node") == outcome.failed_node
    assert request == expected_request
    assert request["rerun_selection"] == [outcome.failed_node]
    assert request["rerun_mode"] == "asset_only"


def _assert_manifest_repair_request(dagster: object, outcome: object) -> None:
    request = _single_rerun_request(outcome)
    expected_request = outcome.extras["expected_request"]

    assert request == expected_request
    assert request["failed_node"] == PHASE3_MANIFEST_ASSET_KEY
    assert request["rerun_selection"] == [REPAIR_MANIFEST_ASSET_KEY]
    assert request["rerun_mode"] == "repair_only"
    assert request["requires_manual_ack"] is True
    assert PHASE3_FORMAL_COMMIT_ASSET_KEY not in request["rerun_selection"]
    assert isinstance(outcome.run_request, dagster.RunRequest)
    assert asset_selection_strings(outcome.run_request.asset_selection) == [
        REPAIR_MANIFEST_ASSET_KEY,
    ]


def _single_alert_for_failed_node(
    payloads: tuple[dict[str, object], ...],
    failed_node: str | None,
) -> dict[str, object]:
    matches = [
        payload
        for payload in payloads
        if payload.get("failed_node") == failed_node
    ]
    assert len(matches) == 1
    return matches[0]


def _single_rerun_request(outcome: object) -> dict[str, object]:
    assert len(outcome.rerun_requests) == 1
    return outcome.rerun_requests[0]
