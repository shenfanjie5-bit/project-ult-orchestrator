import re
from pathlib import Path

from orchestrator.alerting import AlertPayload, runbook_url_for, with_runbook_url
from orchestrator.policy import load_gate_policy


REPO_ROOT = Path(__file__).resolve().parents[2]
LITE_POLICY_PATH = REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"
RUNBOOK_PATH = REPO_ROOT / "docs" / "RUNBOOK_P5.md"
RUNBOOK_RELATIVE_PATH = "docs/RUNBOOK_P5.md"


def test_runbook_url_for_default_phase_failure_action_anchor() -> None:
    assert (
        runbook_url_for("phase2", "data_quality", "fail_run")
        == "docs/RUNBOOK_P5.md#phase2-data_quality-fail_run"
    )
    assert (
        runbook_url_for("phase3", "infra", "repair_manifest")
        == "docs/RUNBOOK_P5.md#phase3-infra-repair_manifest"
    )


def test_runbook_url_for_matrix_scenarios_points_to_existing_anchor() -> None:
    profile = load_gate_policy(LITE_POLICY_PATH)
    anchors = _runbook_anchors()

    for entry in profile.phase_matrix:
        url = runbook_url_for(
            entry.phase.value,
            entry.failure_class.value,
            entry.action.value,
            scenario_id=entry.scenario_id,
        )
        path, _separator, anchor = url.partition("#")

        assert path == RUNBOOK_RELATIVE_PATH
        assert anchor in anchors


def test_runbook_url_for_scenario_id_can_target_generic_infra_anchor() -> None:
    assert (
        runbook_url_for(
            "phase0",
            "infra",
            "fail_run",
            scenario_id="infra_unavailable_hard_stop",
        )
        == "docs/RUNBOOK_P5.md#phase2-infra-fail_run"
    )


def test_with_runbook_url_fills_missing_url_without_mutating_payload() -> None:
    payload = AlertPayload(
        cycle_id="c1",
        phase="phase2",
        status="failed",
        failed_node="phase2_stock_AAPL",
        action="fail_run",
        summary="pool failed",
        failure_class="data_quality",
    )

    enriched = with_runbook_url(payload)

    assert payload.runbook_url is None
    assert enriched.runbook_url == (
        "docs/RUNBOOK_P5.md#phase2-data_quality-fail_run"
    )


def test_with_runbook_url_uses_payload_scenario_id() -> None:
    payload = AlertPayload(
        cycle_id="c1",
        phase="phase1",
        status="failed",
        failed_node="graph_store",
        action="fail_run",
        summary="infra unavailable",
        failure_class="infra",
        scenario_id="infra_unavailable_hard_stop",
    )

    enriched = with_runbook_url(payload)

    assert enriched.runbook_url == "docs/RUNBOOK_P5.md#phase2-infra-fail_run"


def test_with_runbook_url_preserves_explicit_url() -> None:
    payload = AlertPayload(
        cycle_id="c1",
        phase="phase2",
        status="failed",
        failed_node="phase2_stock_AAPL",
        action="fail_run",
        summary="pool failed",
        failure_class="data_quality",
        runbook_url="docs/custom.md#anchor",
    )

    assert with_runbook_url(payload) is payload


def _runbook_anchors() -> set[str]:
    text = RUNBOOK_PATH.read_text(encoding="utf-8")
    return set(re.findall(r'<a id="([^"]+)"></a>', text))
