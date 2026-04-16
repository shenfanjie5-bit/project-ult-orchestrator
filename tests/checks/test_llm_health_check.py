from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from orchestrator.policy import load_gate_policy


_REPO_ROOT = Path(__file__).resolve().parents[2]
_LITE_POLICY_PATH = _REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"
_DIRECT_PROVIDER_IO = re.compile(r"litellm|requests[.]|httpx|urlopen")


@dataclass(frozen=True, slots=True)
class _FakeHealthResult:
    healthy: bool
    summary: str
    provider: str | None


class _FakeHealthProbe:
    def __init__(self, result: _FakeHealthResult) -> None:
        self._result = result

    def check_health(self) -> _FakeHealthResult:
        return self._result


class _RaisingHealthProbe:
    provider = "fake-llm"

    def check_health(self) -> object:
        raise RuntimeError("probe timeout")


@pytest.fixture
def gate_policy_resource() -> object:
    return SimpleNamespace(policy=load_gate_policy(_LITE_POLICY_PATH))


def test_llm_health_check_passes_when_probe_is_healthy(
    caplog: pytest.LogCaptureFixture,
    gate_policy_resource: object,
) -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.checks.asset_checks import llm_health_check

    with caplog.at_level(logging.WARNING):
        result = llm_health_check(
            gate_policy=gate_policy_resource,
            llm_health_probe=_FakeHealthProbe(
                _FakeHealthResult(
                    healthy=True,
                    summary="provider ready",
                    provider="fake-llm",
                ),
            ),
        )

    assert result.passed is True
    assert result.metadata == {
        "provider": "fake-llm",
        "summary": "provider ready",
    }
    assert caplog.records == []


def test_llm_health_check_fails_phase0_infra_with_fail_run_action(
    gate_policy_resource: object,
) -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.checks.asset_checks import llm_health_check

    result = llm_health_check(
        gate_policy=gate_policy_resource,
        llm_health_probe=_FakeHealthProbe(
            _FakeHealthResult(
                healthy=False,
                summary="probe failed",
                provider="fake-llm",
            ),
        ),
    )

    assert result.passed is False
    assert result.metadata["failure_class"] == "infra"
    assert result.metadata["action"] == "fail_run"
    assert result.metadata["provider"] == "fake-llm"
    assert result.metadata["summary"] == "probe failed"


def test_llm_health_check_dispatches_fail_run_alert(
    caplog: pytest.LogCaptureFixture,
    gate_policy_resource: object,
) -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.checks.asset_checks import llm_health_check

    with caplog.at_level(logging.WARNING):
        llm_health_check(
            gate_policy=gate_policy_resource,
            llm_health_probe=_FakeHealthProbe(
                _FakeHealthResult(
                    healthy=False,
                    summary="probe failed",
                    provider="fake-llm",
                ),
            ),
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload["phase"] == "phase0"
    assert payload["failed_node"] == "llm_health_check"
    assert payload["action"] == "fail_run"
    assert payload["failure_class"] == "infra"
    assert payload["summary"] == "LLM health check failed; stop before Phase 1."


def test_llm_health_check_probe_exception_is_policy_classified(
    caplog: pytest.LogCaptureFixture,
    gate_policy_resource: object,
) -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.checks.asset_checks import llm_health_check

    with caplog.at_level(logging.WARNING):
        result = llm_health_check(
            gate_policy=gate_policy_resource,
            llm_health_probe=_RaisingHealthProbe(),
        )

    payload = json.loads(caplog.records[-1].message)

    assert result.passed is False
    assert result.metadata["provider"] == "fake-llm"
    assert result.metadata["summary"] == "llm health probe failed: probe timeout"
    assert result.metadata["failure_class"] == "infra"
    assert result.metadata["action"] == "fail_run"
    assert payload["failure_class"] == "infra"
    assert payload["action"] == "fail_run"


def test_llm_health_check_is_dagster_asset_check_definition() -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.checks import llm_health_check

    definitions = dagster.Definitions(asset_checks=[llm_health_check])

    assert "llm_health_check" in _check_names(definitions)


def test_orchestrator_checks_do_not_import_provider_io_clients() -> None:
    scanned_paths = [
        *(_REPO_ROOT / "src" / "orchestrator" / "checks").rglob("*.py"),
        *(_REPO_ROOT / "src" / "orchestrator" / "resources").rglob("*.py"),
    ]
    matches: list[str] = []

    for path in scanned_paths:
        text = path.read_text(encoding="utf-8")
        if _DIRECT_PROVIDER_IO.search(text):
            matches.append(str(path.relative_to(_REPO_ROOT)))

    assert matches == []


def _check_names(defs: Any) -> set[str]:
    names: set[str] = set()
    for check_def in defs.asset_checks or ():
        names.update(
            check_key.name
            for check_key in getattr(check_def, "check_keys", ())
        )
        names.update(spec.name for spec in getattr(check_def, "specs", ()))
        if name := getattr(check_def, "name", None):
            names.add(name)
    return names
