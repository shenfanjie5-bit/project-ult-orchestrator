from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
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
class _FakeProviderHealthStatus:
    provider: str
    model: str
    reachable: bool = True
    latency_ms: float | None = 20.0
    quota_status: str = "available"
    error: str | None = None


@dataclass(frozen=True, slots=True)
class _FakeHealthReport:
    provider_statuses: tuple[_FakeProviderHealthStatus, ...]
    all_critical_targets_available: bool
    summary: str


class _FakeHealthProbe:
    def __init__(self, report: _FakeHealthReport) -> None:
        self._report = report

    def check_health(self) -> _FakeHealthReport:
        return self._report


class _RaisingHealthProbe:
    provider = "fake-llm"

    def check_health(self) -> object:
        raise RuntimeError("probe timeout")


class _StatusListHealthProbe:
    def check_health(self) -> tuple[_FakeProviderHealthStatus, ...]:
        return (
            _FakeProviderHealthStatus(
                provider="fake-llm",
                model="fast-model",
            ),
            _FakeProviderHealthStatus(
                provider="backup-llm",
                model="deep-model",
            ),
        )


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
                _FakeHealthReport(
                    provider_statuses=(
                        _FakeProviderHealthStatus(
                            provider="fake-llm",
                            model="fast-model",
                            latency_ms=18.5,
                        ),
                        _FakeProviderHealthStatus(
                            provider="backup-llm",
                            model="deep-model",
                            latency_ms=42.0,
                        ),
                    ),
                    all_critical_targets_available=True,
                    summary="2 provider/model targets ready",
                ),
            ),
        )

    assert result.passed is True
    assert _metadata_value(result.metadata, "all_critical_targets_available") == "true"
    assert _metadata_value(result.metadata, "target_count") == "2"
    assert _metadata_value(result.metadata, "unavailable_target_count") == "0"
    assert _metadata_value(result.metadata, "providers") == "backup-llm, fake-llm"
    assert _metadata_value(result.metadata, "provider_models") == (
        "fake-llm/fast-model, backup-llm/deep-model"
    )
    provider_statuses = json.loads(
        _metadata_value(result.metadata, "provider_statuses")
    )
    assert provider_statuses == [
        {
            "error": None,
            "latency_ms": 18.5,
            "model": "fast-model",
            "provider": "fake-llm",
            "quota_status": "available",
            "reachable": True,
        },
        {
            "error": None,
            "latency_ms": 42.0,
            "model": "deep-model",
            "provider": "backup-llm",
            "quota_status": "available",
            "reachable": True,
        },
    ]
    assert caplog.records == []


def test_llm_health_check_passes_when_only_noncritical_target_is_unavailable(
    gate_policy_resource: object,
) -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.checks.asset_checks import llm_health_check

    result = llm_health_check(
        gate_policy=gate_policy_resource,
        llm_health_probe=_FakeHealthProbe(
            _FakeHealthReport(
                provider_statuses=(
                    _FakeProviderHealthStatus(
                        provider="critical-llm",
                        model="critical-model",
                    ),
                    _FakeProviderHealthStatus(
                        provider="optional-llm",
                        model="optional-model",
                        reachable=False,
                        latency_ms=None,
                        quota_status="unavailable",
                        error="optional target unavailable",
                    ),
                ),
                all_critical_targets_available=True,
                summary="critical targets ready; optional target unavailable",
            ),
        ),
    )

    assert result.passed is True
    assert _metadata_value(result.metadata, "all_critical_targets_available") == "true"
    assert _metadata_value(result.metadata, "unavailable_target_count") == "1"
    assert _metadata_value(result.metadata, "unavailable_provider_models") == (
        "optional-llm/optional-model"
    )


def test_llm_health_check_accepts_provider_status_list_contract(
    gate_policy_resource: object,
) -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.checks.asset_checks import llm_health_check

    result = llm_health_check(
        gate_policy=gate_policy_resource,
        llm_health_probe=_StatusListHealthProbe(),
    )

    assert result.passed is True
    assert _metadata_value(result.metadata, "summary") == (
        "2 provider/model target(s) available"
    )
    assert _metadata_value(result.metadata, "all_critical_targets_available") == "true"
    assert _metadata_value(result.metadata, "provider_models") == (
        "fake-llm/fast-model, backup-llm/deep-model"
    )


def test_llm_health_check_fails_phase0_infra_with_fail_run_action(
    gate_policy_resource: object,
) -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")

    from orchestrator.checks.asset_checks import llm_health_check

    result = llm_health_check(
        gate_policy=gate_policy_resource,
        llm_health_probe=_FakeHealthProbe(
            _FakeHealthReport(
                provider_statuses=(
                    _FakeProviderHealthStatus(
                        provider="fake-llm",
                        model="critical-model",
                        reachable=False,
                        latency_ms=None,
                        quota_status="available",
                        error="connection refused",
                    ),
                    _FakeProviderHealthStatus(
                        provider="backup-llm",
                        model="fallback-model",
                    ),
                ),
                all_critical_targets_available=False,
                summary="critical target fake-llm/critical-model unavailable",
            ),
        ),
    )

    assert result.passed is False
    assert _metadata_value(result.metadata, "failure_class") == "infra"
    assert _metadata_value(result.metadata, "action") == "fail_run"
    assert _metadata_value(result.metadata, "scenario_id") == (
        "phase0_llm_health_check_failed"
    )
    assert _metadata_value(result.metadata, "providers") == "backup-llm, fake-llm"
    assert _metadata_value(result.metadata, "provider_models") == (
        "fake-llm/critical-model, backup-llm/fallback-model"
    )
    assert _metadata_value(result.metadata, "unavailable_provider_models") == (
        "fake-llm/critical-model"
    )
    assert _metadata_value(result.metadata, "summary") == (
        "critical target fake-llm/critical-model unavailable"
    )


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
                _FakeHealthReport(
                    provider_statuses=(
                        _FakeProviderHealthStatus(
                            provider="fake-llm",
                            model="critical-model",
                            reachable=False,
                            latency_ms=None,
                            quota_status="quota_exceeded",
                            error="quota exhausted",
                        ),
                    ),
                    all_critical_targets_available=False,
                    summary="critical target quota exhausted",
                ),
            ),
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload["phase"] == "phase0"
    assert payload["failed_node"] == "llm_health_check"
    assert payload["action"] == "fail_run"
    assert payload["failure_class"] == "infra"
    assert payload["scenario_id"] == "phase0_llm_health_check_failed"
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
    assert _metadata_value(result.metadata, "provider") == "fake-llm"
    assert _metadata_value(result.metadata, "provider_models") == "fake-llm/unknown"
    assert _metadata_value(result.metadata, "summary") == (
        "llm health probe failed: probe timeout"
    )
    assert _metadata_value(result.metadata, "failure_class") == "infra"
    assert _metadata_value(result.metadata, "action") == "fail_run"
    assert _metadata_value(result.metadata, "scenario_id") == (
        "phase0_llm_health_check_failed"
    )
    assert payload["failure_class"] == "infra"
    assert payload["action"] == "fail_run"
    assert payload["scenario_id"] == "phase0_llm_health_check_failed"


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


def _metadata_value(metadata: Mapping[str, object], key: str) -> str:
    value = metadata[key]
    text = getattr(value, "text", None)
    if isinstance(text, str):
        return text
    if isinstance(value, str):
        return value
    return str(value)
