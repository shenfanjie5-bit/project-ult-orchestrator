import importlib
import sys
from inspect import signature
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

from orchestrator.checks import DataReadinessSignal

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DBT_MANIFEST_PATH = _REPO_ROOT / "dbt_stub" / "target" / "manifest.json"


@pytest.fixture
def definitions_exports() -> dict[str, Any]:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")
    if not _DBT_MANIFEST_PATH.exists():
        pytest.skip("dbt manifest is not compiled; run make dbt-compile")

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.cycle import build_daily_cycle_jobs, daily_cycle_job
    from orchestrator.jobs.phase0 import dbt_phase0_assets, phase0_readiness_ping
    from orchestrator.resources import ResourceBundle

    return {
        "AssetKey": dagster.AssetKey,
        "Definitions": dagster.Definitions,
        "dagster": dagster,
        "build_daily_cycle_jobs": build_daily_cycle_jobs,
        "build_definitions": build_definitions,
        "daily_cycle_job": daily_cycle_job,
        "dbt_phase0_assets": dbt_phase0_assets,
        "phase0_readiness_ping": phase0_readiness_ping,
        "ResourceBundle": ResourceBundle,
    }


def test_build_definitions_collects_p1a_surface(
    definitions_exports: dict[str, Any],
) -> None:
    AssetKey = definitions_exports["AssetKey"]
    ResourceBundle = definitions_exports["ResourceBundle"]
    build_definitions = definitions_exports["build_definitions"]
    dagster = definitions_exports["dagster"]
    provider = _fake_provider(dagster)

    defs = build_definitions(
        module_factories=[provider],
        policy_path="config/policy/gate_policy.lite.yaml",
    )

    assert len(defs.jobs) == 1
    assert len(defs.schedules) == 1
    assert len(defs.sensors) == 2
    assert {sensor.name for sensor in defs.sensors} == {
        "data_readiness_sensor",
        "manual_rerun_sensor",
    }
    assert AssetKey(["fake_phase0_asset"]) in _asset_keys(defs)
    assert AssetKey(["candidate_freeze"]) in _asset_keys(defs)
    assert "phase0_ping_check" in _check_names(defs)
    assert "llm_health_check" in _check_names(defs)
    assert "fake_phase0_check" in _check_names(defs)
    assert "gate_policy" in defs.resources
    assert "resource_bundle" in defs.resources
    assert "data_readiness" in defs.resources
    assert "fake_data_platform_resource" in defs.resources
    assert "llm_health_probe" in defs.resources
    assert "orchestration_context_stub" not in defs.resources

    bundle = defs.resources["resource_bundle"]
    assert isinstance(bundle, ResourceBundle)
    assert bundle.resource_keys == (
        "fake_data_platform_resource",
        "llm_health_probe",
    )
    assert bundle.source_modules == (__name__,)
    assert bundle.config_ref == "config/policy/gate_policy.lite.yaml"
    assert bundle.read_only is True


def test_build_definitions_dagster_only_registers_full_cycle_entrypoints(
    definitions_exports: dict[str, Any],
) -> None:
    build_definitions = definitions_exports["build_definitions"]

    defs = build_definitions(policy_path="config/policy/gate_policy.lite.yaml")

    assert _job_names(defs) == {"daily_cycle_job"}
    assert _target_name(_schedule_by_name(defs, "daily_cycle_schedule")) == (
        "daily_cycle_job"
    )
    assert _target_name(_sensor_by_name(defs, "data_readiness_sensor")) == (
        "daily_cycle_job"
    )
    assert "temporal_handoff_sensor" not in {
        sensor.name for sensor in defs.sensors or ()
    }


def test_build_definitions_temporal_backend_registers_phase0_entrypoint(
    definitions_exports: dict[str, Any],
    tmp_path: Path,
) -> None:
    build_definitions = definitions_exports["build_definitions"]
    dagster = definitions_exports["dagster"]
    policy_path = _policy_with_backend(tmp_path, "dagster_plus_temporal")

    defs = build_definitions(
        module_factories=[_fake_provider(dagster)],
        policy_path=policy_path,
    )

    assert _job_names(defs) == {
        "daily_cycle_phase0_job",
        "daily_cycle_job",
    }
    assert _target_name(_schedule_by_name(defs, "daily_cycle_schedule")) == (
        "daily_cycle_phase0_job"
    )
    assert _target_name(_sensor_by_name(defs, "data_readiness_sensor")) == (
        "daily_cycle_phase0_job"
    )
    assert "temporal_handoff_sensor" in {
        sensor.name for sensor in defs.sensors or ()
    }
    assert "temporal_handoff_client" not in defs.resources


def test_build_definitions_backs_data_readiness_sensor_resources(
    definitions_exports: dict[str, Any],
) -> None:
    build_definitions = definitions_exports["build_definitions"]

    defs = build_definitions(policy_path="config/policy/gate_policy.lite.yaml")
    sensor = _sensor_by_name(defs, "data_readiness_sensor")

    assert sensor.required_resource_keys == {"data_readiness", "gate_policy"}
    assert sensor.required_resource_keys <= set(defs.resources)


def test_data_readiness_sensor_evaluates_with_definition_resources(
    definitions_exports: dict[str, Any],
) -> None:
    dagster = definitions_exports["dagster"]
    build_definitions = definitions_exports["build_definitions"]
    provider = _fake_readiness_provider(
        dagster,
        resource_key="data_readiness_provider",
    )

    defs = build_definitions(
        module_factories=[provider],
        policy_path="config/policy/gate_policy.lite.yaml",
    )
    sensor = _sensor_by_name(defs, "data_readiness_sensor")

    assert "data_readiness" in defs.resources
    assert "data_readiness_provider" in defs.resources

    result = _evaluate_sensor_tick(dagster, sensor, defs)

    run_requests = list(result.run_requests or ())
    assert len(run_requests) == 1
    assert run_requests[0].run_key == "cycle-20260416"
    assert run_requests[0].tags["phase"] == "phase0"


def test_data_readiness_sensor_defaults_fail_closed_without_provider(
    definitions_exports: dict[str, Any],
) -> None:
    dagster = definitions_exports["dagster"]
    build_definitions = definitions_exports["build_definitions"]

    defs = build_definitions(policy_path="config/policy/gate_policy.lite.yaml")
    sensor = _sensor_by_name(defs, "data_readiness_sensor")

    result = _evaluate_sensor_tick(dagster, sensor, defs)

    assert list(result.run_requests or ()) == []
    assert "data_readiness resource is not configured" in result.skip_message


def test_build_definitions_signature_matches_contract(
    definitions_exports: dict[str, Any],
) -> None:
    build_definitions = definitions_exports["build_definitions"]

    assert list(signature(build_definitions).parameters) == [
        "module_factories",
        "policy_path",
    ]


def test_module_level_defs_requires_milestone_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")
    if not _DBT_MANIFEST_PATH.exists():
        pytest.skip("dbt manifest is not compiled; run make dbt-compile")

    monkeypatch.setenv("ORCHESTRATOR_DEFINITIONS_PROFILE", "milestone-1")
    monkeypatch.delenv("ORCHESTRATOR_MODULE_FACTORIES", raising=False)
    _clear_definition_imports()

    try:
        with pytest.raises(
            RuntimeError,
            match=(
                "milestone Definitions assembly requires upstream "
                "data-platform and reasoner-runtime module factories"
            ),
        ):
            importlib.import_module("orchestrator.definitions")
    finally:
        _clear_definition_imports()


def test_module_level_defs_loads_configured_milestone_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dagster = pytest.importorskip("dagster", reason="dagster is not installed")
    pytest.importorskip("dagster_dbt", reason="dagster-dbt is not installed")
    if not _DBT_MANIFEST_PATH.exists():
        pytest.skip("dbt manifest is not compiled; run make dbt-compile")

    module_name = "fake_milestone_factories"
    fake_module = ModuleType(module_name)

    def build_data_platform_provider() -> object:
        return _fake_provider(dagster)

    def build_readiness_provider() -> object:
        return _fake_readiness_provider(
            dagster,
            resource_key="data_readiness_provider",
        )

    fake_module.build_data_platform_provider = build_data_platform_provider
    fake_module.build_readiness_provider = build_readiness_provider
    monkeypatch.setitem(sys.modules, module_name, fake_module)
    monkeypatch.setenv("ORCHESTRATOR_DEFINITIONS_PROFILE", "milestone-1")
    monkeypatch.setenv(
        "ORCHESTRATOR_MODULE_FACTORIES",
        (
            f"{module_name}:build_data_platform_provider,"
            f"{module_name}:build_readiness_provider"
        ),
    )
    _clear_definition_imports()

    try:
        definitions_module = importlib.import_module("orchestrator.definitions")
        defs = definitions_module.defs

        assert dagster.AssetKey(["candidate_freeze"]) in _asset_keys(defs)
        assert "data_readiness" in defs.resources
        assert "data_readiness_provider" in defs.resources
        assert "llm_health_probe" in defs.resources
    finally:
        _clear_definition_imports()


def test_require_milestone_surface_env_false_overrides_profile(
    definitions_exports: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import orchestrator.definitions as definitions_module

    monkeypatch.setenv("ORCHESTRATOR_DEFINITIONS_PROFILE", "milestone-1")
    monkeypatch.setenv("ORCHESTRATOR_REQUIRE_MILESTONE_SURFACE", "false")

    assert definitions_module._requires_milestone_surface() is False

    monkeypatch.setenv("ORCHESTRATOR_REQUIRE_MILESTONE_SURFACE", "true")

    assert definitions_module._requires_milestone_surface() is True


def test_build_definitions_is_loadable(
    definitions_exports: dict[str, Any],
) -> None:
    Definitions = definitions_exports["Definitions"]
    build_definitions = definitions_exports["build_definitions"]
    dagster = definitions_exports["dagster"]

    Definitions.validate_loadable(
        build_definitions(module_factories=[_fake_provider(dagster)]),
    )


def test_build_definitions_keeps_llm_check_fail_closed_without_probe_resource(
    definitions_exports: dict[str, Any],
) -> None:
    build_definitions = definitions_exports["build_definitions"]

    defs = build_definitions(policy_path="config/policy/gate_policy.lite.yaml")

    assert "llm_health_probe" in defs.resources
    assert "llm_health_check" in _check_names(defs)

    probe_resource = defs.resources["llm_health_probe"]
    create_resource = getattr(probe_resource, "create_resource")
    probe = create_resource(None)
    health = probe.check_health()
    assert health.healthy is False
    assert health.provider == "missing"
    assert "not configured" in health.summary


def test_daily_cycle_job_selects_phase0_readiness_ping(
    definitions_exports: dict[str, Any],
) -> None:
    AssetKey = definitions_exports["AssetKey"]
    daily_cycle_job = definitions_exports["daily_cycle_job"]
    dbt_phase0_assets = definitions_exports["dbt_phase0_assets"]
    phase0_readiness_ping = definitions_exports["phase0_readiness_ping"]

    selected_keys = daily_cycle_job.selection.resolve(
        [phase0_readiness_ping, dbt_phase0_assets],
    )

    assert AssetKey(["phase0_readiness_ping"]) in selected_keys


def test_build_daily_cycle_jobs_signature_and_p1a_job(
    definitions_exports: dict[str, Any],
) -> None:
    build_daily_cycle_jobs = definitions_exports["build_daily_cycle_jobs"]
    daily_cycle_job = definitions_exports["daily_cycle_job"]

    assert list(signature(build_daily_cycle_jobs).parameters) == ["phase_config"]
    assert build_daily_cycle_jobs({"enabled_phases": ["phase0"]}) == (
        daily_cycle_job,
    )


def test_provider_cannot_override_reserved_resource_keys(
    definitions_exports: dict[str, Any],
) -> None:
    build_definitions = definitions_exports["build_definitions"]

    class DbtOverrideProvider:
        def get_assets(self) -> tuple[object, ...]:
            return ()

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {"dbt": object()}

    with pytest.raises(ValueError, match="duplicate resource key: dbt"):
        build_definitions(module_factories=[DbtOverrideProvider()])


def test_duplicate_provider_resource_key_raises_value_error(
    definitions_exports: dict[str, Any],
) -> None:
    build_definitions = definitions_exports["build_definitions"]

    class DuplicateResourceProvider:
        def get_assets(self) -> tuple[object, ...]:
            return ()

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {"duplicate_data_platform_resource": object()}

    with pytest.raises(
        ValueError,
        match="duplicate resource key: duplicate_data_platform_resource",
    ):
        build_definitions(
            module_factories=[
                DuplicateResourceProvider(),
                DuplicateResourceProvider(),
            ],
        )


def test_phase0_provider_missing_candidate_freeze_is_rejected(
    definitions_exports: dict[str, Any],
) -> None:
    build_definitions = definitions_exports["build_definitions"]
    dagster = definitions_exports["dagster"]

    @dagster.asset(name="fake_phase0_asset", group_name="phase0")
    def fake_phase0_asset() -> str:
        return "ok"

    class MissingCandidateProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (fake_phase0_asset,)

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {}

    with pytest.raises(
        ValueError,
        match="phase0 provider assets must include candidate_freeze",
    ):
        build_definitions(module_factories=[MissingCandidateProvider()])


def _fake_provider(dagster: Any) -> object:
    class FakeDataPlatformResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> dict[str, str]:
            return {"status": "ok"}

    class FakeLLMHealthResult:
        healthy = True
        summary = "provider ready"
        provider = "fake-llm"

    class FakeLLMHealthProbe:
        def check_health(self) -> FakeLLMHealthResult:
            return FakeLLMHealthResult()

    class FakeLLMHealthProbeResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> FakeLLMHealthProbe:
            return FakeLLMHealthProbe()

    @dagster.asset(name="fake_phase0_asset", group_name="phase0")
    def fake_phase0_asset() -> str:
        return "ok"

    @dagster.asset(name="candidate_freeze", group_name="phase0")
    def candidate_freeze() -> str:
        return "ok"

    @dagster.asset_check(asset=fake_phase0_asset, name="fake_phase0_check")
    def fake_phase0_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    class FakeDataPlatformProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (fake_phase0_asset, candidate_freeze)

        def get_checks(self) -> tuple[object, ...]:
            return (fake_phase0_check,)

        def get_resources(self) -> dict[str, object]:
            return {
                "fake_data_platform_resource": cast(
                    object,
                    FakeDataPlatformResource(),
                ),
                "llm_health_probe": cast(
                    object,
                    FakeLLMHealthProbeResource(),
                ),
            }

    return FakeDataPlatformProvider()


def _fake_readiness_provider(
    dagster: Any,
    resource_key: str = "data_readiness",
) -> object:
    class FakeReadinessProvider:
        def get_data_readiness_signal(self) -> DataReadinessSignal:
            return DataReadinessSignal(
                ready=True,
                cycle_id="cycle-20260416",
            )

    class FakeReadinessResource(dagster.ConfigurableResource):
        def create_resource(self, context: object) -> FakeReadinessProvider:
            return FakeReadinessProvider()

    class FakeReadinessProviderFactory:
        def get_assets(self) -> tuple[object, ...]:
            return ()

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {
                resource_key: cast(object, FakeReadinessResource()),
            }

    return FakeReadinessProviderFactory()


def _asset_keys(defs: Any) -> set[object]:
    return {
        asset_key
        for asset_def in defs.assets or ()
        for asset_key in getattr(asset_def, "keys", ())
    }


def _sensor_by_name(defs: Any, name: str) -> Any:
    for sensor in defs.sensors or ():
        if sensor.name == name:
            return sensor
    pytest.fail(f"missing sensor {name}")


def _schedule_by_name(defs: Any, name: str) -> Any:
    for schedule in defs.schedules or ():
        if schedule.name == name:
            return schedule
    pytest.fail(f"missing schedule {name}")


def _evaluate_sensor_tick(dagster: Any, sensor: Any, defs: Any) -> Any:
    context = dagster.build_sensor_context(definitions=defs)
    if callable(getattr(context, "__enter__", None)):
        with context as active_context:
            return sensor.evaluate_tick(active_context)
    return sensor.evaluate_tick(context)


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


def _job_names(defs: Any) -> set[str]:
    return {job.name for job in defs.jobs or ()}


def _target_name(definition: Any) -> str | None:
    for attribute_name in ("job_name", "target_name"):
        value = getattr(definition, attribute_name, None)
        if isinstance(value, str):
            return value

    for attribute_name in ("job", "job_def", "_job", "_job_def"):
        target = getattr(definition, attribute_name, None)
        name = getattr(target, "name", None)
        if isinstance(name, str):
            return name

    targets = getattr(definition, "targets", None)
    if targets:
        first_target = next(iter(targets))
        for attribute_name in ("job_name", "target_name"):
            value = getattr(first_target, attribute_name, None)
            if isinstance(value, str):
                return value
    return None


def _policy_with_backend(tmp_path: Path, backend: str) -> Path:
    policy_path = tmp_path / f"gate_policy.{backend}.yaml"
    source_path = (
        _REPO_ROOT / "config" / "policy" / "gate_policy.lite.yaml"
    )
    policy_text = source_path.read_text(encoding="utf-8")
    policy_path.write_text(
        policy_text.replace(
            "execution_backend: dagster_only",
            f"execution_backend: {backend}",
        ),
        encoding="utf-8",
    )
    return policy_path


def _clear_definition_imports() -> None:
    sys.modules.pop("orchestrator.definitions", None)
