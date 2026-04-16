from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.integration.conftest import (
    asset_check_evaluations,
    asset_materialization_keys,
)
from tests.integration.test_phase3_publish_wiring import (
    _asset_def_for_key,
    _asset_dependency_keys,
    _fake_phase0_surface_provider,
    _fake_phase1_provider,
    _fake_phase2_provider,
    _fake_phase3_provider,
)


def test_audit_eval_hook_materializes_after_publish_manifest(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.audit import (
        AUDIT_EVAL_GROUP_NAME,
        RETROSPECTIVE_HOOK_ASSET_KEY,
    )
    from orchestrator.jobs.cycle import daily_cycle_job
    from orchestrator.jobs.phase3 import PHASE3_MANIFEST_ASSET_KEY

    phase3_calls: list[str] = []
    audit_calls: list[str] = []
    defs = build_definitions(
        module_factories=[
            _fake_phase0_surface_provider(dagster),
            _fake_phase1_provider(dagster),
            _fake_phase2_provider(dagster),
            _fake_phase3_provider(dagster, phase3_calls),
            _fake_audit_eval_provider(dagster, audit_calls),
        ],
        policy_path=stub_policy_path,
    )

    dagster.Definitions.validate_loadable(defs)

    manifest_key = dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY])
    hook_key = dagster.AssetKey([RETROSPECTIVE_HOOK_ASSET_KEY])
    selected_keys = daily_cycle_job.selection.resolve(defs.assets or ())
    hook_def = _asset_def_for_key(defs.assets or (), hook_key)

    assert _asset_keys_for_group(defs, AUDIT_EVAL_GROUP_NAME) == {hook_key}
    assert hook_key in selected_keys
    assert manifest_key in _asset_dependency_keys(hook_def, hook_key)
    assert "fake_audit_eval_check" in _check_names(defs)
    assert "fake_audit_eval_resource" in defs.resources

    result = defs.get_job_def("daily_cycle_job").execute_in_process(
        instance=dagster_instance,
        tags={"cycle_id": "cycle-20260416"},
    )
    materialized_keys = asset_materialization_keys(result)
    materialized_key_sequence = _asset_materialization_key_sequence(result)
    evaluations = asset_check_evaluations(result)

    assert result.success is True
    assert manifest_key in materialized_keys
    assert hook_key in materialized_keys
    assert materialized_key_sequence.index(manifest_key) < (
        materialized_key_sequence.index(hook_key)
    )
    assert audit_calls == [RETROSPECTIVE_HOOK_ASSET_KEY]
    assert any(
        _check_name(evaluation) == "fake_audit_eval_check"
        and getattr(evaluation, "passed", None) is True
        for evaluation in evaluations
    )


def test_audit_eval_hook_is_skipped_when_publish_manifest_fails(
    dagster_module: object,
    dagster_instance: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions
    from orchestrator.jobs.audit import RETROSPECTIVE_HOOK_ASSET_KEY
    from orchestrator.jobs.phase3 import PHASE3_MANIFEST_ASSET_KEY

    phase3_calls: list[str] = []
    audit_calls: list[str] = []
    defs = build_definitions(
        module_factories=[
            _fake_phase0_surface_provider(dagster),
            _fake_phase1_provider(dagster),
            _fake_phase2_provider(dagster),
            _fake_phase3_manifest_failure_provider(dagster, phase3_calls),
            _fake_audit_eval_provider(dagster, audit_calls),
        ],
        policy_path=stub_policy_path,
    )

    dagster.Definitions.validate_loadable(defs)

    result = defs.get_job_def("daily_cycle_job").execute_in_process(
        instance=dagster_instance,
        raise_on_error=False,
        tags={"cycle_id": "cycle-20260416"},
    )
    materialized_keys = asset_materialization_keys(result)

    assert result.success is False
    assert dagster.AssetKey([PHASE3_MANIFEST_ASSET_KEY]) not in materialized_keys
    assert dagster.AssetKey([RETROSPECTIVE_HOOK_ASSET_KEY]) not in materialized_keys
    assert audit_calls == []


def test_audit_eval_hook_without_manifest_dependency_is_rejected(
    dagster_module: object,
    stub_policy_path: str,
    tmp_dbt_project: Path,
) -> None:
    dagster = dagster_module

    from orchestrator.definitions import build_definitions

    with pytest.raises(
        ValueError,
        match="audit_eval.*retrospective_hook.*cycle_publish_manifest",
    ):
        build_definitions(
            module_factories=[
                _fake_phase0_surface_provider(dagster),
                _fake_phase1_provider(dagster),
                _fake_phase2_provider(dagster),
                _fake_phase3_provider(dagster, phase3_calls=[]),
                _fake_audit_eval_provider(
                    dagster,
                    audit_calls=[],
                    depends_on_manifest=False,
                ),
            ],
            policy_path=stub_policy_path,
        )


def _fake_audit_eval_provider(
    dagster: Any,
    audit_calls: list[str],
    *,
    depends_on_manifest: bool = True,
) -> object:
    from orchestrator.jobs.audit import (
        AUDIT_EVAL_GROUP_NAME,
        RETROSPECTIVE_HOOK_ASSET_KEY,
    )

    if depends_on_manifest:

        @dagster.asset(
            name=RETROSPECTIVE_HOOK_ASSET_KEY,
            group_name=AUDIT_EVAL_GROUP_NAME,
        )
        def retrospective_hook(cycle_publish_manifest: str) -> str:
            assert cycle_publish_manifest
            audit_calls.append(RETROSPECTIVE_HOOK_ASSET_KEY)
            return "retrospective-ok"

    else:

        @dagster.asset(
            name=RETROSPECTIVE_HOOK_ASSET_KEY,
            group_name=AUDIT_EVAL_GROUP_NAME,
        )
        def retrospective_hook() -> str:
            audit_calls.append(RETROSPECTIVE_HOOK_ASSET_KEY)
            return "retrospective-ok"

    @dagster.asset_check(asset=retrospective_hook, name="fake_audit_eval_check")
    def fake_audit_eval_check() -> object:
        return dagster.AssetCheckResult(passed=True)

    class FakeAuditEvalResource(dagster.ConfigurableResource):
        enabled: bool = True

    class FakeAuditEvalProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (retrospective_hook,)

        def get_checks(self) -> tuple[object, ...]:
            return (fake_audit_eval_check,)

        def get_resources(self) -> dict[str, object]:
            return {"fake_audit_eval_resource": FakeAuditEvalResource()}

    return FakeAuditEvalProvider()


def _fake_phase3_manifest_failure_provider(
    dagster: Any,
    phase3_calls: list[str],
) -> object:
    from orchestrator.jobs.phase3 import (
        PHASE3_FORMAL_COMMIT_ASSET_KEY,
        PHASE3_GROUP_NAME,
        PHASE3_MANIFEST_ASSET_KEY,
    )

    @dagster.asset(
        name=PHASE3_FORMAL_COMMIT_ASSET_KEY,
        group_name=PHASE3_GROUP_NAME,
    )
    def formal_objects_commit(l7: str) -> str:
        assert l7
        phase3_calls.append(PHASE3_FORMAL_COMMIT_ASSET_KEY)
        return "formal-commit-ok"

    @dagster.asset(
        name=PHASE3_MANIFEST_ASSET_KEY,
        group_name=PHASE3_GROUP_NAME,
    )
    def cycle_publish_manifest(formal_objects_commit: str) -> str:
        assert formal_objects_commit
        phase3_calls.append(PHASE3_MANIFEST_ASSET_KEY)
        raise RuntimeError("fake manifest write failed")

    class FakePhase3ManifestFailureProvider:
        def get_assets(self) -> tuple[object, ...]:
            return (formal_objects_commit, cycle_publish_manifest)

        def get_checks(self) -> tuple[object, ...]:
            return ()

        def get_resources(self) -> dict[str, object]:
            return {}

    return FakePhase3ManifestFailureProvider()


def _asset_keys_for_group(defs: Any, group_name: str) -> set[object]:
    return {
        asset_key
        for asset_def in defs.assets or ()
        for asset_key in getattr(asset_def, "keys", ())
        if getattr(asset_def, "group_names_by_key", {}).get(asset_key) == group_name
    }


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


def _check_name(evaluation: object) -> str | None:
    check_name = getattr(evaluation, "check_name", None)
    if isinstance(check_name, str):
        return check_name
    check_key = getattr(evaluation, "check_key", None)
    name = getattr(check_key, "name", None)
    return name if isinstance(name, str) else None


def _asset_materialization_key_sequence(result: object) -> list[object]:
    keys: list[object] = []
    for event in tuple(getattr(result, "all_events", ())):
        if not (
            getattr(event, "is_step_materialization", False)
            or getattr(event, "event_type_value", None) == "ASSET_MATERIALIZATION"
        ):
            continue

        asset_key = getattr(event, "asset_key", None)
        if asset_key is None:
            event_specific_data = getattr(event, "event_specific_data", None)
            materialization = getattr(event_specific_data, "materialization", None)
            asset_key = getattr(materialization, "asset_key", None)
        if asset_key is not None:
            keys.append(asset_key)
    return keys
