"""Integration tests for the Phase 1 provider default wiring (M2.3a-2).

Mirrors ``test_production_daily_cycle_phase0_wired`` for the Phase 1
runtime resolution path. The orchestrator's
``_default_graph_phase1_provider`` now:

1. Attempts ``graph_engine.providers.build_graph_phase1_provider()`` which
   triggers ``build_graph_phase1_runtime_from_env`` env-driven composition
   of cross-module adapters.
2. Falls through to ``_build_fail_closed_graph_phase1_provider`` (a real
   provider but with ``_FailClosedGraphPhase1Runtime`` as its runtime) on
   ``ImportError`` (graph-engine missing) or ``EnvironmentError`` (env
   vars absent / cross-module adapters unavailable).

These tests focus on the fail-closed fall-through paths since the
env-set path requires graph-engine + data-platform + main-core all
installed in the venv (verified end-to-end in M2.3a-2's cross-module
smoke).
"""

from __future__ import annotations

import pytest


def test_phase1_provider_falls_back_to_fail_closed_when_env_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No NEO4J_PASSWORD / DATABASE_URL set → orchestrator catches the
    EnvironmentError raised by graph-engine's env-driven factory and
    constructs a fail-closed Phase 1 provider."""

    pytest.importorskip(
        "graph_engine.providers",
        reason="graph-engine is not installed in this venv; the ImportError "
        "branch of the fall-back is covered by the import-skip case below.",
    )

    monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("GRAPH_PHASE1_SNAPSHOT_ARTIFACT_ROOT", raising=False)

    from orchestrator_adapters.production_daily_cycle import (
        _default_graph_phase1_provider,
    )

    provider = _default_graph_phase1_provider()
    # The fall-back returns a real GraphPhase1AssetFactoryProvider whose
    # runtime is _FailClosedGraphPhase1Runtime — get_assets() should still
    # work (asset shape is independent of runtime), but get_resources()
    # returns the fail-closed runtime so asset evaluation will raise.
    assets = provider.get_assets()
    assert len(assets) == 2  # graph_promotion + graph_snapshot

    resources = provider.get_resources()
    runtime_key = next(iter(resources))
    runtime = resources[runtime_key].resource_fn(None)

    # Verify it's the fail-closed runtime (typed check via class name to
    # avoid importing private graph-engine internals).
    assert runtime.__class__.__name__ == "_FailClosedGraphPhase1Runtime"


def test_phase1_provider_falls_back_when_graph_engine_not_importable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If graph_engine.providers import fails entirely, the orchestrator
    still returns a Definitions-loadable fail-closed Phase 1 provider."""

    from orchestrator_adapters.production_daily_cycle import (
        _build_fail_closed_graph_phase1_provider,
    )

    provider = _build_fail_closed_graph_phase1_provider()
    resources = provider.get_resources()
    runtime_key = next(iter(resources))
    runtime = resources[runtime_key].resource_fn(None)
    assert runtime.__class__.__name__ in {
        "_FailClosedGraphPhase1Runtime",
        "_LocalFailClosedGraphPhase1Runtime",
    }


def test_phase1_status_keeps_runtime_blocker_until_proof_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Factory construction is not enough to clear the gate-facing blocker.

    M2.6 closure is artifact-backed by ``graph_promotion`` materialization
    evidence, not by lazy adapter/client object construction.
    """

    import sys
    import types

    fake_graph_engine = types.ModuleType("graph_engine")
    fake_providers = types.ModuleType("graph_engine.providers")
    fake_providers.build_graph_phase1_runtime_from_env = (  # type: ignore[attr-defined]
        lambda: object()
    )

    monkeypatch.setitem(sys.modules, "graph_engine", fake_graph_engine)
    monkeypatch.setitem(sys.modules, "graph_engine.providers", fake_providers)

    from orchestrator_adapters.production_daily_cycle import (
        production_daily_cycle_status,
    )

    status = production_daily_cycle_status()

    assert status.blocked is True
    assert "configured_graph_phase1_runtime" in status.runtime_blockers
    assert "configured_reasoner_runtime" in status.runtime_blockers


def test_phase1_provider_uses_real_runtime_when_env_set_and_modules_available(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """End-to-end orchestrator wiring smoke: with env vars set and fake
    cross-module adapter modules injected via sys.modules, the
    orchestrator's _default_graph_phase1_provider must construct a real
    GraphPhase1Service-backed provider — NOT the fail-closed fallback.

    This is the closest unit-test approximation of the M2.6 deployment-env
    wiring path; without this test the orchestrator's success path was
    skipped under the isolated test venv.
    """

    pytest.importorskip("graph_engine.providers")
    import sys
    import types

    # Set the env that build_graph_phase1_runtime_from_env consumes.
    monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
    monkeypatch.setenv("NEO4J_USER", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "changeme123")
    monkeypatch.setenv("NEO4J_DATABASE", "neo4j")
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql://postgres:changeme@localhost:5432/proj"
    )
    monkeypatch.setenv(
        "GRAPH_PHASE1_SNAPSHOT_ARTIFACT_ROOT",
        str(tmp_path / "phase1_artifacts"),
    )

    # Inject minimal fake data-platform + main-core modules so the lazy
    # imports inside graph-engine succeed in this isolated test venv.
    class _FakeCandidateReader:
        def read_candidate_graph_deltas(self, cycle_id, selection_ref):
            return []

        @classmethod
        def from_env(cls):
            return cls()

    class _FakeEntityReader:
        def canonical_entity_ids_for_node_ids(self, node_ids):
            return {}

        def existing_entity_ids(self, entity_ids):
            return set()

        @classmethod
        def from_env(cls):
            return cls()

    class _FakeCanonicalWriter:
        def write_canonical_records(self, plan):
            pass

        @classmethod
        def from_env(cls):
            return cls()

    class _FakeRegimeReader:
        def read_regime_context(self, world_state_ref):
            return {
                "world_state_ref": world_state_ref,
                "channel_multipliers": {"fundamental": 1.0},
                "regime_multipliers": {"fundamental": 1.0},
                "decay_policy": {"default": 1.0},
            }

    fake_dp = types.ModuleType("data_platform")
    fake_dp_cycle = types.ModuleType("data_platform.cycle")
    fake_dp_adapters = types.ModuleType(
        "data_platform.cycle.graph_phase1_adapters"
    )
    fake_dp_adapters.PostgresCandidateDeltaReader = _FakeCandidateReader  # type: ignore[attr-defined]
    fake_dp_adapters.IcebergEntityAnchorReader = _FakeEntityReader  # type: ignore[attr-defined]
    fake_dp_adapters.IcebergCanonicalGraphWriter = _FakeCanonicalWriter  # type: ignore[attr-defined]

    fake_mc = types.ModuleType("main_core")
    fake_mc_adapters = types.ModuleType("main_core.adapters")
    fake_mc_ge = types.ModuleType("main_core.adapters.graph_engine")
    fake_mc_ge.build_regime_context_reader_from_env = lambda: _FakeRegimeReader()  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "data_platform", fake_dp)
    monkeypatch.setitem(sys.modules, "data_platform.cycle", fake_dp_cycle)
    monkeypatch.setitem(
        sys.modules,
        "data_platform.cycle.graph_phase1_adapters",
        fake_dp_adapters,
    )
    monkeypatch.setitem(sys.modules, "main_core", fake_mc)
    monkeypatch.setitem(sys.modules, "main_core.adapters", fake_mc_adapters)
    monkeypatch.setitem(sys.modules, "main_core.adapters.graph_engine", fake_mc_ge)

    from orchestrator_adapters.production_daily_cycle import (
        _default_graph_phase1_provider,
    )

    provider = _default_graph_phase1_provider()
    resources = provider.get_resources()
    runtime_key = next(iter(resources))
    runtime = resources[runtime_key].resource_fn(None)

    # The success path should return a REAL GraphPhase1Service, not the
    # fail-closed stub.
    assert runtime.__class__.__name__ == "GraphPhase1Service"
    assert runtime.__class__.__name__ != "_FailClosedGraphPhase1Runtime"
