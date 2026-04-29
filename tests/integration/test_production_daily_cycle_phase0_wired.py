"""Integration tests for the Phase 0 status provider default wiring.

The orchestrator's ``ProductionPhase0Provider`` was originally hard-wired to
``_FailClosedGraphStatusProvider`` whenever no override was supplied. M2.3a-1
adds env-driven default wiring: if ``graph_engine`` is importable and
``NEO4J_*`` / ``DATABASE_URL`` env vars are present, the provider returns a
real :class:`graph_engine.providers.Neo4jGraphStatusProvider` instead of the
fail-closed stub.

These tests pin both directions:

* env present + graph-engine importable → real provider returned
* env absent → fail-closed provider returned (covered by
  ``test_production_daily_cycle_default_graph_runtime_fails_closed``)
* explicit override always wins (test seam)
"""

from __future__ import annotations

import pytest


def test_production_phase0_provider_uses_graph_engine_when_env_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When NEO4J_PASSWORD + DATABASE_URL are set and graph-engine is
    installed, the default Phase 0 wiring resolves to a real
    Neo4jGraphStatusProvider (not the fail-closed stub).

    This test does NOT require live PG/Neo4j connectivity — graph-engine
    constructs Neo4jClient lazily, and the PostgreSQL store factory is
    similarly lazy. We only assert the wired type, not that it can read.
    """

    pytest.importorskip(
        "graph_engine.providers",
        reason="graph-engine is not installed in this venv; default wiring "
        "falls through to fail-closed (covered elsewhere).",
    )

    monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
    monkeypatch.setenv("NEO4J_USER", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "changeme123")
    monkeypatch.setenv("NEO4J_DATABASE", "neo4j")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://postgres:changeme@localhost:5432/proj",
    )

    from graph_engine.providers import Neo4jGraphStatusProvider

    from orchestrator_adapters.production_daily_cycle import (
        GRAPH_STATUS_PROVIDER_RESOURCE_KEY,
        ProductionPhase0Provider,
    )

    provider = ProductionPhase0Provider()
    resources = provider.get_resources()

    graph_status_provider = resources[
        GRAPH_STATUS_PROVIDER_RESOURCE_KEY
    ].resource_fn(None)

    assert isinstance(graph_status_provider, Neo4jGraphStatusProvider)


def test_production_phase0_provider_falls_back_when_env_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No NEO4J_PASSWORD → fall through to _FailClosedGraphStatusProvider.

    Mirrors the existing M1-era fail-closed test but pins the provider
    *type* rather than just its raise-on-call behaviour. Provides a quick
    sentinel for regressions if the wire-through helper is ever changed
    to swallow EnvironmentError silently.
    """

    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    from orchestrator_adapters.production_daily_cycle import (
        GRAPH_STATUS_PROVIDER_RESOURCE_KEY,
        ProductionPhase0Provider,
        _FailClosedGraphStatusProvider,
    )

    provider = ProductionPhase0Provider()
    resources = provider.get_resources()
    graph_status_provider = resources[
        GRAPH_STATUS_PROVIDER_RESOURCE_KEY
    ].resource_fn(None)

    assert isinstance(graph_status_provider, _FailClosedGraphStatusProvider)


def test_production_phase0_provider_honours_explicit_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit graph_status_provider always wins, even with env set."""

    pytest.importorskip("main_core", reason="main-core is required for P2 assets")

    monkeypatch.setenv("NEO4J_PASSWORD", "would-be-real")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://postgres:changeme@localhost:5432/proj",
    )

    from orchestrator_adapters.production_daily_cycle import (
        GRAPH_STATUS_PROVIDER_RESOURCE_KEY,
        ProductionPhase0Provider,
    )

    class _Sentinel:
        def get_graph_status(self, *, candidate_freeze, cycle_id):
            return {"sentinel": True, "cycle_id": cycle_id}

    sentinel = _Sentinel()
    provider = ProductionPhase0Provider(graph_status_provider=sentinel)
    resources = provider.get_resources()
    graph_status_provider = resources[
        GRAPH_STATUS_PROVIDER_RESOURCE_KEY
    ].resource_fn(None)

    assert graph_status_provider is sentinel
