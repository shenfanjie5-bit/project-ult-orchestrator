"""Canonical contract-tier tests for orchestrator public API.

Per SUBPROJECT_TESTING_STANDARD.md §3.2 + §13.3 + iron rule #4: a
module with public interfaces must have a non-empty canonical contract
tier.

orchestrator's contract surface per §10:
- min-cycle CLI argv contract (--profile / --fixture /
  --run-artifacts-dir / --report)
- The 5 public entrypoint signatures (assembly Protocol contract)
- Top-level CLI dispatcher (orchestrator + min-cycle console scripts)
"""

from __future__ import annotations

import inspect

import pytest


class TestMinCycleCliArgvContract:
    """min-cycle CLI argv must accept the four required flags assembly's
    e2e fixture emits per
    `assembly/src/assembly/tests/e2e/fixtures/minimal_cycle/manifest.yaml`.
    """

    REQUIRED_FLAGS = ("--profile", "--fixture", "--run-artifacts-dir", "--report")

    @pytest.mark.parametrize("flag", REQUIRED_FLAGS)
    def test_min_cycle_parser_lists_flag(self, flag: str) -> None:
        """Probe the parser directly (not --help) to avoid argparse's
        SystemExit and capsys ordering quirks. Each required flag must
        be a registered argparse option.
        """
        from orchestrator.cli.min_cycle import _parse_args
        import argparse

        # Reach into _parse_args's argparse: we can't easily get its
        # parser, so we probe via "missing flag → SystemExit(2)".
        # If the flag is required but absent, argparse exits 2.
        # If we then provide all four required flags, parse succeeds.
        with pytest.raises(SystemExit):
            _parse_args([])  # missing all required → SystemExit
        # Parse with all four flags present → no exception.
        args = _parse_args(
            ["--profile", "p", "--fixture", "/tmp/x.yaml",
             "--run-artifacts-dir", "/tmp/r", "--report", "/tmp/o.json"]
        )
        # The probed flag must map to a known attribute (argparse
        # converts --foo-bar → foo_bar).
        attr_name = flag.lstrip("-").replace("-", "_")
        assert hasattr(args, attr_name), (
            f"min-cycle parser missing attribute for flag {flag!r} "
            f"(expected attr {attr_name!r})"
        )


class TestTopLevelOrchestratorCliDispatch:
    """The top-level `orchestrator` console script must dispatch to
    diag and min-cycle subcommands. Dropping either is breaking.

    We probe by parsing the dispatcher's argparse parser directly —
    cleaner than --help (which raises SystemExit) and doesn't run the
    subcommand bodies.
    """

    def test_orchestrator_dispatcher_lists_diag_and_min_cycle(self) -> None:
        from orchestrator.cli.main import _build_parser

        parser = _build_parser()
        # Find the subparsers action.
        subparsers_action = next(
            a for a in parser._actions
            if a.__class__.__name__ == "_SubParsersAction"
        )
        registered = set(subparsers_action.choices.keys())
        assert "diag" in registered, registered
        assert "min-cycle" in registered, registered

    def test_orchestrator_min_cycle_subcommand_is_callable(self) -> None:
        from orchestrator.cli.min_cycle import main as min_cycle_main

        assert callable(min_cycle_main)

    def test_orchestrator_diag_subcommand_is_callable(self) -> None:
        from orchestrator.cli.diag import main as diag_main

        assert callable(diag_main)


class TestPublicEntrypointsSignatures:
    """The five public entrypoints' signatures must match assembly
    Protocol exactly (assembly compat checks enforce this; we duplicate
    here so per-module CI catches drift before assembly e2e).
    """

    EXPECT = {
        "health_probe": ("check", "timeout_sec", inspect.Parameter.KEYWORD_ONLY),
        "smoke_hook": ("run", "profile_id", inspect.Parameter.KEYWORD_ONLY),
        "init_hook": ("initialize", "resolved_env", inspect.Parameter.KEYWORD_ONLY),
        "cli": ("invoke", "argv", inspect.Parameter.POSITIONAL_OR_KEYWORD),
    }

    @pytest.mark.parametrize(
        "kind,method_name,param_name,param_kind",
        [(k, m, p, pk) for k, (m, p, pk) in EXPECT.items()],
    )
    def test_method_signature(
        self, kind: str, method_name: str, param_name: str, param_kind: int
    ) -> None:
        from orchestrator import public

        instance = getattr(public, kind)
        method = getattr(instance, method_name)
        sig = inspect.signature(method)
        params = [
            p for p in sig.parameters.values()
            if p.kind not in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
        ]
        assert len(params) == 1
        actual = params[0]
        assert actual.name == param_name
        assert actual.kind == param_kind
        assert actual.default is inspect.Parameter.empty

    def test_version_declaration_declare_no_params(self) -> None:
        from orchestrator import public

        sig = inspect.signature(public.version_declaration.declare)
        params = [
            p for p in sig.parameters.values()
            if p.kind not in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
        ]
        assert params == []


class TestArtifactPayloadContract:
    """Post-`upgrade-min-cycle-real-execution` artifact contract — see
    `tests/cli/test_min_cycle.py::TestRealPhaseExecution` for the live
    end-to-end checks; here we lock the *contract* names so a refactor
    that renames fields (without coordinating with assembly e2e) fails
    at the contract tier first.
    """

    REQUIRED_PAYLOAD_FIELDS = frozenset(
        {
            "kind",
            "scenario_id",
            "profile_id",
            "produced_by",
            "real_phase_execution",
            "cycle_publish_manifest_id",
            "phases_executed",
            "published_at",
        }
    )

    def test_runtime_artifact_emit_function_signature(self) -> None:
        """The `_emit_runtime_artifacts` function (private but
        contractually critical) must accept these 5 keyword args. If
        someone renames a kwarg, assembly e2e silently breaks.
        """
        from orchestrator.cli.min_cycle import _emit_runtime_artifacts

        sig = inspect.signature(_emit_runtime_artifacts)
        # All keyword-only after the * marker.
        kw_names = {
            name for name, p in sig.parameters.items()
            if p.kind == inspect.Parameter.KEYWORD_ONLY
        }
        assert kw_names == {
            "run_artifacts_dir",
            "required_artifacts",
            "scenario_id",
            "profile_id",
            "expected_phases",
        }, (
            f"_emit_runtime_artifacts kwargs drift: {kw_names}"
        )
