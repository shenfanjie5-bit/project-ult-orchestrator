"""Dagster resources for gate checks."""

from __future__ import annotations

from pydantic import PrivateAttr

from dagster import ConfigurableResource, InitResourceContext

from orchestrator.policy import GatePolicyProfile, load_gate_policy


class GatePolicyResource(ConfigurableResource):
    """Load a versioned gate policy for Dagster check execution."""

    policy_path: str

    _policy: GatePolicyProfile | None = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._policy = load_gate_policy(self.policy_path)

    @property
    def policy(self) -> GatePolicyProfile:
        if self._policy is None:
            self._policy = load_gate_policy(self.policy_path)
        return self._policy


__all__ = ["GatePolicyResource"]
