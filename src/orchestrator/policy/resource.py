"""Dagster resource wrapper for gate policy loading."""

from dagster import ConfigurableResource

from orchestrator.policy.loader import load_gate_policy
from orchestrator.policy.schema import GatePolicyProfile


class GatePolicyResource(ConfigurableResource):
    """Load the configured gate policy profile for a run."""

    policy_path: str

    def create_resource(self, context: object) -> GatePolicyProfile:
        return load_gate_policy(self.policy_path)


__all__ = ["GatePolicyResource"]
