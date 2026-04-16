"""YAML loader for gate policy profiles."""

import logging
from pathlib import Path

import yaml

from orchestrator.policy.contracts_adapter import CONTRACTS_VERSION
from orchestrator.policy.schema import GatePolicyProfile


logger = logging.getLogger(__name__)


def load_gate_policy(policy_path: Path | str) -> GatePolicyProfile:
    """Load and validate a gate policy profile from YAML."""

    policy_file = Path(policy_path)
    raw_policy = yaml.safe_load(policy_file.read_text(encoding="utf-8"))
    profile = GatePolicyProfile.model_validate(raw_policy)

    if profile.contract_version != CONTRACTS_VERSION:
        logger.warning(
            "Gate policy contract_version %s does not match contracts adapter %s",
            profile.contract_version,
            CONTRACTS_VERSION,
        )

    return profile


__all__ = ["load_gate_policy"]
