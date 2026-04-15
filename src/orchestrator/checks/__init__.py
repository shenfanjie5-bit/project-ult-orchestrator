"""Phase 0 asset check wiring."""

from dagster import AssetCheckResult, asset_check

from orchestrator.jobs.phase0 import phase0_readiness_ping


@asset_check(asset=phase0_readiness_ping, name="phase0_ping_check")
def phase0_ping_check() -> AssetCheckResult:
    return AssetCheckResult(passed=True)


__all__ = ["phase0_ping_check"]
