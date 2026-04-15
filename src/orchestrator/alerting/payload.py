from dataclasses import dataclass


@dataclass
class AlertPayload:
    cycle_id: str
    phase: str
    status: str
    failed_node: str | None
    action: str
    summary: str
    runbook_url: str | None = None
