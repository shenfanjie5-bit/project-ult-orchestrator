from dataclasses import dataclass


@dataclass
class AlertPayload:
    cycle_id: str
    phase: str
    status: str
    failed_node: str | None
    action: str
    summary: str
    failure_class: str | None = None
    runbook_url: str | None = None
