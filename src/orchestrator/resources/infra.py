"""Infrastructure hard-stop adapters for core Dagster resources."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from inspect import isgenerator
from types import MappingProxyType
from typing import NoReturn

from orchestrator.checks.classifier import classify_gate_result
from orchestrator.checks.decision_handler import dispatch_gate_decision_alert
from orchestrator.checks.models import GateDecision
from orchestrator.policy import (
    FailureClass,
    GateAction,
    GatePolicyProfile,
    PhaseEnum,
    load_gate_policy,
)

INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID = "infra_unavailable_hard_stop"
_INFRA_HARD_STOP_REASON = (
    "Core storage or graph infrastructure is unavailable; hard stop."
)
_FALLBACK_ALERT_CHANNELS = ("logging",)
CORE_INFRASTRUCTURE_RESOURCE_KEYS = frozenset(
    {
        "dbt",
        "gate_policy",
        "data_readiness",
        "llm_health_probe",
        "phase2_pool_failure_rate",
    },
)
INFRASTRUCTURE_RESOURCE_PHASES: Mapping[str, PhaseEnum] = MappingProxyType(
    {
        "dbt": PhaseEnum.PHASE0,
        "gate_policy": PhaseEnum.PHASE0,
        "data_readiness": PhaseEnum.PHASE0,
        "llm_health_probe": PhaseEnum.PHASE0,
        "phase2_pool_failure_rate": PhaseEnum.PHASE2,
    },
)
_GUARDED_METHODS_BY_RESOURCE_KEY: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "data_readiness": frozenset(
            {
                "get_data_readiness_signal",
                "get_readiness_signal",
                "get_data_readiness",
            },
        ),
        "llm_health_probe": frozenset({"check_health"}),
        "phase2_pool_failure_rate": frozenset(
            {"get_phase2_pool_failure_rate_event"},
        ),
    },
)


@dataclass(frozen=True, slots=True, kw_only=True)
class InfrastructureUnavailableEvent:
    """Normalized event for a core infrastructure resource outage."""

    failure_class: FailureClass = FailureClass.INFRA
    resource_key: str
    phase: PhaseEnum
    reason: str
    scenario_id: str = INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID


class InfrastructureUnavailableError(RuntimeError):
    """Raised after an infrastructure failure has been classified and alerted."""

    def __init__(
        self,
        event: InfrastructureUnavailableEvent,
        decision: GateDecision,
        original: BaseException,
    ) -> None:
        super().__init__(_infra_summary(event.resource_key, original))
        self.event = event
        self.decision = decision
        self.original = original


def classify_infrastructure_failure(
    phase: PhaseEnum,
    event: InfrastructureUnavailableEvent,
    policy: GatePolicyProfile,
) -> GateDecision:
    """Classify a core infrastructure outage as a hard run failure."""

    if event.phase is not phase:
        msg = (
            "infrastructure failure event phase does not match classifier phase: "
            f"event={event.phase.value} phase={phase.value}"
        )
        raise ValueError(msg)
    if event.failure_class is not FailureClass.INFRA:
        msg = "infrastructure failure events must use failure_class=infra"
        raise ValueError(msg)
    if event.scenario_id != INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID:
        msg = (
            "infrastructure failure events must use scenario_id="
            f"{INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID!r}"
        )
        raise ValueError(msg)

    decision = classify_gate_result(phase, event, policy)
    if (
        decision.failure_class is not FailureClass.INFRA
        or decision.action is not GateAction.FAIL_RUN
    ):
        msg = (
            "infra_unavailable_hard_stop must map to "
            "failure_class=infra and action=fail_run"
        )
        raise ValueError(msg)

    return decision


def guard_infrastructure_resource(
    resource_key: str,
    resource: object,
    *,
    phase: PhaseEnum,
    policy_path: str,
) -> object:
    """Return a Dagster resource that alerts and fails on infra unavailability."""

    if resource_key not in CORE_INFRASTRUCTURE_RESOURCE_KEYS:
        return resource

    from dagster import ResourceDefinition

    def _resource_fn(context: object) -> Iterable[object]:
        finalizers: tuple[Callable[[], None], ...] = ()
        try:
            value, finalizers = _initialize_resource(resource, context)
        except InfrastructureUnavailableError:
            raise
        except Exception as exc:
            _raise_infrastructure_unavailable(
                resource_key=resource_key,
                phase=phase,
                policy_path=policy_path,
                context=context,
                exc=exc,
            )

        try:
            yield _GuardedInfrastructureValue(
                resource_key=resource_key,
                value=value,
                phase=phase,
                policy_path=policy_path,
                context=context,
            )
        finally:
            _run_finalizers(finalizers)

    return ResourceDefinition(
        resource_fn=_resource_fn,
        description=f"Guarded infrastructure resource for {resource_key}.",
    )


class _GuardedInfrastructureValue:
    __slots__ = ("_context", "_phase", "_policy_path", "_resource_key", "_value")

    def __init__(
        self,
        *,
        resource_key: str,
        value: object,
        phase: PhaseEnum,
        policy_path: str,
        context: object,
    ) -> None:
        self._resource_key = resource_key
        self._value = value
        self._phase = phase
        self._policy_path = policy_path
        self._context = context

    def __getattr__(self, name: str) -> object:
        try:
            attribute = getattr(self._value, name)
        except AttributeError:
            raise
        except InfrastructureUnavailableError:
            raise
        except Exception as exc:
            _raise_infrastructure_unavailable(
                resource_key=self._resource_key,
                phase=self._phase,
                policy_path=self._policy_path,
                context=self._context,
                exc=exc,
            )

        if callable(attribute) and name in _GUARDED_METHODS_BY_RESOURCE_KEY.get(
            self._resource_key,
            frozenset(),
        ):
            return _guard_method_call(
                resource_key=self._resource_key,
                phase=self._phase,
                policy_path=self._policy_path,
                context=self._context,
                method=attribute,
            )

        return attribute


def _guard_method_call(
    *,
    resource_key: str,
    phase: PhaseEnum,
    policy_path: str,
    context: object,
    method: Callable[..., object],
) -> Callable[..., object]:
    def _wrapped(*args: object, **kwargs: object) -> object:
        try:
            return method(*args, **kwargs)
        except InfrastructureUnavailableError:
            raise
        except Exception as exc:
            _raise_infrastructure_unavailable(
                resource_key=resource_key,
                phase=phase,
                policy_path=policy_path,
                context=context,
                exc=exc,
            )

    return _wrapped


def _initialize_resource(
    resource: object,
    context: object,
) -> tuple[object, tuple[Callable[[], None], ...]]:
    finalizers: list[Callable[[], None]] = []
    setup_for_execution = getattr(resource, "setup_for_execution", None)
    if callable(setup_for_execution):
        setup_for_execution(context)

    teardown_after_execution = getattr(resource, "teardown_after_execution", None)
    if callable(teardown_after_execution):
        finalizers.append(lambda: teardown_after_execution(context))

    create_resource = getattr(resource, "create_resource", None)
    if callable(create_resource):
        return create_resource(context), tuple(finalizers)

    resource_fn = getattr(resource, "resource_fn", None)
    if callable(resource_fn):
        created = resource_fn(context)
        if isgenerator(created):
            try:
                value = next(created)
            except StopIteration as exc:
                raise RuntimeError("resource generator did not yield a value") from exc
            finalizers.append(lambda: _close_resource_generator(created))
            return value, tuple(finalizers)
        return created, tuple(finalizers)

    return resource, tuple(finalizers)


def _close_resource_generator(generator: object) -> None:
    try:
        next(generator)  # type: ignore[arg-type]
    except StopIteration:
        return
    raise RuntimeError("resource generator yielded more than once")


def _run_finalizers(finalizers: Iterable[Callable[[], None]]) -> None:
    for finalizer in reversed(tuple(finalizers)):
        finalizer()


def _raise_infrastructure_unavailable(
    *,
    resource_key: str,
    phase: PhaseEnum,
    policy_path: str,
    context: object,
    exc: BaseException,
) -> NoReturn:
    event = InfrastructureUnavailableEvent(
        resource_key=resource_key,
        phase=phase,
        reason=_exception_summary(exc),
    )
    try:
        policy = load_gate_policy(policy_path)
    except Exception:
        decision = _fallback_infrastructure_decision(phase)
        channels = _FALLBACK_ALERT_CHANNELS
    else:
        decision = classify_infrastructure_failure(phase, event, policy)
        channels = policy.alert_channels

    dispatch_gate_decision_alert(
        decision,
        cycle_id=_cycle_id_from_context(context),
        failed_node=resource_key,
        summary=_infra_summary(resource_key, exc),
        channels=channels,
    )
    raise InfrastructureUnavailableError(event, decision, exc) from exc


def _fallback_infrastructure_decision(phase: PhaseEnum) -> GateDecision:
    return GateDecision(
        phase=phase,
        failure_class=FailureClass.INFRA,
        action=GateAction.FAIL_RUN,
        reason=_INFRA_HARD_STOP_REASON,
        scenario_id=INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID,
    )


def _infra_summary(resource_key: str, exc: BaseException) -> str:
    return f"{resource_key} unavailable: {_exception_summary(exc)}"


def _exception_summary(exc: BaseException) -> str:
    message = str(exc)
    if message:
        return f"{type(exc).__name__}: {message}"
    return type(exc).__name__


def _cycle_id_from_context(context: object) -> str:
    dagster_run = getattr(context, "dagster_run", None)
    if dagster_run is None:
        dagster_run = getattr(context, "run", None)
    tags = getattr(dagster_run, "tags", {}) or {}
    cycle_id = tags.get("cycle_id") if isinstance(tags, Mapping) else None
    if isinstance(cycle_id, str) and cycle_id:
        return cycle_id

    run_id = getattr(context, "run_id", None)
    return run_id if isinstance(run_id, str) and run_id else "unknown-infra-run"


__all__ = [
    "CORE_INFRASTRUCTURE_RESOURCE_KEYS",
    "INFRASTRUCTURE_RESOURCE_PHASES",
    "INFRA_UNAVAILABLE_HARD_STOP_SCENARIO_ID",
    "InfrastructureUnavailableError",
    "InfrastructureUnavailableEvent",
    "classify_infrastructure_failure",
    "guard_infrastructure_resource",
]
