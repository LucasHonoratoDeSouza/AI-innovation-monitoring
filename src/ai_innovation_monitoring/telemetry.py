from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
from threading import Lock
from typing import Any

from ai_innovation_monitoring.domain import isoformat, stable_hash, utcnow


@dataclass(slots=True)
class ActivityEvent:
    event_id: str
    created_at: str
    stage: str
    kind: str
    message: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class WorkerState:
    name: str
    status: str = "idle"
    current_item: str = ""
    updated_at: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class TelemetryHub:
    def __init__(self, max_events: int = 400) -> None:
        self._lock = Lock()
        self._events: deque[ActivityEvent] = deque(maxlen=max_events)
        self._workers: dict[str, WorkerState] = {}

    def log(self, stage: str, kind: str, message: str, payload: dict[str, Any] | None = None) -> None:
        now = isoformat(utcnow()) or ""
        event = ActivityEvent(
            event_id=stable_hash(stage, kind, message, now)[:24],
            created_at=now,
            stage=stage,
            kind=kind,
            message=message,
            payload=payload or {},
        )
        with self._lock:
            self._events.appendleft(event)

    def set_worker(
        self,
        name: str,
        *,
        status: str,
        current_item: str = "",
        metrics: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            state = self._workers.get(name) or WorkerState(name=name)
            state.status = status
            state.current_item = current_item
            state.updated_at = isoformat(utcnow())
            if metrics is not None:
                state.metrics = dict(metrics)
            self._workers[name] = state

    def snapshot(self, limit: int = 80) -> dict[str, Any]:
        with self._lock:
            events = [event.to_dict() for event in list(self._events)[:limit]]
            workers = {name: state.to_dict() for name, state in self._workers.items()}
        return {
            "activity": events,
            "workers": workers,
        }
