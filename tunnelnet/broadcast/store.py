"""Thread-safe append-only event store for playback events."""

from __future__ import annotations

import threading
from collections.abc import Callable

from .events import Event

Sink = Callable[[Event], None]


class EventStore:
    """Holds every playback event in global sequence order.

    The store is the durable boundary: in production it can be backed by the
    file :class:`~tunnelnet.store.Store`; here it keeps events in memory and
    notifies optional sinks (the console audit stream is wired as one).
    """

    def __init__(self, sinks: list[Sink] | None = None) -> None:
        self._events: list[Event] = []
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._sinks: list[Sink] = list(sinks or [])
        self._seq = 0

    def add_sink(self, sink: Sink) -> None:
        with self._lock:
            self._sinks.append(sink)

    def append(
        self,
        at: str,
        event: str,
        *,
        zone: str = "",
        command_id: str = "",
        key: str = "",
        priority: int = 0,
        epoch: int = 0,
        detail: str = "",
        data: dict | None = None,
    ) -> Event:
        with self._cond:
            self._seq += 1
            record = Event(
                seq=self._seq,
                at=at,
                event=event,
                zone=zone,
                command_id=command_id,
                key=key,
                priority=priority,
                epoch=epoch,
                detail=detail,
                data=dict(data or {}),
            )
            self._events.append(record)
            for sink in list(self._sinks):
                sink(record)
            self._cond.notify_all()
            return record

    def all(self) -> list[Event]:
        with self._lock:
            return list(self._events)

    def since(self, seq: int) -> list[Event]:
        with self._lock:
            return [event for event in self._events if event.seq > seq]

    def last_seq(self) -> int:
        with self._lock:
            return self._seq

    def count(self) -> int:
        with self._lock:
            return len(self._events)

    def wait_seq(self, seq: int, timeout: float = 0.5) -> bool:
        """Block until an event newer than ``seq`` exists. Used by polling."""

        with self._cond:
            return self._cond.wait_for(lambda: self._seq > seq, timeout=timeout)

    def clear(self) -> None:
        with self._cond:
            self._events.clear()
            self._seq = 0
            self._cond.notify_all()

    def query(
        self,
        *,
        event: str = "",
        zone: str = "",
        command_id: str = "",
        key: str = "",
    ) -> list[Event]:
        with self._lock:
            result = []
            for item in self._events:
                if event and item.event != event:
                    continue
                if zone and item.zone != zone:
                    continue
                if command_id and item.command_id != command_id:
                    continue
                if key and item.key != key:
                    continue
                result.append(item)
            return result
