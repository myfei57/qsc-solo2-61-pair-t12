"""Hardware boundary: playback controller and local indicator drivers.

Production code implements the protocols against the amplifier/zone
controller SDK. The in-memory simulators make the behaviour testable and
let tests force failure, outage and spontaneous mid-play faults.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class PlaybackResult:
    """What the amplifier reports after a hard switch / play request."""

    ok: bool
    detail: str = ""
    interrupted_key: str = ""
    interrupted_priority: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "detail": self.detail,
            "interrupted_key": self.interrupted_key,
        }


class PlaybackController(Protocol):
    """Drives one zone's loudspeaker amplifier / playback controller."""

    def ping(self, zone: str) -> bool:  # pragma: no cover - structural
        ...

    def play(self, zone: str, key: str, priority: int, epoch: int) -> PlaybackResult:  # pragma: no cover - structural
        ...

    def stop(self, zone: str, epoch: int) -> bool:  # pragma: no cover - structural
        ...


class IndicatorController(Protocol):
    """Drives the local evacuation indication (就地疏散指示) of a zone."""

    def activate(self, zone: str) -> bool:  # pragma: no cover - structural
        ...

    def deactivate(self, zone: str) -> bool:  # pragma: no cover - structural
        ...

    def is_on(self, zone: str) -> bool:  # pragma: no cover - structural
        ...


@dataclass
class _Channel:
    online: bool = True
    key: str = ""
    epoch: int = 0
    priority: int = 0
    fail_next: bool = False
    play_calls: int = 0
    cuts: list[str] = field(default_factory=list)


class SimAmplifier:
    """In-memory playback controller.

    ``play`` models the fixed ``immediate`` decision: the previous audio is
    cut at once and the interrupted key is reported back so the service can
    re-queue it.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._channels: dict[str, _Channel] = {}

    def add(self, zone: str) -> None:
        with self._lock:
            self._channels.setdefault(zone, _Channel())

    def _channel(self, zone: str) -> _Channel:
        channel = self._channels.get(zone)
        if channel is None:
            channel = _Channel(online=False)
            self._channels[zone] = channel
        return channel

    def ping(self, zone: str) -> bool:
        with self._lock:
            return self._channel(zone).online

    def play(self, zone: str, key: str, priority: int, epoch: int) -> PlaybackResult:
        with self._lock:
            channel = self._channel(zone)
            channel.play_calls += 1
            if not channel.online:
                return PlaybackResult(False, "amplifier offline")
            interrupted_key = channel.key
            interrupted_priority = channel.priority
            if channel.key:
                channel.cuts.append(channel.key)
            channel.key = key
            channel.priority = priority
            channel.epoch = epoch
            if channel.fail_next:
                channel.fail_next = False
                channel.key = ""
                channel.epoch = 0
                channel.priority = 0
                return PlaybackResult(False, "playback controller rejected the switch", interrupted_key, interrupted_priority)
            return PlaybackResult(True, interrupted_key=interrupted_key, interrupted_priority=interrupted_priority)

    def stop(self, zone: str, epoch: int) -> bool:
        with self._lock:
            channel = self._channel(zone)
            channel.key = ""
            channel.epoch = 0
            channel.priority = 0
            return channel.online

    # -- test/ops fault injection -------------------------------------------

    def fail_next_play(self, zone: str) -> None:
        with self._lock:
            self._channel(zone).fail_next = True

    def take_down(self, zone: str) -> None:
        with self._lock:
            channel = self._channel(zone)
            channel.online = False
            channel.key = ""
            channel.epoch = 0

    def bring_up(self, zone: str) -> None:
        with self._lock:
            channel = self._channel(zone)
            channel.online = True
            channel.fail_next = False

    def is_playing(self, zone: str) -> bool:
        with self._lock:
            return bool(self._channel(zone).key)

    def current_key(self, zone: str) -> str:
        with self._lock:
            return self._channel(zone).key

    def cuts(self, zone: str) -> list[str]:
        with self._lock:
            return list(self._channel(zone).cuts)


class SimIndicator:
    """In-memory local evacuation indicator panel."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._on: set[str] = set()

    def activate(self, zone: str) -> bool:
        with self._lock:
            self._on.add(zone)
            return True

    def deactivate(self, zone: str) -> bool:
        with self._lock:
            self._on.discard(zone)
            return True

    def is_on(self, zone: str) -> bool:
        with self._lock:
            return zone in self._on

    def zones_on(self) -> list[str]:
        with self._lock:
            return sorted(self._on)
