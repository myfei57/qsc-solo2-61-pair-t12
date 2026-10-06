"""Local hardware doubles and protocol descriptions.

The service owns the ordering and state machine. A concrete field device only
has to accept a fenced playback command, stop the current output, or drive the
hard-wired local evacuation indicators.
"""

from __future__ import annotations

from typing import Protocol


class PlaybackController(Protocol):
    """Commands accepted by one zone playback controller."""

    def play(self, zone: str, token: int, broadcast_id: str, content: str) -> None:
        """Immediately replace current output when the fence token is current."""

    def stop(self, zone: str, token: int) -> None:
        """Stop the output associated with the supplied fence token."""

    def online(self, zone: str) -> bool:
        """Report whether the zone controller is accepting commands."""


class IndicatorBank(Protocol):
    """Hard-wired local evacuation indicators used after playback failure."""

    def activate(self, zone: str) -> None:
        """Turn on local flashing/evacuation guidance for one zone."""

    def clear(self, zone: str) -> None:
        """Clear local guidance once a broadcast can safely take over."""

    def active(self, zone: str) -> bool:
        """Report whether local guidance is active in the zone."""


class LocalPlaybackController:
    """In-memory controller used by tests and the deterministic console."""

    def __init__(self, failing_play_zones: set[str] | None = None) -> None:
        self.commands: list[tuple[str, int, str, str]] = []
        self.stops: list[tuple[str, int]] = []
        self._tokens: dict[str, int] = {}
        self._offline: set[str] = set()
        self._failing_play = set(failing_play_zones or ())

    def play(self, zone: str, token: int, broadcast_id: str, content: str) -> None:
        if zone in self._offline or zone in self._failing_play:
            raise RuntimeError(f"playback controller unavailable in {zone}")
        self._tokens[zone] = token
        self.commands.append((zone, token, broadcast_id, content))

    def stop(self, zone: str, token: int) -> None:
        if zone in self._offline:
            raise RuntimeError(f"playback controller offline in {zone}")
        if self._tokens.get(zone) != token:
            raise RuntimeError(f"stale stop token in {zone}")
        self._tokens.pop(zone, None)
        self.stops.append((zone, token))

    def online(self, zone: str) -> bool:
        return zone not in self._offline

    def set_offline(self, zone: str, offline: bool = True) -> None:
        if offline:
            self._offline.add(zone)
        else:
            self._offline.discard(zone)

    def set_play_failure(self, zone: str, failing: bool = True) -> None:
        if failing:
            self._failing_play.add(zone)
        else:
            self._failing_play.discard(zone)


class LocalIndicatorBank:
    """In-memory fallback indicator bank used by tests and the console."""

    def __init__(self) -> None:
        self._active: set[str] = set()

    def activate(self, zone: str) -> None:
        self._active.add(zone)

    def clear(self, zone: str) -> None:
        self._active.discard(zone)

    def active(self, zone: str) -> bool:
        return zone in self._active

    def zones(self) -> list[str]:
        return sorted(self._active)
