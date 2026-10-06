"""Configured public address zones."""

from __future__ import annotations

import threading

from .model import Zone


class ZoneTable:
    """Maps stable zone identifiers to physical zone controllers."""

    def __init__(self) -> None:
        self._zones: dict[str, Zone] = {}
        self._revision = 0
        self._lock = threading.RLock()

    def add(self, zone: Zone) -> Zone:
        with self._lock:
            self._zones[zone.id] = zone
            self._revision += 1
            return zone

    def get(self, zone_id: str) -> Zone | None:
        with self._lock:
            return self._zones.get(zone_id)

    def exists(self, zone_id: str) -> bool:
        return self.get(zone_id) is not None

    def ids(self) -> list[str]:
        with self._lock:
            return [key for key in sorted(self._zones) if self._zones[key].enabled]

    def list(self) -> list[Zone]:
        with self._lock:
            return [self._zones[key] for key in sorted(self._zones)]

    def count(self) -> int:
        with self._lock:
            return len(self._zones)

    def revision(self) -> int:
        return self._revision

    def describe(self) -> str:
        return f"pa zones={self.count()} revision={self.revision()}"

    def as_dict(self) -> dict[str, object]:
        return {
            "zones": [zone.as_dict() for zone in self.list()],
            "revision": self.revision(),
            "count": self.count(),
        }
