"""Data shared by the zone table, queue and audit projections."""

from __future__ import annotations

from dataclasses import dataclass, field

FIRE_PRIORITY = 10
GAS_PRIORITY = 20
MANUAL_PRIORITY = 30
INFORMATION_PRIORITY = 90


@dataclass(frozen=True)
class Broadcast:
    """One addressable announcement and the zones selected for it."""

    id: str
    content: str
    zones: tuple[str, ...]
    priority: int
    source: str = "operator"
    content_hash: str = ""
    queued_at: str = ""
    seq: int = 0

    def identity_key(self) -> str:
        return self.content_hash or self.id

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "content": self.content,
            "zones": list(self.zones),
            "priority": self.priority,
            "source": self.source,
            "content_hash": self.content_hash,
            "queued_at": self.queued_at,
            "seq": self.seq,
            "identity_key": self.identity_key(),
        }


@dataclass
class ZonePlayback:
    """Authoritative runtime state for one loudspeaker zone."""

    zone: str
    state: str = "idle"
    current: Broadcast | None = None
    queued: list[Broadcast] = field(default_factory=list)
    interrupted: Broadcast | None = None
    controller_online: bool = True
    fallback: bool = False
    last_error: str = ""
    revision: int = 0
    session_token: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "zone": self.zone,
            "state": self.state,
            "current": None if self.current is None else self.current.as_dict(),
            "queued": [item.as_dict() for item in self.queued],
            "interrupted": None if self.interrupted is None else self.interrupted.as_dict(),
            "controller_online": self.controller_online,
            "fallback": self.fallback,
            "last_error": self.last_error,
            "revision": self.revision,
            "session_token": self.session_token,
        }


@dataclass
class Zone:
    """Configuration of one addressable zone."""

    id: str
    name: str
    controller_id: str
    enabled: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "name": self.name,
            "controller_id": self.controller_id,
            "enabled": self.enabled,
        }


@dataclass(frozen=True)
class AuditEntry:
    """One playback state transition recorded in the audit trail."""

    id: int
    event: str
    at: str
    broadcast_id: str = ""
    zone: str = ""
    revision: int = 0
    detail: str = ""
    priority: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "event": self.event,
            "at": self.at,
            "broadcast_id": self.broadcast_id,
            "zone": self.zone,
            "revision": self.revision,
            "detail": self.detail,
            "priority": self.priority,
        }


@dataclass(frozen=True)
class PlaybackState:
    """Immutable projection returned by every status and audit read."""

    revision: int
    zones: tuple[ZonePlayback, ...]
    zone_table: tuple[Zone, ...]
    broadcasts: dict[str, Broadcast]
    audit: tuple[AuditEntry, ...]
    zone_results: dict[str, dict[str, str]]

    def as_dict(self) -> dict[str, object]:
        return {
            "revision": self.revision,
            "zones": [zone.as_dict() for zone in self.zones],
            "zone_table": [zone.as_dict() for zone in self.zone_table],
            "broadcasts": {key: item.as_dict() for key, item in self.broadcasts.items()},
            "audit": [entry.as_dict() for entry in self.audit],
            "zone_results": {
                key: dict(values) for key, values in self.zone_results.items()
            },
        }
