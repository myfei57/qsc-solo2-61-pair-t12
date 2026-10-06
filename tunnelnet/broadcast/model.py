"""Domain model for emergency broadcast zoning and preemption.

Two design decisions are fixed here on purpose (see docs/decisions):

* ``PREEMPT_MODE`` is ``immediate``: a fire alarm hard-cuts whatever is
  playing, after a short attention tone. Waiting for a sentence boundary
  delays the evacuation order by an unbounded number of seconds.
* ``FALLBACK_SCOPE`` is ``zone``: when a broadcast fails on some, but not
  all, target zones, only the failed zones fall back to their local
  evacuation indicators; healthy zones keep the voice message. The
  in-flight command is still rolled back as one transaction so no zone is
  left half-commanded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum

# --- Fixed design decisions -------------------------------------------------

PREEMPT_MODE = "immediate"
"""High priority broadcasts cut the current audio immediately."""

FALLBACK_SCOPE = "zone"
"""Only the zones whose playback controller failed fall back to indicators."""

ATTENTION_TONE_SECONDS = 1.0
"""Short alert tone inserted before the cut-in message."""


class Priority(IntEnum):
    """Broadcast priority. Higher numbers interrupt lower numbers."""

    ROUTINE = 10
    NOTICE = 30
    ABNORMAL = 60
    FIRE = 90

    @classmethod
    def names(cls) -> dict[str, int]:
        return {member.name.lower(): int(member) for member in cls}

    @classmethod
    def coerce(cls, value: object) -> int:
        if isinstance(value, bool):
            raise ValueError("priority must be an integer 1..100 or a level name")
        if isinstance(value, int):
            if not 1 <= value <= 100:
                raise ValueError("priority must be between 1 and 100")
            return int(value)
        if isinstance(value, str):
            name = value.strip().lower()
            if name in cls.names():
                return cls.names()[name]
            if name.isdigit():
                number = int(name)
                if 1 <= number <= 100:
                    return number
        raise ValueError("priority must be an integer 1..100 or a level name")

    @staticmethod
    def label(value: int) -> str:
        for member in Priority:
            if int(member) == value:
                return member.name.lower()
        return str(value)


# --- Configuration ----------------------------------------------------------


@dataclass(frozen=True)
class Zone:
    """One loudspeaker partition (管廊分区) with a playback controller."""

    id: str
    name: str = ""
    enabled: bool = True

    def as_dict(self) -> dict[str, object]:
        return {"id": self.id, "name": self.name, "enabled": self.enabled}


@dataclass(frozen=True)
class Broadcast:
    """One queued or playing announcement."""

    key: str
    title: str
    priority: int
    duration: float
    loop: bool = False
    source: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "title": self.title,
            "priority": self.priority,
            "priority_label": Priority.label(self.priority),
            "duration": self.duration,
            "loop": self.loop,
            "source": self.source,
        }


# Zone projection states
ST_IDLE = "idle"
ST_PLAYING = "playing"
ST_INDICATOR = "indicator"
ST_OFFLINE = "offline"

# Command lifecycle states
CMD_ACCEPTED = "accepted"
CMD_PLAYING = "playing"
CMD_FINISHED = "finished"
CMD_PREEMPTED = "preempted"
CMD_QUEUED = "queued"
CMD_ROLLED_BACK = "rolled_back"
CMD_REJECTED = "rejected"
CMD_DUPLICATE = "duplicate"


@dataclass
class QueuedBroadcast:
    """A broadcast waiting in a zone's priority queue."""

    broadcast: Broadcast
    queued_at: str
    reason: str = "submitted"
    seq: int = 0

    def as_dict(self) -> dict[str, object]:
        data = self.broadcast.as_dict()
        data.update({"queued_at": self.queued_at, "reason": self.reason})
        return data


@dataclass
class ZoneView:
    """The projected playback state of one zone."""

    zone_id: str
    name: str = ""
    enabled: bool = True
    online: bool = True
    state: str = ST_IDLE
    indicator_on: bool = False
    epoch: int = 0
    current: Broadcast | None = None
    current_epoch: int = 0
    current_started_at: str = ""
    resume_offset: float = 0.0
    queue: list[QueuedBroadcast] = field(default_factory=list)
    last_error: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "zone_id": self.zone_id,
            "name": self.name,
            "enabled": self.enabled,
            "online": self.online,
            "state": self.state,
            "indicator_on": self.indicator_on,
            "epoch": self.epoch,
            "current": None if self.current is None else self.current.as_dict(),
            "current_epoch": self.current_epoch,
            "current_started_at": self.current_started_at,
            "resume_offset": self.resume_offset,
            "queue": [item.as_dict() for item in self.queue],
            "queue_depth": len(self.queue),
            "last_error": self.last_error,
        }


@dataclass
class CommandResult:
    """Outcome of one broadcast trigger across its target zones."""

    command_id: str
    key: str
    broadcast: Broadcast
    status: str
    zones: list[str]
    started: list[str] = field(default_factory=list)
    queued: list[str] = field(default_factory=list)
    indicator: list[str] = field(default_factory=list)
    rolled_back: list[str] = field(default_factory=list)
    restored: dict[str, str] = field(default_factory=dict)
    duplicate: bool = False
    detail: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "command_id": self.command_id,
            "key": self.key,
            "broadcast": self.broadcast.as_dict(),
            "status": self.status,
            "preempt_mode": PREEMPT_MODE,
            "fallback_scope": FALLBACK_SCOPE,
            "zones": self.zones,
            "started": self.started,
            "queued": self.queued,
            "indicator": self.indicator,
            "rolled_back": self.rolled_back,
            "restored": self.restored,
            "duplicate": self.duplicate,
            "detail": self.detail,
        }
