"""Append-only playback events: the single source of truth.

The zone table, the playback queue, the audit trail and the console
projection never hold independent state: they are all views over the same
ordered event sequence. Every state change of a zone or a command is an
:class:`Event`, appended in order; the live playback state is folded from
these events, and the audit stream reads the same records.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any

# Command / trigger lifecycle
EV_COMMAND_DUPLICATE = "command_duplicate"
EV_COMMAND_REJECTED = "command_rejected"
EV_COMMAND_SUBMITTED = "command_submitted"
EV_COMMAND_FINISHED = "command_finished"
EV_COMMAND_ROLLED_BACK = "command_rolled_back"

# Per-zone playback transitions
EV_PLAYBACK_QUEUED = "playback_queued"
EV_PLAYBACK_STARTING = "playback_starting"
EV_PLAYBACK_STARTED = "playback_started"
EV_PLAYBACK_PREEMPTED = "playback_preempted"
EV_PLAYBACK_FAILED = "playback_failed"
EV_PLAYBACK_FINISHED = "playback_finished"
EV_PLAYBACK_DEQUEUED = "playback_dequeued"

# Rollback compensation and local-indicator fallback
EV_PLAYBACK_RESTORED = "playback_restored"
EV_INDICATOR_ON = "indicator_on"
EV_INDICATOR_OFF = "indicator_off"

# Device health
EV_DEVICE_DOWN = "device_down"
EV_DEVICE_UP = "device_up"

# Zone table
EV_ZONE_REGISTERED = "zone_registered"
EV_ZONE_DISABLED = "zone_disabled"
EV_ZONE_ENABLED = "zone_enabled"


@dataclass(frozen=True)
class Event:
    """One immutable fact about playback state.

    ``seq`` is the global, gap-free order number used by every projection
    and by the console's ``after`` (long-poll) cursor.
    """

    seq: int
    at: str
    event: str
    zone: str = ""
    command_id: str = ""
    key: str = ""
    priority: int = 0
    epoch: int = 0
    detail: str = ""
    data: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
