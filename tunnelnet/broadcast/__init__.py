"""Emergency broadcast zoning, priority preemption and fail-safe fallback."""

from .events import Event
from .store import EventStore
from .hardware import (
    IndicatorController,
    PlaybackController,
    PlaybackResult,
    SimAmplifier,
    SimIndicator,
)
from .model import (
    FALLBACK_SCOPE,
    PREEMPT_MODE,
    Broadcast,
    CommandResult,
    Priority,
    Zone,
)
from .monitor import HeartbeatMonitor
from .service import BroadcastService, make_broadcast
from .state import PlaybackState, fold

__all__ = [
    "Broadcast",
    "BroadcastService",
    "CommandResult",
    "Event",
    "EventStore",
    "FALLBACK_SCOPE",
    "HeartbeatMonitor",
    "IndicatorController",
    "PlaybackController",
    "PlaybackResult",
    "PlaybackState",
    "PREEMPT_MODE",
    "Priority",
    "SimAmplifier",
    "SimIndicator",
    "Zone",
    "fold",
    "make_broadcast",
]
