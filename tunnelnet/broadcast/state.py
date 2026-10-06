"""Projection folded from the event stream.

There is exactly one playback state: the state in this module, rebuilt by
folding :mod:`tunnelnet.broadcast.events` in order. The zone table, the
per-zone priority queue, the command ledger, the console API and the audit
view all read this projection, so the console can never disagree with the
field.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import events as ev
from .model import (
    CMD_ACCEPTED,
    CMD_DUPLICATE,
    FALLBACK_SCOPE,
    PREEMPT_MODE,
    Broadcast,
    QueuedBroadcast,
    Zone,
    ZoneView,
)
from .events import Event

_REASON_RANK = {"preempted": 0, "restored": 0, "submitted": 1}


@dataclass
class CommandState:
    """One broadcast command as seen through the command ledger."""

    command_id: str
    key: str
    title: str
    priority: int
    duration: float
    loop: bool
    source: str
    status: str
    zones: tuple[str, ...]
    started: list[str] = field(default_factory=list)
    queued: list[str] = field(default_factory=list)
    indicator: list[str] = field(default_factory=list)
    rolled_back: list[str] = field(default_factory=list)
    restored: dict[str, str] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""
    detail: str = ""
    duplicate_of: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "command_id": self.command_id,
            "key": self.key,
            "title": self.title,
            "priority": self.priority,
            "duration": self.duration,
            "loop": self.loop,
            "source": self.source,
            "status": self.status,
            "zones": list(self.zones),
            "started": list(self.started),
            "queued": list(self.queued),
            "indicator": list(self.indicator),
            "rolled_back": list(self.rolled_back),
            "restored": dict(self.restored),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "detail": self.detail,
            "duplicate_of": self.duplicate_of,
        }


class PlaybackState:
    """The folded playback state shared by every reader."""

    def __init__(self) -> None:
        self.last_seq = 0
        self.zones: dict[str, Zone] = {}
        self.views: dict[str, ZoneView] = {}
        self.commands: dict[str, CommandState] = {}

    # -- queue helpers ------------------------------------------------------

    def _queue_find(self, view: ZoneView, key: str) -> QueuedBroadcast | None:
        for item in view.queue:
            if item.broadcast.key == key:
                return item
        return None

    def _queue_put(self, view: ZoneView, event: Event, reason: str) -> None:
        broadcast = Broadcast(
            key=event.key,
            title=event.data.get("title", ""),
            priority=event.priority,
            duration=float(event.data.get("duration", 0.0)),
            loop=bool(event.data.get("loop", False)),
            source=event.data.get("source", ""),
        )
        order_seq = int(event.data.get("order_seq", event.seq))
        item = QueuedBroadcast(
            broadcast=broadcast, queued_at=event.at, reason=reason, seq=order_seq
        )
        existing = self._queue_find(view, event.key)
        if existing is not None:
            existing.queued_at = event.at
            existing.reason = reason
            existing.seq = order_seq
        else:
            view.queue.append(item)
        view.queue.sort(key=lambda queued: (-queued.broadcast.priority, _REASON_RANK.get(queued.reason, 9), queued.seq))

    def _queue_remove(self, view: ZoneView, key: str) -> bool:
        for index, item in enumerate(view.queue):
            if item.broadcast.key == key:
                del view.queue[index]
                return True
        return False

    def _set_state(self, view: ZoneView, state: str) -> None:
        if not view.online:
            view.state = "offline"
        elif view.current is not None:
            # Live voice always wins the display, even when an indicator
            # event arrives out of order after playback_started.
            view.state = "playing"
        elif view.indicator_on:
            view.state = "indicator"
        else:
            view.state = "idle" if state == "playing" else state

    # -- fold ---------------------------------------------------------------

    def apply(self, event: Event) -> None:
        self.last_seq = event.seq

        if event.event == ev.EV_ZONE_REGISTERED:
            zone = Zone(
                id=event.zone,
                name=event.data.get("name", ""),
                enabled=bool(event.data.get("enabled", True)),
            )
            self.zones[zone.id] = zone
            view = self.views.get(zone.id)
            if view is None:
                view = ZoneView(zone_id=zone.id)
                self.views[zone.id] = view
            view.name = zone.name
            view.enabled = zone.enabled
            self._track_command(event)
            return

        view = self.views.get(event.zone) if event.zone else None
        if view is None:
            # Command-level events carry no zone but still update the ledger.
            self._track_command(event)
            return  # events for unknown zones are ignored by zone views

        if event.event == ev.EV_ZONE_ENABLED:
            view.enabled = True
            if event.zone in self.zones:
                enabled_zone = Zone(id=event.zone, name=view.name, enabled=True)
                self.zones[event.zone] = enabled_zone
        elif event.event == ev.EV_ZONE_DISABLED:
            view.enabled = False
            if event.zone in self.zones:
                disabled_zone = Zone(id=event.zone, name=view.name, enabled=False)
                self.zones[event.zone] = disabled_zone

        elif event.event == ev.EV_DEVICE_DOWN:
            view.online = False
            view.state = "offline"
            view.current = None
            view.last_error = event.detail

        elif event.event == ev.EV_DEVICE_UP:
            view.online = True
            view.last_error = ""
            self._set_state(view, "idle")

        elif event.event == ev.EV_INDICATOR_ON:
            view.indicator_on = True
            self._set_state(view, "idle")

        elif event.event == ev.EV_INDICATOR_OFF:
            view.indicator_on = False
            self._set_state(view, "idle")

        elif event.event == ev.EV_PLAYBACK_QUEUED:
            self._queue_put(view, event, event.data.get("reason", "submitted"))
            self._track_command_zone(event, "queued")

        elif event.event == ev.EV_PLAYBACK_DEQUEUED:
            self._queue_remove(view, event.key)

        elif event.event == ev.EV_PLAYBACK_PREEMPTED:
            # The displaced broadcast is re-queued so it can resume once the
            # higher priority command ends.
            if event.data.get("displaced_key"):
                displaced = Event(
                    seq=event.seq,
                    at=event.at,
                    event=event.event,
                    zone=event.zone,
                    command_id=event.data.get("displaced_command", ""),
                    key=event.data["displaced_key"],
                    priority=int(event.data.get("displaced_priority", 0)),
                    data={
                        "title": event.data.get("displaced_title", ""),
                        "duration": event.data.get("displaced_duration", 0.0),
                        "loop": event.data.get("displaced_loop", False),
                        "source": event.data.get("displaced_source", ""),
                        "order_seq": int(event.data.get("displaced_order_seq", event.seq)),
                    },
                )
                self._queue_put(view, displaced, "preempted")

        elif event.event == ev.EV_PLAYBACK_STARTING:
            view.epoch = event.epoch
            view.current = Broadcast(
                key=event.key,
                title=event.data.get("title", ""),
                priority=event.priority,
                duration=float(event.data.get("duration", 0.0)),
                loop=bool(event.data.get("loop", False)),
                source=event.data.get("source", ""),
            )
            view.current_epoch = event.epoch
            view.current_started_at = event.at
            view.resume_offset = float(event.data.get("resume_offset", 0.0))
            self._queue_remove(view, event.key)
            self._set_state(view, "playing")

        elif event.event == ev.EV_PLAYBACK_STARTED:
            view.indicator_on = False
            view.last_error = ""
            self._set_state(view, "playing")
            self._track_command_zone(event, "started")

        elif event.event == ev.EV_PLAYBACK_FAILED:
            view.last_error = event.detail
            view.resume_offset = 0.0
            view.current = None
            view.current_epoch = 0
            view.current_started_at = ""
            view.indicator_on = True
            self._set_state(view, "idle")
            self._track_command_zone(event, "indicator")

        elif event.event == ev.EV_PLAYBACK_FINISHED:
            if view.current is not None and view.current.key == event.key:
                view.current = None
                view.current_epoch = 0
                view.current_started_at = ""
                view.resume_offset = 0.0
            self._set_state(view, "idle")

        elif event.event == ev.EV_PLAYBACK_RESTORED:
            # Compensation epoch invalidates every callback of the rolled
            # back command; the displaced owner is handed back in order.
            view.epoch = event.epoch
            self._queue_remove(view, event.key)
            if event.data.get("restored_key"):
                restored = Event(
                    seq=event.seq,
                    at=event.at,
                    event=event.event,
                    zone=event.zone,
                    command_id=event.data.get("restored_command", ""),
                    key=event.data["restored_key"],
                    priority=int(event.data.get("restored_priority", 0)),
                    data={
                        "title": event.data.get("restored_title", ""),
                        "duration": event.data.get("restored_duration", 0.0),
                        "loop": event.data.get("restored_loop", False),
                        "source": event.data.get("restored_source", ""),
                        "order_seq": int(event.data.get("restored_order_seq", event.seq)),
                    },
                )
                self._queue_put(view, restored, "restored")

        self._track_command(event)

    def _track_command_zone(self, event: Event, bucket: str) -> None:
        if not event.command_id:
            return
        command = self.commands.get(event.command_id)
        if command is None:
            return
        target = {
            "started": command.started,
            "queued": command.queued,
            "indicator": command.indicator,
        }.get(bucket)
        if target is None:
            return
        if event.zone not in target:
            target.append(event.zone)
        for other in (command.started, command.queued, command.indicator):
            if other is not target and event.zone in other:
                other.remove(event.zone)

    def _track_command(self, event: Event) -> None:
        if not event.command_id or event.command_id not in self.commands:
            return
        command = self.commands[event.command_id]
        command.updated_at = event.at
        if event.event == ev.EV_COMMAND_REJECTED:
            command.status = "rejected"
            command.detail = event.detail
        elif event.event == ev.EV_COMMAND_DUPLICATE:
            command.status = "duplicate"
        elif event.event == ev.EV_COMMAND_ROLLED_BACK:
            command.detail = event.detail
            if event.data.get("compensated"):
                # Healthy zones were actually compensated (saga rollback).
                command.status = "rolled_back"
            elif command.started or command.queued:
                # Some healthy zones kept voice; failed zones indicate.
                command.status = "partial"
            elif command.indicator:
                # Every target zone lost voice.
                command.status = "partial"
        elif event.event == ev.EV_COMMAND_FINISHED:
            command.status = "finished"
        elif event.event == ev.EV_PLAYBACK_FAILED and command.status not in (
            "rolled_back",
            "finished",
            "rejected",
        ):
            # A failed zone may still have healthy siblings playing; the
            # command ledger settles the final rolled_back/partial status.
            command.status = "partial"
        elif event.event == ev.EV_PLAYBACK_STARTED and command.status in (CMD_ACCEPTED, "queued", "preempted"):
            command.status = "playing"
        elif event.event == ev.EV_PLAYBACK_QUEUED and command.status in (CMD_ACCEPTED, "playing"):
            command.status = "queued"
        if event.event == ev.EV_PLAYBACK_RESTORED:
            if event.zone not in command.rolled_back:
                command.rolled_back.append(event.zone)
            # An empty restored key means "restore the zone to idle".
            command.restored[event.zone] = event.data.get("restored_key", "")

    def register_command(self, event: Event) -> None:
        if event.command_id in self.commands:
            return
        self.commands[event.command_id] = CommandState(
            command_id=event.command_id,
            key=event.key,
            title=event.data.get("title", ""),
            priority=event.priority,
            duration=float(event.data.get("duration", 0.0)),
            loop=bool(event.data.get("loop", False)),
            source=event.data.get("source", ""),
            status="duplicate" if event.event == ev.EV_COMMAND_DUPLICATE else CMD_ACCEPTED,
            zones=tuple(event.data.get("zones", [])),
            created_at=event.at,
            updated_at=event.at,
            detail=event.detail,
            duplicate_of=event.data.get("duplicate_of", ""),
        )
        self.last_seq = event.seq

    # -- reads --------------------------------------------------------------

    def zone_view(self, zone_id: str) -> ZoneView | None:
        return self.views.get(zone_id)

    def command(self, command_id: str) -> CommandState | None:
        return self.commands.get(command_id)

    def command_by_key(self, key: str) -> CommandState | None:
        for command in self.commands.values():
            if command.key == key:
                return command
        return None

    def decision(self) -> dict[str, str]:
        return {"preempt_mode": PREEMPT_MODE, "fallback_scope": FALLBACK_SCOPE}

    def snapshot(self) -> dict[str, object]:
        return {
            "decision": self.decision(),
            "last_seq": self.last_seq,
            "zones": [self.views[key].as_dict() for key in sorted(self.views)],
            "commands": [
                self.commands[key].as_dict() for key in sorted(self.commands)
            ],
            "counts": {
                "zones": len(self.views),
                "online": sum(1 for view in self.views.values() if view.online),
                "playing": sum(1 for view in self.views.values() if view.state == "playing"),
                "indicator": sum(1 for view in self.views.values() if view.indicator_on),
                "queued": sum(len(view.queue) for view in self.views.values()),
                "commands": len(self.commands),
            },
        }


def fold(events_list: list[Event]) -> PlaybackState:
    """Rebuild the whole playback state from its event history."""

    state = PlaybackState()
    for event in events_list:
        if event.event in (ev.EV_COMMAND_SUBMITTED, ev.EV_COMMAND_DUPLICATE):
            state.register_command(event)
        state.apply(event)
    return state
