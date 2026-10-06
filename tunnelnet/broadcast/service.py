"""Emergency broadcast service: zoned playback, priority preemption.

Guarantees:

* **Immediate preemption** (fixed decision): a higher priority broadcast
  hard-cuts the current audio on every target zone; the displaced
  broadcast is re-queued in place and resumed afterwards.
* **Saga rollback with per-zone fallback**: a multi-zone command starts
  everywhere it can; any zone whose controller fails falls back to its
  local evacuation indicator, and the zones that did start are
  compensated (rolled back to what they were playing). Healthy zones from
  unrelated commands are never disturbed.
* **Ordering barrier**: each takeover advances a per-zone monotonic
  ``epoch``; every amplifier callback carrying a stale epoch is discarded,
  so late failures or completions can never clobber a newer broadcast.
* **Idempotency**: triggering the same broadcast key again while the
  command is live returns the original outcome without replaying.
"""

from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone

from . import events as ev
from .hardware import IndicatorController, PlaybackController, PlaybackResult
from .model import (
    ATTENTION_TONE_SECONDS,
    Broadcast,
    CommandResult,
    Priority,
    Zone,
)
from .state import PlaybackState, fold
from .store import EventStore

DEFAULT_DURATION = 30.0
HEARTBEAT_INTERVAL = 2.0


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _same_broadcast(existing: Broadcast, candidate: Broadcast) -> bool:
    return (
        existing.key == candidate.key
        and existing.priority == candidate.priority
        and existing.duration == candidate.duration
    )


class _Timer:
    """A timer whose callback can be proven stale by its epoch guard."""

    def __init__(self, service: "BroadcastService", zone: str, epoch: int, key: str) -> None:
        self._service = service
        self._zone = zone
        self._epoch = epoch
        self._key = key
        self._timer: threading.Timer | None = None

    def schedule(self, delay: float) -> None:
        self._timer = threading.Timer(delay, self.fire)
        self._timer.daemon = True
        self._timer.start()

    def fire(self) -> None:
        self._service._handle_completion(self._zone, self._epoch, self._key)

    def cancel(self) -> None:
        if self._timer is not None:
            self._timer.cancel()


class BroadcastService:
    """Zoned emergency broadcast with priority queueing and preemption."""

    def __init__(
        self,
        amplifier: PlaybackController,
        indicator: IndicatorController,
        store: EventStore | None = None,
    ) -> None:
        self._amp = amplifier
        self._indicator = indicator
        self.store = store or EventStore()
        self._lock = threading.RLock()
        self._state = fold(self.store.all())
        self._timers: dict[str, _Timer] = {}
        self._completion_seq = 0

    # -- time ----------------------------------------------------------------

    def now(self) -> str:
        return utc_now()

    # -- event funnel: every mutation goes through here ----------------------

    def _emit(self, event: str, **kwargs) -> "ev.Event":
        record = self.store.append(self.now(), event, **kwargs)
        if record.event in (ev.EV_COMMAND_SUBMITTED, ev.EV_COMMAND_DUPLICATE):
            self._state.register_command(record)
        self._state.apply(record)
        return record

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return self._state.snapshot()

    def state_view(self) -> PlaybackState:
        with self._lock:
            return self._state

    # -- zone table ----------------------------------------------------------

    def register_zone(self, zone: Zone) -> Zone:
        with self._lock:
            self._amp.add(zone.id) if hasattr(self._amp, "add") else None
            self._emit(ev.EV_ZONE_REGISTERED, zone=zone.id, data=zone.as_dict())
            return zone

    def register_zones(self, zones: list[Zone]) -> list[Zone]:
        return [self.register_zone(zone) for zone in zones]

    def set_zone_enabled(self, zone_id: str, enabled: bool) -> bool:
        with self._lock:
            if zone_id not in self._state.views:
                raise KeyError(f"unknown zone {zone_id}")
            self._emit(ev.EV_ZONE_ENABLED if enabled else ev.EV_ZONE_DISABLED, zone=zone_id)
            return enabled

    def zones(self) -> list[Zone]:
        with self._lock:
            return [self._state.zones[key] for key in sorted(self._state.zones)]

    # -- trigger -------------------------------------------------------------

    def trigger(
        self,
        broadcast: Broadcast,
        zones: list[str],
        *,
        idempotency_key: str = "",
    ) -> CommandResult:
        """Queue or preemptively play one broadcast across target zones.

        Idempotent: repeating the same key returns the stored outcome.
        """

        key = idempotency_key or broadcast.key
        with self._lock:
            # --- idempotency check -----------------------------------------
            existing = self._state.command_by_key(key)
            if existing is not None and existing.status in (
                "accepted",
                "playing",
                "partial",
            ):
                self._emit(
                    ev.EV_COMMAND_DUPLICATE,
                    command_id=uuid.uuid4().hex,
                    key=key,
                    priority=broadcast.priority,
                    detail="duplicate trigger suppressed",
                    data={**broadcast.as_dict(), "zones": zones, "duplicate_of": existing.command_id},
                )
                return CommandResult(
                    command_id=existing.command_id,
                    key=key,
                    broadcast=broadcast,
                    status="duplicate",
                    zones=list(zones),
                    started=list(existing.started),
                    queued=list(existing.queued),
                    indicator=list(existing.indicator),
                    duplicate=True,
                    detail=f"same result as {existing.command_id}",
                )

            target_ids = self._validate_zones(zones)
            command_id = uuid.uuid4().hex
            self._emit(
                ev.EV_COMMAND_SUBMITTED,
                command_id=command_id,
                key=key,
                priority=broadcast.priority,
                data={**broadcast.as_dict(), "zones": target_ids},
            )

            # --- prepare: no hardware is touched yet ------------------------
            prepared: dict[str, dict[str, object]] = {}
            rejected: list[str] = []
            offline: list[str] = []
            for zone_id in target_ids:
                view = self._state.views[zone_id]
                if not view.enabled:
                    rejected.append(zone_id)
                    continue
                # A fresh ping decides readiness; the projected online flag
                # only changes when a heartbeat observes it.
                if not view.online or not self._amp.ping(zone_id):
                    if view.online:
                        self._device_lost(zone_id, "amplifier offline at prepare")
                    # Voice was never attempted: fail straight to the local
                    # indicator, without entering the rollback set.
                    offline.append(zone_id)
                    self._safe_indicator(zone_id, True)
                    continue
                prepared[zone_id] = self._displacement(view)

            # --- commit: every prepared zone follows one decision path:
            # hard switch (preempt), queue behind stronger content, or fail ---
            started: list[str] = []
            queued: list[str] = []
            failed: list[str] = []
            for zone_id, displacement in prepared.items():
                outcome = self._start_zone(zone_id, broadcast, command_id, displacement)
                if outcome is None:
                    queued.append(zone_id)
                elif outcome.ok:
                    started.append(zone_id)
                else:
                    failed.append(zone_id)

            # --- compensate: roll back the zones that did start -------------
            # Only failures during commit are partial-execution failures and
            # justify compensating healthy zones. A zone known to be offline
            # during prepare never started anything, so the healthy zones
            # keep their voice.
            voice_lost = failed + offline
            rolled_back: list[str] = []
            restored: dict[str, str] = {}
            if failed and started:
                for zone_id in list(started):
                    restored_key = self._rollback_zone(zone_id, command_id, broadcast)
                    rolled_back.append(zone_id)
                    started.remove(zone_id)
                    restored[zone_id] = restored_key

            for zone_id in failed:
                # _start_zone already recorded playback_failed for these;
                # the only remaining job is the fail-safe local indication.
                self._safe_indicator(zone_id, True)

            # --- settle command status --------------------------------------
            attempted = started or queued or voice_lost
            if rejected and not attempted:
                self._emit(
                    ev.EV_COMMAND_REJECTED,
                    command_id=command_id,
                    key=key,
                    priority=broadcast.priority,
                    detail="no target zone was available",
                )
            elif voice_lost:
                if rolled_back:
                    detail = "partial rollback; healthy zones compensated, failed zones on local indication"
                elif started or queued:
                    detail = "partial delivery; failed zones on local indication"
                else:
                    detail = "all target zones fell back to local indication"
                self._emit(
                    ev.EV_COMMAND_ROLLED_BACK,
                    command_id=command_id,
                    key=key,
                    priority=broadcast.priority,
                    detail=detail,
                    data={"compensated": bool(rolled_back)},
                )

            command = self._state.commands[command_id]
            return CommandResult(
                command_id=command_id,
                key=key,
                broadcast=broadcast,
                status=command.status,
                zones=target_ids,
                started=started,
                queued=queued,
                indicator=voice_lost + rejected,
                rolled_back=rolled_back,
                restored=restored,
                detail=command.detail,
            )

    def _validate_zones(self, zones: list[str]) -> list[str]:
        if not zones:
            raise ValueError("at least one target zone is required")
        target_ids: list[str] = []
        for zone_id in zones:
            if zone_id not in self._state.views:
                raise KeyError(f"unknown zone {zone_id}")
            if zone_id not in target_ids:
                target_ids.append(zone_id)
        return target_ids

    def _next_order_seq(self) -> int:
        self._completion_seq += 1
        return self._completion_seq

    def _displacement(self, view) -> dict[str, object]:
        """Snapshot what would be displaced on one zone (prepare phase)."""

        if view.current is not None:
            return {
                "kind": "current",
                "key": view.current.key,
                "priority": view.current.priority,
                "title": view.current.title,
                "duration": view.current.duration,
                "loop": view.current.loop,
                "source": view.current.source,
                "command_id": self._command_id_for(view.current.key, view.zone_id),
                "order_seq": view.current_epoch,
                "resume_offset": 0.0,
            }
        if view.queue:
            head = view.queue[0]
            return {
                "kind": "queued",
                "key": head.broadcast.key,
                "priority": head.broadcast.priority,
                "title": head.broadcast.title,
                "duration": head.broadcast.duration,
                "loop": head.broadcast.loop,
                "source": head.broadcast.source,
                "command_id": self._command_id_for(head.broadcast.key, view.zone_id),
                "order_seq": head.seq,
            }
        return {"kind": "none"}

    def _command_id_for(self, key: str, zone_id: str) -> str:
        for command in self._state.commands.values():
            if command.key == key and zone_id in command.zones:
                return command.command_id
        return ""

    def _queue_find(self, view, key: str):
        return self._state._queue_find(view, key)

    def _start_zone(
        self,
        zone_id: str,
        broadcast: Broadcast,
        command_id: str,
        displacement: dict[str, object],
    ) -> PlaybackResult | None:
        """Commit one zone: queue, preempt, or hard-switch immediately."""

        view = self._state.views[zone_id]
        current = view.current
        queued_head = view.queue[0].broadcast if view.queue else None
        stronger = (
            (current is not None and current.priority >= broadcast.priority and not _same_broadcast(current, broadcast))
            or (current is None and queued_head is not None and queued_head.priority >= broadcast.priority)
        )
        if stronger:
            # Stronger content owns (or is first in line for) the speaker.
            if self._queue_find(view, broadcast.key) is None:
                self._emit(
                    ev.EV_PLAYBACK_QUEUED,
                    zone=zone_id,
                    command_id=command_id,
                    key=broadcast.key,
                    priority=broadcast.priority,
                    data={**broadcast.as_dict(), "reason": "submitted", "order_seq": self._next_order_seq()},
                )
            command = self._state.commands[command_id]
            if zone_id not in command.queued:
                command.queued.append(zone_id)
            return None

        epoch = view.epoch + 1
        # Cancel any pending completion of the previous broadcast first.
        old_timer = self._timers.pop(zone_id, None)
        if old_timer is not None:
            old_timer.cancel()

        if displacement.get("kind") in ("current", "queued"):
            # Immediate cut (or jump the queue): the displaced broadcast is
            # re-queued so it resumes after the higher priority message.
            displaced_command_id = str(displacement.get("command_id", ""))
            self._emit(
                ev.EV_PLAYBACK_PREEMPTED,
                zone=zone_id,
                command_id=displaced_command_id,
                key=str(displacement["key"]),
                priority=int(displacement["priority"]),
                epoch=epoch,
                detail=(
                    f"immediately cut by priority {broadcast.priority} after "
                    f"{ATTENTION_TONE_SECONDS:.0f}s attention tone"
                    if displacement["kind"] == "current"
                    else f"jumped the queue ahead by priority {broadcast.priority}"
                ),
                data={
                    "displaced_key": displacement["key"],
                    "displaced_command": displaced_command_id,
                    "displaced_priority": displacement["priority"],
                    "displaced_title": displacement["title"],
                    "displaced_duration": displacement["duration"],
                    "displaced_loop": displacement["loop"],
                    "displaced_source": displacement["source"],
                    "displaced_order_seq": displacement["order_seq"],
                    "new_key": broadcast.key,
                    "new_priority": broadcast.priority,
                    "mode": "immediate",
                },
            )
            if displaced_command_id:
                dcmd = self._state.commands.get(displaced_command_id)
                if dcmd is not None and zone_id in dcmd.started:
                    dcmd.started.remove(zone_id)
                if dcmd is not None and zone_id not in dcmd.queued:
                    dcmd.queued.append(zone_id)

        self._emit(
            ev.EV_PLAYBACK_STARTING,
            zone=zone_id,
            command_id=command_id,
            key=broadcast.key,
            priority=broadcast.priority,
            epoch=epoch,
            data={**broadcast.as_dict(), "resume_offset": 0.0},
        )
        result = self._amp.play(zone_id, broadcast.key, broadcast.priority, epoch)
        if result.ok:
            self._emit(
                ev.EV_PLAYBACK_STARTED,
                zone=zone_id,
                command_id=command_id,
                key=broadcast.key,
                priority=broadcast.priority,
                epoch=epoch,
                data={"interrupted_key": result.interrupted_key},
            )
            self._safe_indicator(zone_id, False)
            timer = _Timer(self, zone_id, epoch, broadcast.key)
            self._timers[zone_id] = timer
            timer.schedule(ATTENTION_TONE_SECONDS + broadcast.duration)
            return result

        # Hardware rejected the switch. The new epoch must still invalidate
        # anything in flight; the zone is heading to local indication.
        self._emit(
            ev.EV_PLAYBACK_FAILED,
            zone=zone_id,
            command_id=command_id,
            key=broadcast.key,
            priority=broadcast.priority,
            epoch=epoch,
            detail=result.detail or "playback controller failed",
        )
        return result

    def _rollback_zone(self, zone_id: str, failed_command_id: str, failed_broadcast: Broadcast) -> str:
        """Compensate one healthy zone after a sibling zone failed."""

        view = self._state.views[zone_id]
        old_timer = self._timers.pop(zone_id, None)
        if old_timer is not None:
            old_timer.cancel()
        finish_key = view.current.key if view.current is not None else ""
        finish_priority = view.current.priority if view.current is not None else 0
        finish_command = self._command_id_for(finish_key, zone_id) if finish_key else ""
        epoch = view.epoch + 1
        self._amp.stop(zone_id, epoch)
        if finish_key:
            # The hardware stop must be visible to every projection,
            # otherwise the console would still show the rolled-back voice.
            self._emit(
                ev.EV_PLAYBACK_FINISHED,
                zone=zone_id,
                command_id=finish_command,
                key=finish_key,
                priority=finish_priority,
                epoch=epoch,
                detail="stopped during saga compensation",
            )
        # Find the owner we displaced: the preempted entry is at the head of
        # the queue because re-queued preemptions sort ahead of submissions.
        restored_key = ""
        restored = None
        if view.queue:
            restored = view.queue[0]
            restored_key = restored.broadcast.key
        epoch = view.epoch + 1
        self._emit(
            ev.EV_PLAYBACK_RESTORED,
            zone=zone_id,
            command_id=failed_command_id,
            key=failed_broadcast.key,
            priority=failed_broadcast.priority,
            epoch=epoch,
            detail="saga compensation: rolled back after partial failure",
            data={
                "restored_key": restored_key,
                "restored_command": self._command_id_for(restored_key, zone_id) if restored_key else "",
                "restored_priority": restored.broadcast.priority if restored else 0,
                "restored_title": restored.broadcast.title if restored else "",
                "restored_duration": restored.broadcast.duration if restored else 0.0,
                "restored_loop": restored.broadcast.loop if restored else False,
                "restored_source": restored.broadcast.source if restored else "",
                "restored_order_seq": restored.seq if restored else 0,
            },
        )
        # Our failed broadcast must not linger in the queue.
        self._state._queue_remove(view, failed_broadcast.key)

        if restored is not None:
            resume = restored.broadcast
            command_id = self._command_id_for(restored_key, zone_id)
            result = self._amp.play(zone_id, resume.key, resume.priority, epoch)
            if result.ok:
                self._emit(
                    ev.EV_PLAYBACK_STARTING,
                    zone=zone_id,
                    command_id=command_id,
                    key=resume.key,
                    priority=resume.priority,
                    epoch=epoch,
                    data={**resume.as_dict(), "resume_offset": 0.0},
                )
                self._emit(
                    ev.EV_PLAYBACK_STARTED,
                    zone=zone_id,
                    command_id=command_id,
                    key=resume.key,
                    priority=resume.priority,
                    epoch=epoch,
                    data={"restored_after": failed_command_id},
                )
                self._state._queue_remove(view, restored_key)
                timer = _Timer(self, zone_id, epoch, restored_key)
                self._timers[zone_id] = timer
                timer.schedule(ATTENTION_TONE_SECONDS + resume.duration)
            else:
                # Compensation itself failed: fail safe.
                self._emit(
                    ev.EV_PLAYBACK_FAILED,
                    zone=zone_id,
                    command_id=command_id,
                    key=restored_key,
                    priority=resume.priority,
                    epoch=epoch,
                    detail="compensation playback failed; local indication engaged",
                )
                self._safe_indicator(zone_id, True)
        else:
            self._amp.stop(zone_id, epoch)
        return restored_key

    # -- completion drain ----------------------------------------------------

    def _handle_completion(self, zone_id: str, epoch: int, key: str) -> None:
        """Timer callback: only acts when the zone is still on that epoch."""

        with self._lock:
            view = self._state.views.get(zone_id)
            if view is None or view.epoch != epoch:
                return  # stale timer: the zone was preempted or rolled back
            if view.current is None or view.current.key != key:
                return
            command_id = self._command_id_for(key, zone_id)
            old_timer = self._timers.pop(zone_id, None)
            if old_timer is not None:
                old_timer.cancel()
            self._amp.stop(zone_id, epoch)
            self._emit(
                ev.EV_PLAYBACK_FINISHED,
                zone=zone_id,
                command_id=command_id,
                key=key,
                priority=view.current.priority,
                epoch=epoch,
                detail="one play-through finished",
            )
            command = self._state.commands.get(command_id) if command_id else None
            if command is not None and zone_id in command.started:
                command.started.remove(zone_id)
            # Drain the queue first: if the message loops or a queued
            # broadcast of the same command takes over, it is not finished.
            self._drain_queue(zone_id)
            if command is not None:
                still_live = bool(command.started or command.queued)
                if not still_live and command.status not in ("rolled_back", "rejected", "duplicate"):
                    self._emit(
                        ev.EV_COMMAND_FINISHED,
                        command_id=command_id,
                        key=key,
                        priority=command.priority,
                    )

    def _drain_queue(self, zone_id: str) -> None:
        """Play the highest priority item once the speaker is free."""

        view = self._state.views.get(zone_id)
        if view is None or view.current is not None:
            return
        if not view.online or not view.queue:
            return
        head = view.queue[0]
        broadcast = head.broadcast
        command_id = self._command_id_for(broadcast.key, zone_id)
        epoch = view.epoch + 1
        self._emit(
            ev.EV_PLAYBACK_STARTING,
            zone=zone_id,
            command_id=command_id,
            key=broadcast.key,
            priority=broadcast.priority,
            epoch=epoch,
            data={**broadcast.as_dict(), "resume_offset": 0.0},
        )
        result = self._amp.play(zone_id, broadcast.key, broadcast.priority, epoch)
        if result.ok:
            self._emit(
                ev.EV_PLAYBACK_STARTED,
                zone=zone_id,
                command_id=command_id,
                key=broadcast.key,
                priority=broadcast.priority,
                epoch=epoch,
                data={"drained": True},
            )
            self._safe_indicator(zone_id, False)
            self._state._queue_remove(view, broadcast.key)
            timer = _Timer(self, zone_id, epoch, broadcast.key)
            self._timers[zone_id] = timer
            timer.schedule(ATTENTION_TONE_SECONDS + broadcast.duration)
        else:
            self._emit(
                ev.EV_PLAYBACK_FAILED,
                zone=zone_id,
                command_id=command_id,
                key=broadcast.key,
                priority=broadcast.priority,
                epoch=epoch,
                detail="queued playback failed; local indication engaged",
            )
            self._safe_indicator(zone_id, True)

    # -- stop / cancel -------------------------------------------------------

    def stop(self, key: str = "", *, zone: str = "") -> dict[str, object]:
        """Manually stop a broadcast by key (or everything in one zone)."""

        with self._lock:
            stopped: list[str] = []
            target_views = [self._state.views[zone]] if zone else list(self._state.views.values())
            for view in target_views:
                if view is None:
                    continue
                if view.current is not None and (not key or view.current.key == key):
                    old_timer = self._timers.pop(view.zone_id, None)
                    if old_timer is not None:
                        old_timer.cancel()
                    epoch = view.epoch + 1
                    self._amp.stop(view.zone_id, epoch)
                    self._emit(
                        ev.EV_PLAYBACK_FINISHED,
                        zone=view.zone_id,
                        command_id=self._command_id_for(view.current.key, view.zone_id),
                        key=view.current.key,
                        priority=view.current.priority,
                        epoch=epoch,
                        detail="stopped by operator",
                    )
                    stopped.append(view.zone_id)
                    self._drain_queue(view.zone_id)
                elif key:
                    if self._state._queue_remove(view, key):
                        self._emit(
                            ev.EV_PLAYBACK_DEQUEUED,
                            zone=view.zone_id,
                            key=key,
                            detail="removed from queue by operator",
                        )
                        stopped.append(view.zone_id)
            return {"stopped": stopped, "key": key}

    # -- health / heartbeat --------------------------------------------------

    def heartbeat(self, zone_id: str) -> bool:
        """Poll one controller; engage local indication when it is dead."""

        with self._lock:
            view = self._state.views.get(zone_id)
            if view is None:
                return False
            alive = self._amp.ping(zone_id)
            if alive and not view.online:
                self._device_recovered(zone_id)
            elif not alive and view.online:
                self._device_lost(zone_id, "heartbeat lost")
            return alive

    def _device_lost(self, zone_id: str, reason: str) -> None:
        view = self._state.views[zone_id]
        old_timer = self._timers.pop(zone_id, None)
        if old_timer is not None:
            old_timer.cancel()
        key = view.current.key if view.current else ""
        command_id = self._command_id_for(key, zone_id) if key else ""
        priority = view.current.priority if view.current else 0
        self._emit(
            ev.EV_DEVICE_DOWN,
            zone=zone_id,
            command_id=command_id,
            key=key,
            priority=priority,
            detail=reason,
        )
        self._emit(
            ev.EV_PLAYBACK_FAILED,
            zone=zone_id,
            command_id=command_id,
            key=key,
            priority=priority,
            detail=f"{reason}; local indication engaged",
        )
        self._safe_indicator(zone_id, True)

    def _device_recovered(self, zone_id: str) -> None:
        self._emit(ev.EV_DEVICE_UP, zone=zone_id, detail="heartbeat restored")
        self._safe_indicator(zone_id, False)
        self._emit(
            ev.EV_INDICATOR_OFF,
            zone=zone_id,
            detail="voice path restored; local indication stood down",
        )
        self._drain_queue(zone_id)

    def _safe_indicator(self, zone_id: str, on: bool) -> None:
        try:
            if on:
                self._indicator.activate(zone_id)
            else:
                self._indicator.deactivate(zone_id)
        except Exception as exc:  # noqa: BLE001 - indicator is the last resort
            self.store.append(
                self.now(),
                ev.EV_INDICATOR_ON if on else ev.EV_INDICATOR_OFF,
                zone=zone_id,
                detail=f"indicator driver error: {exc}",
            )
            return
        self._emit(
            ev.EV_INDICATOR_ON if on else ev.EV_INDICATOR_OFF,
            zone=zone_id,
            detail="playback failed; local evacuation indication engaged"
            if on
            else "voice playback healthy; local indication stood down",
        )

    # -- audit reads ---------------------------------------------------------

    def audit(self, **filters) -> list[dict[str, object]]:
        return [event.as_dict() for event in self.store.query(**filters)]

    def events_after(self, seq: int) -> list[dict[str, object]]:
        return [event.as_dict() for event in self.store.since(seq)]

    def describe(self) -> str:
        counts = self._state.snapshot()["counts"]
        return (
            f"broadcast zones={counts['zones']} online={counts['online']} "
            f"playing={counts['playing']} indicator={counts['indicator']} "
            f"queued={counts['queued']} commands={counts['commands']}"
        )


def make_broadcast(
    key: str,
    *,
    title: str = "",
    priority: object = Priority.NOTICE,
    duration: float = DEFAULT_DURATION,
    loop: bool = False,
    source: str = "",
) -> Broadcast:
    """Convenience constructor that coerces named priority levels."""

    return Broadcast(
        key=key,
        title=title or key,
        priority=Priority.coerce(priority),
        duration=duration,
        loop=loop,
        source=source,
    )
