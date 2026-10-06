"""Priority queue, immediate preemption and per-zone fallback service."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from tunnelnet.drain import utc_now

from .hardware import IndicatorBank, PlaybackController
from .model import (
    FIRE_PRIORITY,
    AuditEntry,
    Broadcast,
    PlaybackState,
    Zone,
    ZonePlayback,
)
from .policy import IMMEDIATE_CUT, PER_ZONE_FALLBACK
from .table import ZoneTable


class BroadcastConflict(ValueError):
    """An idempotency key is already used by a different broadcast."""


class BroadcastService:
    """Coordinate one shared playback state across all zones.

    Emergency classes use immediate cut rather than sentence-boundary cut. The
    trade-off is a brief loss of the interrupted sentence in exchange for no
    delay in life-safety instructions; the field device plays a short attention
    tone before speech. Playback failure is contained to the failed zone. That
    zone rolls back the preemption to its safe local-indicator state, while
    successful zones in the same announcement keep playing.
"""

    def __init__(
        self,
        table: ZoneTable | None = None,
        controller: PlaybackController | None = None,
        indicators: IndicatorBank | None = None,
        clock: Callable[[], str] = utc_now,
    ) -> None:
        self.table = table if table is not None else ZoneTable()
        self._controller: Any = controller
        self._indicators: Any = indicators
        self._clock = clock
        self._lock = threading.RLock()
        self._revision = 0
        self._seq = 0
        self._audit_seq = 0
        self._zones: dict[str, ZonePlayback] = {}
        self._broadcasts: dict[str, Broadcast] = {}
        self._aliases: dict[str, str] = {}
        self._results: dict[str, dict[str, str]] = {}
        self._audit: list[AuditEntry] = []
        for zone in self.table.list():
            self._zones[zone.id] = ZonePlayback(zone=zone.id)

    def add_zone(self, zone_id: str, name: str = "", controller_id: str = "") -> None:
        self.table.add(
            Zone(id=zone_id, name=name or zone_id, controller_id=controller_id or zone_id)
        )
        with self._lock:
            self._zones.setdefault(zone_id, ZonePlayback(zone=zone_id))

    def trigger(
        self,
        broadcast_id: str,
        content: str,
        zones: tuple[str, ...] | list[str],
        priority: int = FIRE_PRIORITY,
        source: str = "operator",
        content_hash: str = "",
    ) -> dict[str, object]:
        """Queue or immediately play an announcement in each target zone.

        Repeating the same identifier or content hash is idempotent. The first
        request supplies the canonical zones and content; a conflicting reuse
        is rejected instead of issuing a second hardware command.
        """

        target_zones = self._validated_zones(tuple(zones))
        if not broadcast_id:
            raise ValueError("broadcast id is required")
        if not content:
            raise ValueError("content is required")
        if priority <= 0:
            raise ValueError("priority must be positive")
        with self._lock:
            existing = self._lookup(broadcast_id)
            if existing is None and content_hash:
                existing = self._lookup(content_hash)
            if existing is not None:
                if (
                    existing.content != content
                    or existing.priority != priority
                    or set(existing.zones) != set(target_zones)
                ):
                    raise BroadcastConflict(f"broadcast {broadcast_id} already exists")
                current_results = {
                    zone_id: self._zone_broadcast_status(existing, zone_id)
                    for zone_id in existing.zones
                }
                return self._trigger_result(existing, idempotent=True, results=current_results)

            self._seq += 1
            broadcast = Broadcast(
                id=broadcast_id,
                content=content,
                zones=target_zones,
                priority=priority,
                source=source,
                content_hash=content_hash,
                queued_at=self._clock(),
                seq=self._seq,
            )
            self._broadcasts[broadcast_id] = broadcast
            if content_hash and content_hash != broadcast_id:
                self._aliases[content_hash] = broadcast_id
            results: dict[str, str] = {}
            for zone_id in target_zones:
                results[zone_id] = self._trigger_zone_for(broadcast, zone_id)
            self._results[broadcast_id] = results
            self._record("broadcast_triggered", broadcast=broadcast, detail=IMMEDIATE_CUT)
            return self._trigger_result(broadcast, idempotent=False, results=results)

    def stop(
        self,
        broadcast_id: str,
        zones: tuple[str, ...] | list[str] | None = None,
    ) -> dict[str, object]:
        """Stop and remove one broadcast; failed commands activate indicators."""

        with self._lock:
            broadcast = self._require_broadcast(broadcast_id)
            target_zones = self._validated_zones(tuple(zones or broadcast.zones))
            results: dict[str, str] = {}
            for zone_id in target_zones:
                results[zone_id] = self._stop_zone(broadcast, zone_id)
            self._results[broadcast_id] = results
            return self._trigger_result(broadcast, idempotent=False, results=results, stopped=True)

    def complete(self, broadcast_id: str, zone_id: str) -> dict[str, str]:
        """Mark one zone output finished and promote its queued broadcast."""

        with self._lock:
            broadcast = self._require_broadcast(broadcast_id)
            self._require_zone(zone_id)
            zone = self._zones[zone_id]
            if zone.current is not broadcast and (zone.current is None or zone.current.id != broadcast_id):
                raise ValueError(f"broadcast {broadcast_id} is not playing in {zone_id}")
            if not zone.controller_online:
                self._activate_fallback(zone, "controller offline at completion")
                result = "fallback"
            else:
                zone.current = None
                zone.state = "idle"
                zone.last_error = ""
                zone.revision = self._bump()
                advanced = self._advance(zone)
                result = "playing" if advanced == "playing" else "stopped"
                self._record("broadcast_completed", broadcast=broadcast, zone_id=zone_id)
            self._results.setdefault(broadcast_id, {})[zone_id] = result
            return {"zone": zone_id, "status": result}

    def heartbeat(self, zone_id: str, online: bool) -> dict[str, object]:
        """Update controller liveness and fail safe when it disappears."""

        with self._lock:
            zone = self._zones[self._require_zone(zone_id)]
            previous = zone.controller_online
            zone.controller_online = online
            zone.revision = self._bump()
            if previous and not online:
                current = zone.current
                self._activate_fallback(zone, "controller heartbeat lost")
                if current is not None:
                    self._results.setdefault(current.id, {})[zone_id] = "fallback"
                self._record("controller_offline", zone_id=zone_id, detail=zone.last_error)
            elif online and not previous:
                self._record("controller_online", zone_id=zone_id)
            return {"zone": zone_id, "online": online, "fallback": zone.fallback}

    def reset_fallback(self, zone_id: str) -> dict[str, object]:
        """Manually acknowledge local fallback after the zone is repaired."""

        with self._lock:
            zone = self._zones[self._require_zone(zone_id)]
            if not zone.controller_online:
                raise RuntimeError(f"controller in {zone_id} is still offline")
            if self._indicators is not None:
                self._indicators.clear(zone_id)
            zone.fallback = False
            zone.last_error = ""
            zone.revision = self._bump()
            self._record("fallback_cleared", zone_id=zone_id)
            if zone.state in {"fallback", "idle"}:
                zone.state = "idle" if not zone.queued else "queued"
                zone.revision = self._bump()
                if not zone.queued:
                    result = "idle"
                else:
                    result = self._advance(zone)
            else:
                result = zone.state
            return {"zone": zone_id, "status": result, "fallback": zone.fallback}

    def state(self) -> PlaybackState:
        with self._lock:
            return PlaybackState(
                revision=self._revision,
                zones=tuple(self._copy_zone(self._zones[key]) for key in sorted(self._zones)),
                zone_table=tuple(self.table.list()),
                broadcasts=dict(self._broadcasts),
                audit=tuple(self._audit),
                zone_results={key: dict(value) for key, value in self._results.items()},
            )

    def zone(self, zone_id: str) -> ZonePlayback:
        with self._lock:
            return self._copy_zone(self._zones[self._require_zone(zone_id)])

    def broadcast(self, broadcast_id: str) -> Broadcast:
        with self._lock:
            return self._require_broadcast(broadcast_id)

    def broadcast_status(self, broadcast_id: str) -> dict[str, object]:
        with self._lock:
            broadcast = self._require_broadcast(broadcast_id)
            return {
                "broadcast": broadcast.as_dict(),
                "zones": {zone_id: self._zone_broadcast_status(broadcast, zone_id) for zone_id in broadcast.zones},
                "status": self._aggregate_status(
                    {zone_id: self._zone_broadcast_status(broadcast, zone_id) for zone_id in broadcast.zones}
                ),
            }

    def audit_trail(self) -> list[AuditEntry]:
        with self._lock:
            return list(self._audit)

    def audit_count(self) -> int:
        with self._lock:
            return len(self._audit)

    def fallback_zones(self) -> list[str]:
        with self._lock:
            return [key for key in sorted(self._zones) if self._zones[key].fallback]

    def describe(self) -> str:
        state = self.state()
        return (
            f"pa zones={len(state.zones)} broadcasts={len(state.broadcasts)} "
            f"fallback={len(self.fallback_zones())} revision={state.revision}"
        )

    def _trigger_zone_for(self, broadcast: Broadcast, zone_id: str) -> str:
        zone = self._zones[zone_id]
        if not zone.controller_online:
            self._activate_fallback(zone, "controller offline")
            self._record("playback_failed", broadcast=broadcast, zone_id=zone_id, detail=zone.last_error)
            return "fallback"
        if zone.fallback:
            self._record("fallback_blocked", broadcast=broadcast, zone_id=zone_id)
            return "fallback"

        current = zone.current
        if current is None:
            return self._start(zone, broadcast)
        if current.id == broadcast.id:
            return "playing"
        if broadcast.priority >= current.priority:
            self._insert(zone.queued, broadcast)
            zone.state = "queued"
            zone.revision = self._bump()
            self._results.setdefault(broadcast.id, {})[zone_id] = "queued"
            self._record("broadcast_queued", broadcast=broadcast, zone_id=zone_id)
            return "queued"

        zone.interrupted = current
        return self._preempt(zone, broadcast, current)

    def _start(self, zone: ZonePlayback, broadcast: Broadcast) -> str:
        token = self._bump()
        zone.session_token = token
        zone.revision = token
        try:
            if self._controller is not None:
                self._controller.play(zone.zone, token, broadcast.id, broadcast.content)
        except Exception as exc:  # fail safe on any field-device failure
            self._activate_fallback(zone, str(exc))
            self._record("playback_failed", broadcast=broadcast, zone_id=zone.zone, detail=str(exc))
            return "fallback"
        zone.current = broadcast
        zone.interrupted = None
        zone.state = "playing"
        zone.fallback = False
        zone.last_error = ""
        self._results.setdefault(broadcast.id, {})[zone.zone] = "playing"
        self._record("broadcast_playing", broadcast=broadcast, zone_id=zone.zone)
        return "playing"

    def _preempt(
        self,
        zone: ZonePlayback,
        broadcast: Broadcast,
        current: Broadcast,
    ) -> str:
        token = self._bump()
        zone.session_token = token
        zone.revision = token
        try:
            if self._controller is not None:
                # play() is the hardware cut point; emergency policy is immediate.
                self._controller.play(zone.zone, token, broadcast.id, broadcast.content)
        except Exception as exc:
            preempted = current
            zone.interrupted = None
            self._activate_fallback(zone, str(exc))
            self._results.setdefault(broadcast.id, {})[zone.zone] = "fallback"
            self._results.setdefault(preempted.id, {})[zone.zone] = "fallback"
            self._record("preemption_rolled_back", broadcast=preempted, zone_id=zone.zone, detail=str(exc))
            self._record("preemption_failed", broadcast=broadcast, zone_id=zone.zone, detail=str(exc))
            self._record("playback_failed", broadcast=broadcast, zone_id=zone.zone, detail=str(exc))
            return "fallback"
        zone.current = broadcast
        zone.state = "playing"
        self._insert(zone.queued, current)
        self._results.setdefault(broadcast.id, {})[zone.zone] = "playing"
        self._results.setdefault(current.id, {})[zone.zone] = "queued"
        self._record(
            "broadcast_preempted",
            broadcast=current,
            zone_id=zone.zone,
            detail=f"preempted_by={broadcast.id}",
        )
        self._record("broadcast_playing", broadcast=broadcast, zone_id=zone.zone, detail=IMMEDIATE_CUT)
        return "playing"

    def _stop_zone(self, broadcast: Broadcast, zone_id: str) -> str:
        zone = self._zones[zone_id]
        found = False
        if zone.current is not None and zone.current.id == broadcast.id:
            found = True
            if not zone.controller_online:
                self._activate_fallback(zone, "controller offline during stop")
                result = "fallback"
            else:
                try:
                    if self._controller is not None:
                        self._controller.stop(zone_id, zone.session_token)
                except Exception as exc:
                    self._activate_fallback(zone, str(exc))
                    self._record("stop_failed", broadcast=broadcast, zone_id=zone_id, detail=str(exc))
                    result = "fallback"
                else:
                    zone.current = None
                    zone.state = "idle"
                    zone.revision = self._bump()
                    result = "stopped"
        else:
            before = len(zone.queued)
            zone.queued = [item for item in zone.queued if item.id != broadcast.id]
            if zone.interrupted is not None and zone.interrupted.id == broadcast.id:
                zone.interrupted = None
            found = len(zone.queued) != before or found
            result = "stopped"
            zone.revision = self._bump()
        if found:
            self._results.setdefault(broadcast.id, {})[zone_id] = result
            self._record("broadcast_stopped", broadcast=broadcast, zone_id=zone_id, detail=result)
            if result != "fallback" and zone.current is None:
                advanced = self._advance(zone)
                if advanced == "playing":
                    result = "playing"
                    self._results[broadcast.id][zone_id] = result
        else:
            result = "stopped"
        return result

    def _advance(self, zone: ZonePlayback) -> str:
        if zone.fallback or not zone.controller_online or not zone.queued:
            zone.state = "fallback" if zone.fallback else "idle"
            zone.revision = self._bump()
            return zone.state
        broadcast = zone.queued.pop(0)
        if zone.interrupted is not None and zone.interrupted.id == broadcast.id:
            zone.interrupted = None
        result = self._start(zone, broadcast)
        if result == "playing":
            self._record("broadcast_resumed", broadcast=broadcast, zone_id=zone.zone)
        return result

    def _activate_fallback(self, zone: ZonePlayback, error: str) -> None:
        if self._indicators is not None:
            self._indicators.activate(zone.zone)
        zone.fallback = True
        zone.state = "fallback"
        zone.current = None
        zone.last_error = error
        zone.session_token = 0
        zone.revision = self._bump()
        self._record("local_fallback_activated", zone_id=zone.zone, detail=error)

    def _insert(self, queue: list[Broadcast], broadcast: Broadcast) -> None:
        for index, item in enumerate(queue):
            if (broadcast.priority, broadcast.seq) < (item.priority, item.seq):
                queue.insert(index, broadcast)
                return
        queue.append(broadcast)

    def _trigger_result(
        self,
        broadcast: Broadcast,
        idempotent: bool,
        results: dict[str, str] | None = None,
        stopped: bool = False,
    ) -> dict[str, object]:
        actual = results if results is not None else self._results.get(broadcast.id, {})
        return {
            "broadcast": broadcast.as_dict(),
            "idempotent": idempotent,
            "zones": {zone_id: actual.get(zone_id, "unknown") for zone_id in broadcast.zones},
            "status": self._aggregate_status(actual) if actual else "idempotent",
            "preemption": IMMEDIATE_CUT,
            "fallback_scope": PER_ZONE_FALLBACK,
            "stopped": stopped,
        }

    def _zone_broadcast_status(self, broadcast: Broadcast, zone_id: str) -> str:
        zone = self._zones[zone_id]
        if zone.fallback and self._results.get(broadcast.id, {}).get(zone_id) == "fallback":
            return "fallback"
        if zone.current is not None and zone.current.id == broadcast.id:
            return "playing"
        if any(item.id == broadcast.id for item in zone.queued):
            return "queued"
        if zone.interrupted is not None and zone.interrupted.id == broadcast.id:
            return "interrupted"
        return self._results.get(broadcast.id, {}).get(zone_id, "stopped")

    def _aggregate_status(self, results: dict[str, str]) -> str:
        statuses = set(results.values())
        if not statuses:
            return "unknown"
        if statuses == {"playing"}:
            return "playing"
        if statuses == {"queued"}:
            return "queued"
        if statuses == {"fallback"}:
            return "fallback"
        if "fallback" in statuses:
            return "partial_fallback"
        if "playing" in statuses:
            return "partial_playing"
        return sorted(statuses)[0]

    def _validated_zones(self, zones: tuple[str, ...]) -> tuple[str, ...]:
        if not zones:
            raise ValueError("zones are required")
        known = {zone.id for zone in self.table.list()}
        for zone_id in zones:
            if not zone_id:
                raise ValueError("zone id is required")
            if known and zone_id not in known:
                raise ValueError(f"zone {zone_id} is not in the zone table")
        return tuple(dict.fromkeys(zones))

    def _require_zone(self, zone_id: str) -> str:
        if zone_id not in self._zones:
            raise ValueError(f"zone {zone_id} not found")
        return zone_id

    def _require_broadcast(self, broadcast_id: str) -> Broadcast:
        broadcast = self._lookup(broadcast_id)
        if broadcast is None:
            raise ValueError(f"broadcast {broadcast_id} not found")
        return broadcast

    def _lookup(self, broadcast_id: str) -> Broadcast | None:
        canonical = self._aliases.get(broadcast_id, broadcast_id)
        return self._broadcasts.get(canonical)

    def _copy_zone(self, zone: ZonePlayback) -> ZonePlayback:
        return ZonePlayback(
            zone=zone.zone,
            state=zone.state,
            current=zone.current,
            queued=list(zone.queued),
            interrupted=zone.interrupted,
            controller_online=zone.controller_online,
            fallback=zone.fallback,
            last_error=zone.last_error,
            revision=zone.revision,
            session_token=zone.session_token,
        )

    def _bump(self) -> int:
        self._revision += 1
        return self._revision

    def _record(
        self,
        event: str,
        broadcast: Broadcast | None = None,
        zone_id: str = "",
        detail: str = "",
    ) -> AuditEntry:
        self._audit_seq += 1
        entry = AuditEntry(
            id=self._audit_seq,
            event=event,
            at=self._clock(),
            broadcast_id="" if broadcast is None else broadcast.id,
            zone=zone_id,
            revision=self._revision,
            detail=detail,
            priority=0 if broadcast is None else broadcast.priority,
        )
        self._audit.append(entry)
        return entry
