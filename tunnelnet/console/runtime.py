"""Wiring of every control component behind the console."""

from __future__ import annotations

from tunnelnet.access import AccessService
from tunnelnet.audit import AuditRegistry, MarkSet
from tunnelnet.broadcast import (
    BroadcastService,
    EventStore,
    HeartbeatMonitor,
    SimAmplifier,
    SimIndicator,
    Zone,
)
from tunnelnet.cabin import PartitionTable, PowerBook, Registry as CabinRegistry, SuppressionLog
from tunnelnet.cable import CableRegistry, MovingAverage
from tunnelnet.drain import DrainService, PumpUnit
from tunnelnet.env import Environment
from tunnelnet.fire import FireService
from tunnelnet.ns import Registry as NamespaceRegistry
from tunnelnet.patrol import PatrolService
from tunnelnet.quota import QuotaService
from tunnelnet.store import Store
from tunnelnet.vent import VentService

DEFAULT_ZONES = ("zone-1", "zone-2", "zone-3")
DEFAULT_THRESHOLD = 75.0
DEFAULT_BROADCAST_ZONES = (
    ("zone-1", "防火分区一"),
    ("zone-2", "防火分区二"),
    ("zone-3", "防火分区三"),
)


class Runtime:
    """Owns one instance of each control component for a single store."""

    def __init__(self, store: Store) -> None:
        self.store = store
        self.namespaces = NamespaceRegistry()
        self.cabins = CabinRegistry()
        self.partitions = PartitionTable()
        self.filter = MovingAverage(3)
        self.cables = CableRegistry()
        self.env = Environment(self.filter, self.partitions, DEFAULT_THRESHOLD)
        self.access = AccessService(store, self.cabins)
        self.pump = PumpUnit()
        self.drain = DrainService(self.cabins, self.pump, store)
        self.ventilation = VentService()
        self.suppression = SuppressionLog()
        self.dedupe = MarkSet()
        self.fire = FireService(store, self.ventilation, self.suppression, self.dedupe, DEFAULT_ZONES)
        self.patrol = PatrolService(self.cabins)
        self.power_book = PowerBook()
        self.quota = QuotaService(self.cabins, self.power_book)
        self.audit = AuditRegistry()
        self.broadcast_events = EventStore()
        self.amplifier = SimAmplifier()
        self.indicator = SimIndicator()
        self.broadcast = BroadcastService(self.amplifier, self.indicator, self.broadcast_events)
        for zone_id, name in DEFAULT_BROADCAST_ZONES:
            self.broadcast.register_zone(Zone(id=zone_id, name=name))
        # The console audit stream and the playback state share one event log.
        self.broadcast_events.add_sink(self._mirror_broadcast_event)
        self.heartbeat = HeartbeatMonitor(
            ping=self.broadcast.heartbeat,
            zones=lambda: [zone.id for zone in self.broadcast.zones()],
        )

    def _mirror_broadcast_event(self, event) -> None:  # pragma: no cover - thin glue
        from tunnelnet.audit import Record
        import uuid

        self.audit.append(
            Record(id=uuid.uuid4().hex, event=f"broadcast_{event.event}", at=event.at)
        )

    def close(self) -> None:
        """Stop background monitors owned by this runtime."""

        self.heartbeat.stop()

