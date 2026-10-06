"""Behavioural tests for zoned emergency public address control."""

from __future__ import annotations

import unittest

from tunnelnet.pa import (
    FIRE_PRIORITY,
    INFORMATION_PRIORITY,
    BroadcastConflict,
    BroadcastService,
    LocalIndicatorBank,
    LocalPlaybackController,
    Zone,
    ZoneTable,
)


class PublicAddressCase(unittest.TestCase):
    def setUp(self) -> None:
        self.table = ZoneTable()
        self.table.add(Zone("zone-1", "first", "ctrl-1"))
        self.table.add(Zone("zone-2", "second", "ctrl-2"))
        self.table.add(Zone("zone-3", "third", "ctrl-3"))
        self.controller = LocalPlaybackController()
        self.indicators = LocalIndicatorBank()
        self.pa = BroadcastService(
            self.table, self.controller, self.indicators, clock=lambda: "t1"
        )

    def test_zone_table_and_shared_state_projection(self) -> None:
        result = self.pa.trigger("b1", "evacuate", ["zone-1"], FIRE_PRIORITY)
        self.assertEqual(result["status"], "playing")
        state = self.pa.state()
        zone = next(item for item in state.zones if item.zone == "zone-1")
        self.assertEqual(zone.current.id, "b1")
        self.assertEqual(zone.revision, state.revision)
        self.assertEqual(state.broadcasts["b1"].content, "evacuate")
        self.assertEqual(state.audit[-1].broadcast_id, "b1")
        self.assertEqual(state.audit[-1].revision, zone.revision)
        self.assertEqual(self.pa.broadcast_status("b1")["zones"]["zone-1"], "playing")

    def test_fire_immediately_preempts_lower_priority_content(self) -> None:
        self.pa.trigger("info", "routine notice", ["zone-1"], INFORMATION_PRIORITY)
        first_token = self.pa.zone("zone-1").revision

        result = self.pa.trigger("fire", "leave now", ["zone-1"], FIRE_PRIORITY)

        self.assertEqual(result["preemption"], "immediate_cut")
        zone = self.pa.zone("zone-1")
        self.assertEqual(zone.state, "playing")
        self.assertEqual(zone.current.id, "fire")
        self.assertEqual(zone.interrupted.id, "info")
        self.assertEqual([item.id for item in zone.queued], ["info"])
        self.assertGreater(zone.revision, first_token)
        self.assertEqual(self.controller.commands[-1][:3], ("zone-1", zone.session_token, "fire"))
        events = [entry.event for entry in self.pa.audit_trail()]
        self.assertIn("broadcast_preempted", events)

    def test_lower_priority_broadcast_waits(self) -> None:
        self.pa.trigger("fire", "leave now", ["zone-1"], FIRE_PRIORITY)
        result = self.pa.trigger("info", "later", ["zone-1"], INFORMATION_PRIORITY)
        self.assertEqual(result["status"], "queued")
        zone = self.pa.zone("zone-1")
        self.assertEqual(zone.current.id, "fire")
        self.assertEqual([item.id for item in zone.queued], ["info"])

    def test_completion_resumes_preempted_content(self) -> None:
        self.pa.trigger("info", "later", ["zone-1"], INFORMATION_PRIORITY)
        self.pa.trigger("fire", "leave now", ["zone-1"], FIRE_PRIORITY)
        result = self.pa.complete("fire", "zone-1")
        self.assertEqual(result["status"], "playing")
        zone = self.pa.zone("zone-1")
        self.assertEqual(zone.current.id, "info")
        self.assertIsNone(zone.interrupted)
        self.assertEqual(zone.queued, [])

    def test_trigger_is_idempotent_by_id_and_content_hash(self) -> None:
        first = self.pa.trigger(
            "fire-1", "leave now", ["zone-1", "zone-2"], FIRE_PRIORITY, content_hash="hash-1"
        )
        repeated = self.pa.trigger(
            "fire-1", "leave now", ["zone-2", "zone-1"], FIRE_PRIORITY, content_hash="hash-1"
        )
        hash_repeat = self.pa.trigger(
            "fire-retry", "leave now", ["zone-1", "zone-2"], FIRE_PRIORITY, content_hash="hash-1"
        )
        self.assertFalse(first["idempotent"])
        self.assertTrue(repeated["idempotent"])
        self.assertTrue(hash_repeat["idempotent"])
        self.assertEqual(len(self.controller.commands), 2)
        self.assertEqual(len(self.pa.audit_trail()), 3)  # one playing event per zone

    def test_reused_id_with_changed_content_is_conflict(self) -> None:
        self.pa.trigger("b", "first", ["zone-1"], FIRE_PRIORITY)
        with self.assertRaises(BroadcastConflict):
            self.pa.trigger("b", "second", ["zone-1"], FIRE_PRIORITY)

    def test_playback_failure_falls_back_only_failed_zone(self) -> None:
        result = self.pa.trigger("info", "notice", ["zone-1", "zone-2"], INFORMATION_PRIORITY)
        self.assertEqual(result["status"], "playing")

        self.controller.set_offline("zone-2")
        self.pa.heartbeat("zone-2", False)
        result = self.pa.trigger("fire", "leave now", ["zone-1", "zone-2"], FIRE_PRIORITY)

        self.assertEqual(result["status"], "partial_fallback")
        self.assertEqual(result["fallback_scope"], "per_zone")
        self.assertEqual(result["zones"], {"zone-1": "playing", "zone-2": "fallback"})
        self.assertEqual(self.pa.fallback_zones(), ["zone-2"])
        self.assertTrue(self.indicators.active("zone-2"))
        self.assertFalse(self.indicators.active("zone-1"))
        self.assertEqual(self.pa.zone("zone-1").current.id, "fire")
        self.assertEqual(self.pa.zone("zone-2").state, "fallback")

    def test_preemption_failure_rolls_back_that_zone_to_fallback(self) -> None:
        self.pa.trigger("info", "routine", ["zone-1"], INFORMATION_PRIORITY)
        self.controller.set_play_failure("zone-1")
        result = self.pa.trigger("fire", "evacuate", ["zone-1"], FIRE_PRIORITY)
        self.assertEqual(result["zones"]["zone-1"], "fallback")
        zone = self.pa.zone("zone-1")
        self.assertTrue(zone.fallback)
        self.assertIsNone(zone.current)
        self.assertEqual(zone.queued, [])
        self.assertTrue(self.indicators.active("zone-1"))
        events = [entry.event for entry in self.pa.audit_trail()]
        self.assertIn("preemption_failed", events)

    def test_failed_zone_does_not_roll_back_successful_zones_or_their_queues(self) -> None:
        self.pa.trigger("info-a", "first notice", ["zone-1", "zone-2"], INFORMATION_PRIORITY)
        self.pa.trigger("info-b", "second notice", ["zone-1", "zone-2"], INFORMATION_PRIORITY)
        self.controller.set_play_failure("zone-2")
        result = self.pa.trigger("fire", "leave now", ["zone-1", "zone-2"], FIRE_PRIORITY)

        self.assertEqual(result["zones"]["zone-1"], "playing")
        self.assertEqual(result["zones"]["zone-2"], "fallback")
        zone_one = self.pa.zone("zone-1")
        zone_two = self.pa.zone("zone-2")
        self.assertEqual(zone_one.current.id, "fire")
        self.assertEqual([item.id for item in zone_one.queued], ["info-a", "info-b"])
        self.assertIsNone(zone_two.current)
        self.assertEqual(
            [item.id for item in zone_two.queued],
            ["info-b"],
        )
        self.assertTrue(self.indicators.active("zone-2"))
        self.assertFalse(self.indicators.active("zone-1"))

    def test_heartbeat_loss_fails_safe_and_recovery_is_acknowledged(self) -> None:
        self.pa.trigger("info", "routine", ["zone-1"], INFORMATION_PRIORITY)
        self.pa.heartbeat("zone-1", False)
        zone = self.pa.zone("zone-1")
        self.assertTrue(zone.fallback)
        self.assertTrue(self.indicators.active("zone-1"))
        with self.assertRaises(RuntimeError):
            self.pa.reset_fallback("zone-1")
        self.pa.heartbeat("zone-1", True)
        result = self.pa.reset_fallback("zone-1")
        self.assertFalse(result["fallback"])
        self.assertFalse(self.indicators.active("zone-1"))

    def test_stop_is_idempotent_and_advances_queue(self) -> None:
        self.pa.trigger("fire", "evacuate", ["zone-1"], FIRE_PRIORITY)
        self.pa.trigger("info", "later", ["zone-1"], INFORMATION_PRIORITY)
        first = self.pa.stop("fire")
        second = self.pa.stop("fire")
        self.assertEqual(first["zones"]["zone-1"], "playing")
        self.assertEqual(second["zones"]["zone-1"], "stopped")
        self.assertEqual(self.pa.zone("zone-1").current.id, "info")

    def test_unknown_zone_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.pa.trigger("x", "content", ["missing"], FIRE_PRIORITY)


if __name__ == "__main__":
    unittest.main()
