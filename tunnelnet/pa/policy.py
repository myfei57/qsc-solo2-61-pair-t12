"""Fixed emergency playback policy decisions."""

from __future__ import annotations

IMMEDIATE_CUT = "immediate_cut"
SENTENCE_BOUNDARY_CUT = "sentence_boundary_cut"
PER_ZONE_FALLBACK = "per_zone"
BROADCAST_GROUP_FALLBACK = "broadcast_group"

# Life-safety announcements cut immediately. A short attention tone is emitted
# by the field controller before speech to reduce the intelligibility penalty.
PREEMPTION_POLICY = IMMEDIATE_CUT

# Only the zone whose controller failed enters hard-wired local indication.
FALLBACK_POLICY = PER_ZONE_FALLBACK
