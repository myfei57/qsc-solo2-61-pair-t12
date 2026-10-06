"""Background heartbeat that notices a dead playback controller."""

from __future__ import annotations

import threading
from typing import Callable


class HeartbeatMonitor:
    """Polls every registered zone on a fixed interval.

    The detection requirement ("播控坏了也没人知道") is covered by pinging
    each amplifier: a missed ping marks the zone offline, stops any running
    completion timer and engages the local evacuation indication.
    """

    def __init__(
        self,
        ping: Callable[[str], bool],
        zones: Callable[[], list[str]],
        interval: float = 2.0,
    ) -> None:
        self._ping = ping
        self._zones = zones
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="broadcast-heartbeat", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 1.0)
            self._thread = None

    def run_once(self) -> None:
        for zone_id in self._zones():
            self._ping(zone_id)

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self.run_once()
            except Exception:  # noqa: BLE001 - monitoring must never die silently
                pass
