from __future__ import annotations

import random
import threading
import time


class SimpleRateLimiter:
    def __init__(self, qps: float, jitter_ratio: float = 0.1) -> None:
        self.interval = 1.0 / qps if qps > 0 else 0.0
        self.jitter_ratio = jitter_ratio
        self._last_call = 0.0
        # Lock makes a single limiter usable from multiple worker threads
        # so a thread-pooled stage still honours the global qps cap.
        self._lock = threading.Lock()

    def wait(self) -> None:
        if self.interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            target = self._last_call + self.interval
            sleep_for = max(0.0, target - now)
            if sleep_for:
                sleep_for += random.uniform(0.0, self.interval * self.jitter_ratio)
                time.sleep(sleep_for)
            self._last_call = time.monotonic()