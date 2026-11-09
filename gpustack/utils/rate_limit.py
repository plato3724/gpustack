"""Trailing-window counters used by billing quota rules.

The deque itself is process-local and is not the durable copy. The quota
engine snapshots samples into SQL and imports them into a new counter
after a restart. Token usage is recorded after completion, so in-flight
requests can still exceed a cap.

``now`` defaults to ``time.monotonic``, which cannot be compared across
processes. The quota engine injects ``time.time`` so a stored stamp is a
unix timestamp and a restarted process can keep sliding the same window.
A monotonic stamp from before the restart would be a meaningless offset
into the new process's clock.
"""

import math
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, List, Optional, Tuple


@dataclass
class WindowDecision:
    allowed: bool
    total: int
    retry_after: int = 0


class SlidingWindowCounter:
    def __init__(self, now: Optional[Callable[[], float]] = None) -> None:
        self._lock = threading.Lock()
        self._events: Dict[str, Deque[Tuple[float, int]]] = defaultdict(deque)
        self._windows: Dict[str, int] = {}
        # Look up the default at call time so a test that patches
        # time.monotonic before construction still drives the clock.
        self._now = now if now is not None else time.monotonic

    def now(self) -> float:
        """Current instant in this counter's clock."""
        return self._now()

    def _read(self, key: str, window_seconds: int, now: float):
        events = self._events[key]
        self._windows[key] = window_seconds
        while events and events[0][0] <= now - window_seconds:
            events.popleft()
        return events, sum(amount for _, amount in events)

    def _append(self, events, now, amount):
        if amount <= 0:
            return
        # Coalesce within a second to bound memory for long budget windows.
        # The kept stamp is the latest event in that second, which is also
        # the stamp written to SQL (bucket = int(stamp)).
        if events and int(events[-1][0]) == int(now):
            _, previous = events.pop()
            amount += previous
        events.append((now, amount))

    def take_many(self, checks: List[Tuple[str, int, int, int]]) -> WindowDecision:
        """Admit all applicable rules atomically; a denial consumes no quota."""
        now = self._now()
        with self._lock:
            pending = []
            denial = None
            for key, amount, window, limit in checks:
                events, total = self._read(key, window, now)
                if total + amount > limit or (amount == 0 and total >= limit):
                    remaining = total
                    retry = 1
                    for stamp, value in events:
                        remaining -= value
                        retry = max(1, math.ceil(stamp + window - now))
                        if remaining + amount < limit or (
                            amount > 0 and remaining + amount <= limit
                        ):
                            break
                    if denial is None or retry > denial.retry_after:
                        denial = WindowDecision(False, total, retry)
                pending.append((events, amount, total))
            if denial:
                return denial
            for events, amount, _ in pending:
                self._append(events, now, amount)
            return WindowDecision(True, max((t + a for _, a, t in pending), default=0))

    def take(
        self, key: str, amount: int, window_seconds: int, limit: int
    ) -> WindowDecision:
        return self.take_many([(key, amount, window_seconds, limit)])

    def add(self, key: str, amount: int, window_seconds: int) -> int:
        """Record actual consumption, including usage above the limit."""
        now = self._now()
        with self._lock:
            events, total = self._read(key, window_seconds, now)
            self._append(events, now, amount)
            return total + max(0, amount)

    def total(self, key: str, window_seconds: int) -> int:
        with self._lock:
            return self._read(key, window_seconds, self._now())[1]

    def window_of(self, key: str) -> int:
        with self._lock:
            return int(self._windows.get(key, 0))

    def export_samples(self, key: str) -> List[Tuple[float, int]]:
        """Copy the current deque. The caller prunes before exporting.

        Persistence compares this copy to the last successful SQL write
        and upserts only the seconds that changed.
        """
        with self._lock:
            events = self._events.get(key)
            if not events:
                return []
            return [(stamp, amount) for stamp, amount in events]

    def import_samples(
        self, key: str, samples: List[Tuple[float, int]], window_seconds: int
    ) -> None:
        """Replace one counter with restored ``(timestamp, amount)`` samples.

        Timestamps must already be in this counter's clock. The quota
        engine stores unix time and imports into a wall-clock counter, so
        the next read expires samples with the same ``now - window`` test
        the previous process used. Samples are sorted because Retry-After
        walks the deque from oldest to newest.
        """
        restored: Deque[Tuple[float, int]] = deque(
            sorted(
                (float(stamp), int(amount)) for stamp, amount in samples if amount > 0
            )
        )
        with self._lock:
            self._events[key] = restored
            self._windows[key] = window_seconds

    def reset(self, key: str) -> None:
        with self._lock:
            self._events.pop(key, None)
            self._windows.pop(key, None)

    def pop_prefix(self, prefix: str) -> None:
        """Drop every counter whose key starts with ``prefix``."""
        with self._lock:
            for key in [k for k in self._events if k.startswith(prefix)]:
                self._events.pop(key, None)
            for key in [k for k in self._windows if k.startswith(prefix)]:
                self._windows.pop(key, None)
