"""Process-local trailing-window counters shared by billing quota rules.

Counters reset on restart and are independent on each server process. Token
usage is recorded after completion, so in-flight requests can exceed a cap.
"""

import math
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Tuple


@dataclass
class WindowDecision:
    allowed: bool
    total: int
    retry_after: int = 0


class SlidingWindowCounter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: Dict[str, Deque[Tuple[float, int]]] = defaultdict(deque)
        self._windows: Dict[str, int] = {}

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
        if events and int(events[-1][0]) == int(now):
            _, previous = events.pop()
            amount += previous
        events.append((now, amount))

    def take_many(self, checks: List[Tuple[str, int, int, int]]) -> WindowDecision:
        """Admit all applicable rules atomically; a denial consumes no quota."""
        now = time.monotonic()
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
        now = time.monotonic()
        with self._lock:
            events, total = self._read(key, window_seconds, now)
            self._append(events, now, amount)
            return total + max(0, amount)

    def total(self, key: str, window_seconds: int) -> int:
        with self._lock:
            return self._read(key, window_seconds, time.monotonic())[1]

    def reset(self, key: str) -> None:
        with self._lock:
            self._events.pop(key, None)
            self._windows.pop(key, None)


GLOBAL_COUNTER = SlidingWindowCounter()
