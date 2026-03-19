from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass
class ProviderRateLimiter:
    requests_per_minute: int
    threshold_ratio: float = 0.8
    window: timedelta = field(default_factory=lambda: timedelta(minutes=1))
    _requests: deque[datetime] = field(default_factory=deque)

    @property
    def effective_limit(self) -> int:
        return max(1, int(self.requests_per_minute * self.threshold_ratio))

    def can_allow(self, now: datetime | None = None) -> bool:
        now = now or datetime.utcnow()
        self._prune(now)
        return len(self._requests) < self.effective_limit

    def consume(self, now: datetime | None = None) -> bool:
        now = now or datetime.utcnow()
        if not self.can_allow(now):
            return False
        self._requests.append(now)
        return True

    def allow(self, now: datetime | None = None) -> bool:
        return self.consume(now)

    def current_requests(self, now: datetime | None = None) -> int:
        now = now or datetime.utcnow()
        self._prune(now)
        return len(self._requests)

    def remaining_budget(self, now: datetime | None = None) -> int:
        return max(0, self.effective_limit - self.current_requests(now))

    def usage_ratio(self, now: datetime | None = None) -> float:
        if self.effective_limit == 0:
            return 1.0
        return self.current_requests(now) / self.effective_limit

    def _prune(self, now: datetime) -> None:
        cutoff = now - self.window
        while self._requests and self._requests[0] <= cutoff:
            self._requests.popleft()
