"""Shared business logic for sync and async rate limiters."""

import time
from collections import deque
from typing import Any

from pybdl.api.exceptions import BDLRateLimitDelayError, BDLRateLimitError
from pybdl.utils.rate_limiter._cache import PersistentQuotaCache


class RateLimiterBase:
    """Shared quota bookkeeping and cache interaction."""

    def __init__(
        self,
        quotas: dict[int, int | tuple[int, int]],
        is_registered: bool,
        cache: PersistentQuotaCache | None = None,
        max_delay: float | None = None,
        raise_on_limit: bool = True,
        buffer_seconds: float = 0.05,
    ) -> None:
        self.quotas = quotas
        self.is_registered = is_registered
        self.calls: dict[int, deque[float]] = {period: deque() for period in quotas}
        self.cache = cache
        self.cache_key = "reg" if is_registered else "anon"
        self.max_delay = max_delay
        self.raise_on_limit = raise_on_limit
        self.buffer_seconds = buffer_seconds
        if self.cache and self.cache.enabled:
            self._sync_from_cache()

    def _now(self) -> float:
        """Return a clock comparable across processes when using persistent cache."""
        if self.cache and self.cache.enabled:
            return time.time()
        return time.monotonic()

    def _get_limit(self, period: int) -> int:
        limit_value = self.quotas[period]
        if isinstance(limit_value, tuple):
            return limit_value[1] if self.is_registered else limit_value[0]
        return limit_value

    def _sync_from_cache(self, merge: bool = True) -> None:
        if self.cache is None:
            return
        now = self._now()
        for period in self.quotas:
            cached = self.cache.get_valid_timestamps(
                f"{self.cache_key}_{period}",
                now,
                period,
                buffer_seconds=self.buffer_seconds,
            )
            if merge:
                merged = sorted(set(self._valid_local_timestamps(self.calls[period], now, period)) | set(cached))
                self.calls[period] = deque(merged)
            else:
                self.calls[period] = deque(cached)

    def _load_from_cache(self, merge: bool = True) -> None:
        """Backward-compatible alias for tests that still call the old helper."""
        self._sync_from_cache(merge=merge)

    def _save_to_cache(self) -> None:
        if not self.cache or not self.cache.enabled:
            return
        for period in self.quotas:
            self.cache.set(f"{self.cache_key}_{period}", list(self.calls[period]))

    def _valid_local_timestamps(self, timestamps: deque[float], now: float, period: int) -> list[float]:
        if self.cache and self.cache.enabled:
            return PersistentQuotaCache.filter_valid_timestamps(
                list(timestamps),
                now,
                period,
                buffer_seconds=self.buffer_seconds,
            )
        return sorted(value for value in timestamps if value > now - period)

    def _cleanup_expired(self, now: float) -> None:
        for period, q in self.calls.items():
            if self.cache and self.cache.enabled:
                valid = self._valid_local_timestamps(q, now, period)
            else:
                while q and q[0] <= now - period:
                    q.popleft()
                valid = list(q)
            self.calls[period] = deque(valid)

    def _compute_wait(self, now: float) -> float:
        max_wait = 0.0
        for period, q in self.calls.items():
            limit = self._get_limit(period)
            if len(q) >= limit:
                max_wait = max(max_wait, period - (now - q[0]))
        return max_wait

    def _seconds_until_available_at(self, now: float) -> float:
        """Seconds until a quota slot is available (0 if immediate). Includes buffer_seconds."""
        self._cleanup_expired(now)
        wait = self._compute_wait(now)
        return (wait + self.buffer_seconds) if wait > 0 else 0.0

    def _check_wait_and_raise(self, raw_wait: float) -> float:
        if raw_wait <= 0:
            return 0.0

        wait_time = raw_wait + self.buffer_seconds
        if self.raise_on_limit:
            raise BDLRateLimitError(
                retry_after=wait_time,
                limit_info=self._get_limit_info(),
            )
        if self.max_delay is not None and wait_time > self.max_delay:
            raise BDLRateLimitDelayError(
                actual_delay=wait_time,
                max_delay=self.max_delay,
                limit_info=self._get_limit_info(),
            )
        return wait_time

    def _cache_period_configs(self) -> list[tuple[str, int, int]]:
        return [(f"{self.cache_key}_{period}", self._get_limit(period), period) for period in self.quotas]

    def _cache_keys(self) -> list[str]:
        return [f"{self.cache_key}_{period}" for period in self.quotas]

    def _try_record(self, now: float) -> bool:
        if self.cache and self.cache.enabled:
            success = self.cache.try_record_all_periods(self._cache_period_configs(), now)
            self._sync_from_cache(merge=False)
            self._cleanup_expired(now)
            return success

        for period in self.quotas:
            self.calls[period].append(now)
        return True

    def _remove_local_timestamp(self, period: int, recorded_at: float) -> None:
        calls = list(self.calls[period])
        for index in range(len(calls) - 1, -1, -1):
            if calls[index] == recorded_at:
                del calls[index]
                self.calls[period] = deque(calls)
                break

    def _remove_timestamp(self, recorded_at: float) -> None:
        for period in self.quotas:
            self._remove_local_timestamp(period, recorded_at)
        if self.cache and self.cache.enabled:
            self.cache.remove_from_all_periods(self._cache_keys(), recorded_at)

    def _get_remaining(self, now: float) -> dict[int, int]:
        self._cleanup_expired(now)
        return {period: max(0, self._get_limit(period) - len(self.calls[period])) for period in self.quotas}

    def _reset_all(self) -> None:
        for period in self.quotas:
            self.calls[period].clear()
        self._save_to_cache()

    def _get_limit_info(self) -> dict[str, Any]:
        return {
            "quotas": [
                {
                    "period": period,
                    "limit": self._get_limit(period),
                    "current": len(self.calls[period]),
                }
                for period in self.quotas
            ],
            "is_registered": self.is_registered,
        }
