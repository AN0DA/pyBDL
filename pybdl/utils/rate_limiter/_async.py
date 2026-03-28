"""Asynchronous rate limiter."""

import asyncio
import time
from typing import Any, Literal

from pybdl.utils.rate_limiter._base import RateLimiterBase
from pybdl.utils.rate_limiter._cache import PersistentQuotaCache


class AsyncRateLimiter(RateLimiterBase):
    """Asyncio-compatible rate limiter for API requests."""

    def __init__(
        self,
        quotas: dict[int, int | tuple[int, int]],
        is_registered: bool,
        cache: PersistentQuotaCache | None = None,
        max_delay: float | None = None,
        raise_on_limit: bool = True,
        buffer_seconds: float = 0.05,
    ) -> None:
        super().__init__(
            quotas=quotas,
            is_registered=is_registered,
            cache=cache,
            max_delay=max_delay,
            raise_on_limit=raise_on_limit,
            buffer_seconds=buffer_seconds,
        )
        self.lock = asyncio.Lock()

    async def acquire(self) -> float | None:
        if not self.quotas:
            return None

        while True:
            sleep_time = 0.0
            async with self.lock:
                if self.cache and self.cache.enabled:
                    self._sync_from_cache()
                now = time.monotonic()
                self._cleanup_expired(now)
                wait_time = self._compute_wait(now)
                if wait_time <= 0 and self._try_record(now):
                    return now
                sleep_time = self._check_wait_and_raise(wait_time) if wait_time > 0 else 0.0

            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    async def release(self, recorded_at: float | None) -> None:
        if recorded_at is None:
            return
        async with self.lock:
            self._remove_timestamp(recorded_at)

    def get_remaining_quota(self) -> dict[int, int]:
        """Return a snapshot without mutating state or awaiting a lock."""
        now = time.monotonic()
        if self.cache and self.cache.enabled:
            return {
                period: max(
                    0,
                    self._get_limit(period)
                    - len(
                        [
                            value
                            for value in self.cache.get(f"{self.cache_key}_{period}")
                            if isinstance(value, int | float) and value > now - period
                        ]
                    ),
                )
                for period in self.quotas
            }

        return {
            period: max(
                0,
                self._get_limit(period) - len([value for value in self.calls[period] if value > now - period]),
            )
            for period in self.quotas
        }

    async def get_remaining_quota_async(self) -> dict[int, int]:
        async with self.lock:
            if self.cache and self.cache.enabled:
                self._sync_from_cache()
            return self._get_remaining(time.monotonic())

    def reset(self) -> None:
        """Synchronous convenience helper for non-concurrent contexts."""
        self._reset_all()

    async def reset_async(self) -> None:
        async with self.lock:
            self._reset_all()

    async def __aenter__(self) -> "AsyncRateLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        return False
