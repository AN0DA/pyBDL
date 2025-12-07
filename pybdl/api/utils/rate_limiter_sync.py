"""Synchronous rate limiter for API requests."""

import threading
import time
from collections import deque
from typing import Any, Literal

from pybdl.api.exceptions import RateLimitDelayExceeded, RateLimitError
from pybdl.api.utils.quota_cache import PersistentQuotaCache


class RateLimiter:
    """
    Thread-safe synchronous rate limiter for API requests.

    Enforces multiple quota periods (e.g., per second, per minute) and persists usage if a cache is provided.
    """

    def __init__(
        self,
        quotas: dict[int, int | tuple],
        is_registered: bool,
        cache: PersistentQuotaCache | None = None,
        max_delay: float | None = None,
        raise_on_limit: bool = True,
        buffer_seconds: float = 0.05,
    ) -> None:
        """
        Initialize the rate limiter.

        Args:
            quotas: Dictionary of {period_seconds: limit or (anon_limit, reg_limit)}.
            is_registered: Whether the user is registered (affects quota).
            cache: Optional persistent cache for quota usage.
            max_delay: Maximum delay in seconds to wait. None = wait forever, 0 = raise immediately.
            raise_on_limit: If True, raise exception immediately when limit exceeded. If False, wait.
            buffer_seconds: Small buffer time to add to wait calculations (default: 0.05s).
        """
        self.quotas = quotas
        self.is_registered = is_registered
        self.lock = threading.Lock()
        self.calls: dict[int, deque[float]] = {period: deque() for period in quotas}
        self.cache = cache
        # Use unified cache key so sync and async share quota state
        self.cache_key = f"{'reg' if is_registered else 'anon'}"
        self.max_delay = max_delay
        self.raise_on_limit = raise_on_limit
        self.buffer_seconds = buffer_seconds
        if self.cache and self.cache.enabled:
            self._load_from_cache()

    def _get_limit(self, period: int) -> int:
        # quotas: {period: tuple of (anonymous_limit, registered_limit) or int}
        limit_value = self.quotas[period]
        if isinstance(limit_value, tuple):
            return limit_value[1] if self.is_registered else limit_value[0]
        return limit_value

    def _load_from_cache(self, merge: bool = True) -> None:
        """
        Load quota state from cache.

        Args:
            merge: If True, merge cached calls with current calls. If False, replace current with cached.
        """
        if self.cache is not None:
            for period in self.quotas:
                cached = self.cache.get(f"{self.cache_key}_{period}")
                if merge:
                    # Merge cached calls with current calls, keeping all unique timestamps
                    current_times = set(self.calls[period])
                    cached_times = set(cached)
                    merged = sorted(current_times | cached_times)
                    self.calls[period] = deque(merged)
                else:
                    # Replace current calls with cached calls (for atomic check-before-record)
                    self.calls[period] = deque(cached)

    def _save_to_cache(self) -> None:
        if not self.cache or not self.cache.enabled:
            return
        for period in self.quotas:
            self.cache.set(f"{self.cache_key}_{period}", list(self.calls[period]))

    def _get_limit_info(self) -> dict[str, Any]:
        """Get current rate limit information."""
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

    def get_remaining_quota(self) -> dict[int, int]:
        """Get remaining quota for each period."""
        now = time.monotonic()
        remaining = {}

        with self.lock:
            for period in self.quotas:
                q = self.calls[period]
                limit = self._get_limit(period)

                # Clean up old calls
                while q and q[0] <= now - period:
                    q.popleft()

                remaining[period] = max(0, limit - len(q))

        return remaining

    def reset(self) -> None:
        """Reset all quota counters."""
        with self.lock:
            for period in self.quotas:
                self.calls[period].clear()
            self._save_to_cache()

    def acquire(self) -> None:
        """
        Acquire a slot for an API request.

        If rate limit is exceeded:
        - If raise_on_limit=True: Raises RateLimitError immediately
        - If raise_on_limit=False: Sleeps until quota available
        - If max_delay is set and wait_time > max_delay: Raises RateLimitDelayExceeded

        Raises:
            RateLimitError: If the rate limit is exceeded and raise_on_limit=True.
            RateLimitDelayExceeded: If required delay exceeds max_delay.
        """
        now = time.monotonic()
        with self.lock:
            # Reload from cache to get updates from other limiters (sync/async)
            if self.cache and self.cache.enabled:
                self._load_from_cache()
            # Find the longest wait time needed across all periods
            max_wait = 0.0
            for period in self.quotas:
                q = self.calls[period]
                limit = self._get_limit(period)

                # Remove old calls
                while q and q[0] <= now - period:
                    q.popleft()

                if len(q) >= limit:
                    wait_time = period - (now - q[0])
                    max_wait = max(max_wait, wait_time)

            # Handle rate limit exceeded
            if max_wait > 0:
                # Add buffer to be safe
                max_wait += self.buffer_seconds

                if self.raise_on_limit:
                    self._save_to_cache()
                    raise RateLimitError(
                        retry_after=max_wait,
                        limit_info=self._get_limit_info(),
                    )

                if self.max_delay is not None and max_wait > self.max_delay:
                    self._save_to_cache()
                    raise RateLimitDelayExceeded(
                        actual_delay=max_wait,
                        max_delay=self.max_delay,
                        limit_info=self._get_limit_info(),
                    )

                # Wait until quota available
                time.sleep(max_wait)

            # Record this call
            now = time.monotonic()  # Refresh after potential sleep

            # Use atomic cache operation to prevent race conditions
            if self.cache and self.cache.enabled:
                # Try to atomically record the call in cache for each period
                # This ensures only one limiter can successfully record at a time
                for period in self.quotas:
                    limit = self._get_limit(period)
                    cache_key = f"{self.cache_key}_{period}"
                    cleanup_before = now - period  # Remove calls older than this

                    # Try atomic append - this checks limits and records atomically
                    success = self.cache.try_append_if_under_limit(cache_key, now, limit, cleanup_before)

                    if not success:
                        # Failed to record - we're at the limit
                        # Reload to get current state for error message
                        self._load_from_cache(merge=False)
                        q = self.calls[period]
                        while q and q[0] <= now - period:
                            q.popleft()
                        wait_time = (
                            (period - (now - q[0]) + self.buffer_seconds) if q and q[0] > now - period else period
                        )

                        if self.raise_on_limit:
                            raise RateLimitError(
                                retry_after=wait_time,
                                limit_info=self._get_limit_info(),
                            )
                        if self.max_delay is not None and wait_time > self.max_delay:
                            raise RateLimitDelayExceeded(
                                actual_delay=wait_time,
                                max_delay=self.max_delay,
                                limit_info=self._get_limit_info(),
                            )
                        time.sleep(wait_time)
                        now = time.monotonic()  # Refresh after sleep
                        cleanup_before = now - period
                        # Retry after waiting
                        success = self.cache.try_append_if_under_limit(cache_key, now, limit, cleanup_before)
                        if not success:
                            # Still at limit after waiting - reload and raise
                            self._load_from_cache(merge=False)
                            q = self.calls[period]
                            while q and q[0] <= now - period:
                                q.popleft()
                            wait_time = (
                                (period - (now - q[0]) + self.buffer_seconds) if q and q[0] > now - period else period
                            )
                            if self.raise_on_limit:
                                raise RateLimitError(
                                    retry_after=wait_time,
                                    limit_info=self._get_limit_info(),
                                )

                # Reload from cache to sync local state with what was actually recorded
                self._load_from_cache(merge=False)
            else:
                # No cache - just record locally
                for period in self.quotas:
                    self.calls[period].append(now)

    def __enter__(self) -> "RateLimiter":
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        """Context manager exit."""
        return False
