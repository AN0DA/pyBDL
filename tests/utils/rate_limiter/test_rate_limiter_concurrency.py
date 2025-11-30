"""Tests for rate limiter concurrency and thread safety."""

import asyncio
import threading
from typing import Any

from pyldb.api.utils import rate_limiter
from pyldb.api.utils.rate_limiter import RateLimitError


def test_rate_limiter_thread_safety() -> None:
    """Test that rate limiter is thread-safe."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 5}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)
    errors = []

    def worker() -> None:
        try:
            rl.acquire()
        except RateLimitError:
            errors.append(1)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(errors) >= 5


def test_async_rate_limiter_concurrent() -> None:
    """Test that async rate limiter handles concurrent operations."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 5}
    arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)
    errors = []

    async def worker() -> None:
        try:
            await arl.acquire()
        except RateLimitError:
            errors.append(1)

    async def run() -> None:
        await asyncio.gather(*(worker() for _ in range(10)))
        assert sum(errors) >= 5

    asyncio.run(run())
