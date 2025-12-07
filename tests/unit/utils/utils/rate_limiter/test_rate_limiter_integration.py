"""Integration tests for sync/async rate limiter sharing and mixed operations."""

import asyncio
import threading
from typing import Any

import pytest

from pyldb.api.utils import rate_limiter
from pyldb.api.utils.rate_limiter import RateLimitError


@pytest.mark.unit
def test_sync_async_share_cache_file(tmp_path: Any) -> None:
    """Test that sync and async rate limiters share the same cache file."""
    cache_file = tmp_path / "shared_cache.json"
    cache = rate_limiter.PersistentQuotaCache(enabled=True)
    cache.cache_file = str(cache_file)

    quotas: dict[int, int | tuple[Any, ...]] = {1: 3}
    sync_limiter = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)
    async_limiter = rate_limiter.AsyncRateLimiter(quotas, is_registered=False, cache=cache)

    # Make 2 sync calls
    sync_limiter.acquire()
    sync_limiter.acquire()

    # Make 1 async call - should succeed (total 3, limit is 3)
    async def run() -> None:
        await async_limiter.acquire()
        # Next call should fail (would be 4th call)
        with pytest.raises(RateLimitError):
            await async_limiter.acquire()

    asyncio.run(run())


@pytest.mark.unit
def test_sync_async_share_quota_via_cache(tmp_path: Any) -> None:
    """Test that sync and async rate limiters share quota state via cache."""
    cache_file = tmp_path / "shared_quota.json"
    cache = rate_limiter.PersistentQuotaCache(enabled=True)
    cache.cache_file = str(cache_file)

    quotas: dict[int, int | tuple[Any, ...]] = {1: 5}

    # Create sync limiter, use 3 calls, save to cache
    sync_limiter = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)
    sync_limiter.acquire()
    sync_limiter.acquire()
    sync_limiter.acquire()
    sync_limiter._save_to_cache()

    # Create new async limiter, load from cache, should see 3 calls
    async_limiter = rate_limiter.AsyncRateLimiter(quotas, is_registered=False, cache=cache)
    async_limiter._load_from_cache()

    async def run() -> None:
        # Should be able to make 2 more calls (3 + 2 = 5)
        await async_limiter.acquire()
        await async_limiter.acquire()
        # Next call should fail
        with pytest.raises(RateLimitError):
            await async_limiter.acquire()

    asyncio.run(run())


@pytest.mark.unit
def test_mixed_sync_async_operations(tmp_path: Any) -> None:
    """Test mixed sync and async operations sharing cache."""
    cache_file = tmp_path / "mixed_cache.json"
    cache = rate_limiter.PersistentQuotaCache(enabled=True)
    cache.cache_file = str(cache_file)

    quotas: dict[int, int | tuple[Any, ...]] = {1: 10}

    sync_limiter = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)
    async_limiter = rate_limiter.AsyncRateLimiter(quotas, is_registered=False, cache=cache)

    # Make 5 sync calls
    for _ in range(5):
        sync_limiter.acquire()

    async def run() -> None:
        # Make 5 async calls - should succeed (total 10)
        for _ in range(5):
            await async_limiter.acquire()

        # Next call should fail
        with pytest.raises(RateLimitError):
            await async_limiter.acquire()

    asyncio.run(run())


@pytest.mark.unit
def test_concurrent_sync_async_mixed(tmp_path: Any) -> None:
    """Test concurrent sync and async operations."""
    cache_file = tmp_path / "concurrent_cache.json"
    cache = rate_limiter.PersistentQuotaCache(enabled=True)
    cache.cache_file = str(cache_file)

    quotas: dict[int, int | tuple[Any, ...]] = {1: 10}
    sync_limiter = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)
    async_limiter = rate_limiter.AsyncRateLimiter(quotas, is_registered=False, cache=cache)

    sync_calls = []
    async_calls = []

    def sync_worker() -> None:
        try:
            sync_limiter.acquire()
            sync_calls.append(1)
        except RateLimitError:
            pass

    async def async_worker() -> None:
        try:
            await async_limiter.acquire()
            async_calls.append(1)
        except RateLimitError:
            pass

    # Run 10 sync calls concurrently
    threads = [threading.Thread(target=sync_worker) for _ in range(10)]
    for t in threads:
        t.start()

    async def run_async() -> None:
        # Run 10 async calls concurrently
        await asyncio.gather(*(async_worker() for _ in range(10)))

    asyncio.run(run_async())

    for t in threads:
        t.join()

    # Total successful calls should be <= 10 (the limit)
    total_calls = len(sync_calls) + len(async_calls)
    assert total_calls <= 10, f"Expected <= 10 calls, got {total_calls}"
