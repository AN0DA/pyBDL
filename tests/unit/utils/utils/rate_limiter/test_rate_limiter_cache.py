"""Tests for PersistentQuotaCache functionality."""

import multiprocessing
import time
from typing import Any

import pytest

from pybdl.utils import rate_limiter


class DummyCache(rate_limiter.PersistentQuotaCache):
    def __init__(self) -> None:
        super().__init__(enabled=True)  # Set enabled=True so _save is called
        self._data = {}
        self.saved = False

    def _load(self) -> None:
        """Keep in-memory test state; do not reload from the default cache file."""

    def _save(self) -> None:
        self.saved = True

    def _save_unlocked(self) -> None:
        self.saved = True


@pytest.mark.unit
def test_persistent_quota_cache_get_set(tmp_path: Any) -> None:
    """Test basic cache get/set operations."""
    cache_file = tmp_path / "quota_cache.json"
    cache = rate_limiter.PersistentQuotaCache(enabled=True)
    cache.cache_file = cache_file
    cache.set("foo", [1, 2, 3])
    assert cache.get("foo") == [1, 2, 3]
    # Test persistence
    cache2 = rate_limiter.PersistentQuotaCache(enabled=True)
    cache2.cache_file = cache_file
    cache2._load()
    assert cache2.get("foo") == [1, 2, 3]


@pytest.mark.unit
def test_persistent_quota_cache_disabled() -> None:
    """Test cache behavior when disabled."""
    cache = rate_limiter.PersistentQuotaCache(enabled=False)
    cache.set("foo", [1, 2, 3])
    assert cache.get("foo") == []


@pytest.mark.unit
def test_rate_limiter_cache(tmp_path: Any) -> None:
    """Test that rate limiter saves to cache."""
    from pybdl.utils.rate_limiter import BDLRateLimitError

    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    cache = DummyCache()
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)
    rl.acquire()
    rl.acquire()
    with pytest.raises(BDLRateLimitError):
        rl.acquire()  # Triggers save
    assert cache.saved


@pytest.mark.unit
def test_async_rate_limiter_cache(tmp_path: Any) -> None:
    """Test that async rate limiter saves to cache."""
    import asyncio

    from pybdl.utils.rate_limiter import BDLRateLimitError

    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    cache = DummyCache()
    arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False, cache=cache)

    async def run() -> None:
        await arl.acquire()
        await arl.acquire()
        with pytest.raises(BDLRateLimitError):
            await arl.acquire()  # Triggers save
        assert cache.saved

    asyncio.run(run())


@pytest.mark.unit
def test_cache_persistence_across_instances(tmp_path: Any) -> None:
    """Test that quota state persists across different limiter instances."""
    cache_file = tmp_path / "persist_cache.json"
    cache = rate_limiter.PersistentQuotaCache(enabled=True)
    cache.cache_file = cache_file

    quotas: dict[int, int | tuple[Any, ...]] = {1: 5}

    # First instance - make 3 calls
    rl1 = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)
    rl1.acquire()
    rl1.acquire()
    rl1.acquire()

    # Create second instance - should see the 3 calls from cache
    rl2 = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)
    # Should only be able to make 2 more calls
    rl2.acquire()
    rl2.acquire()
    from pybdl.utils.rate_limiter import BDLRateLimitError

    with pytest.raises(BDLRateLimitError):
        rl2.acquire()


@pytest.mark.unit
def test_cache_disabled_behavior() -> None:
    """Test rate limiter behavior when cache is disabled."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 3}
    cache = rate_limiter.PersistentQuotaCache(enabled=False)
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)

    # Should work normally
    rl.acquire()
    rl.acquire()
    rl.acquire()
    from pybdl.utils.rate_limiter import BDLRateLimitError

    with pytest.raises(BDLRateLimitError):
        rl.acquire()

    # But cache should not be used
    rl2 = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)
    # Should be able to make 3 calls again (cache not shared)
    rl2.acquire()
    rl2.acquire()
    rl2.acquire()


@pytest.mark.unit
def test_cache_atomic_write(tmp_path: Any) -> None:
    """Test that cache writes are atomic (temp file + rename)."""
    cache_file = tmp_path / "atomic_cache.json"
    cache = rate_limiter.PersistentQuotaCache(enabled=True)
    cache.cache_file = cache_file

    quotas: dict[int, int | tuple[Any, ...]] = {1: 5}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)

    # Make calls to trigger saves
    for _ in range(3):
        rl.acquire()

    # Verify cache file exists and temp file doesn't
    assert cache_file.exists()
    temp_file = cache_file.with_suffix(".tmp")
    assert not temp_file.exists(), "Temp file should be cleaned up after atomic write"


def _cross_process_acquire(cache_file: str) -> None:
    cache = rate_limiter.PersistentQuotaCache(enabled=True, cache_file=cache_file)
    rate_limiter.RateLimiter({1: 3}, is_registered=False, cache=cache).acquire()


@pytest.mark.unit
def test_cross_process_quota_records_are_shared(tmp_path: Any) -> None:
    """Parallel processes must share quota state through the cache file."""
    cache_file = tmp_path / "cross_process_quota.json"
    processes = [multiprocessing.Process(target=_cross_process_acquire, args=(str(cache_file),)) for _ in range(3)]
    for process in processes:
        process.start()
    for process in processes:
        process.join()
        assert process.exitcode == 0

    shared_cache = rate_limiter.PersistentQuotaCache(enabled=True, cache_file=cache_file)
    rl = rate_limiter.RateLimiter({1: 3}, is_registered=False, cache=shared_cache)
    with pytest.raises(rate_limiter.BDLRateLimitError):
        rl.acquire()


@pytest.mark.unit
def test_clean_list_uses_fresh_time_after_lock(tmp_path: Any) -> None:
    """Live cleanup must not drop entries that look future relative to stale pre-lock time."""
    cache_file = tmp_path / "quota_cache.json"
    cache = rate_limiter.PersistentQuotaCache(enabled=True, cache_file=cache_file)
    key = "anon_1"
    fresh_now = time.time()
    cache._data[key] = [fresh_now - 0.01]

    with cache._interprocess_lock():
        cleaned = cache._clean_list(key, period=1)

    assert cleaned == [fresh_now - 0.01]


@pytest.mark.unit
def test_rate_limiter_release_refunds_quota() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)

    reservation = rl.acquire()
    assert rl.get_remaining_quota()[1] == 1

    rl.release(reservation)
    assert rl.get_remaining_quota()[1] == 2


@pytest.mark.unit
def test_async_rate_limiter_release_refunds_quota() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)

    async def run() -> None:
        reservation = await arl.acquire()
        assert (await arl.get_remaining_quota_async())[1] == 1
        await arl.release(reservation)
        assert (await arl.get_remaining_quota_async())[1] == 2

    import asyncio

    asyncio.run(run())
