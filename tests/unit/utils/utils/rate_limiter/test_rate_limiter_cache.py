"""Tests for PersistentQuotaCache functionality."""

from typing import Any

import pytest

from pyldb.api.utils import rate_limiter


class DummyCache(rate_limiter.PersistentQuotaCache):
    def __init__(self) -> None:
        super().__init__(enabled=True)  # Set enabled=True so _save is called
        self._data = {}
        self.saved = False

    def _save(self) -> None:
        self.saved = True


@pytest.mark.unit
def test_persistent_quota_cache_get_set(tmp_path: Any) -> None:
    """Test basic cache get/set operations."""
    cache_file = tmp_path / "quota_cache.json"
    cache = rate_limiter.PersistentQuotaCache(enabled=True)
    cache.cache_file = str(cache_file)
    cache.set("foo", [1, 2, 3])
    assert cache.get("foo") == [1, 2, 3]
    # Test persistence
    cache2 = rate_limiter.PersistentQuotaCache(enabled=True)
    cache2.cache_file = str(cache_file)
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
    from pyldb.api.utils.rate_limiter import RateLimitError

    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    cache = DummyCache()
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)
    rl.acquire()
    rl.acquire()
    with pytest.raises(RateLimitError):
        rl.acquire()  # Triggers save
    assert cache.saved


@pytest.mark.unit
def test_async_rate_limiter_cache(tmp_path: Any) -> None:
    """Test that async rate limiter saves to cache."""
    import asyncio

    from pyldb.api.utils.rate_limiter import RateLimitError

    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    cache = DummyCache()
    arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False, cache=cache)

    async def run() -> None:
        await arl.acquire()
        await arl.acquire()
        with pytest.raises(RateLimitError):
            await arl.acquire()  # Triggers save
        assert cache.saved

    asyncio.run(run())


@pytest.mark.unit
def test_cache_persistence_across_instances(tmp_path: Any) -> None:
    """Test that quota state persists across different limiter instances."""
    cache_file = tmp_path / "persist_cache.json"
    cache = rate_limiter.PersistentQuotaCache(enabled=True)
    cache.cache_file = str(cache_file)

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
    from pyldb.api.utils.rate_limiter import RateLimitError

    with pytest.raises(RateLimitError):
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
    from pyldb.api.utils.rate_limiter import RateLimitError

    with pytest.raises(RateLimitError):
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
    cache.cache_file = str(cache_file)

    quotas: dict[int, int | tuple[Any, ...]] = {1: 5}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, cache=cache)

    # Make calls to trigger saves
    for _ in range(3):
        rl.acquire()

    # Verify cache file exists and temp file doesn't
    assert cache_file.exists()
    temp_file = cache_file.with_suffix(".tmp")
    assert not temp_file.exists(), "Temp file should be cleaned up after atomic write"
