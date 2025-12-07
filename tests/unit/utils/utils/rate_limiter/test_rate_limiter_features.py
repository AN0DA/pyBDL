"""Tests for rate limiter features: wait/raise, context managers, decorators, helpers."""

import asyncio
import time
from typing import Any

import pytest

from pybdl.api.utils import rate_limiter
from pybdl.api.utils.rate_limiter import RateLimitDelayExceeded, RateLimitError


@pytest.mark.unit
def test_rate_limiter_wait_behavior() -> None:
    """Test that rate limiter can wait instead of raising."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, raise_on_limit=False, max_delay=2.0)

    start_time = time.monotonic()
    rl.acquire()
    rl.acquire()
    # Third call should wait ~1 second
    rl.acquire()
    elapsed = time.monotonic() - start_time

    # Should have waited approximately 1 second (with buffer)
    assert elapsed >= 0.9, f"Expected wait time ~1s, got {elapsed}s"
    assert elapsed < 2.0, f"Should not wait more than max_delay, got {elapsed}s"


@pytest.mark.unit
def test_rate_limiter_max_delay_exceeded() -> None:
    """Test that RateLimitDelayExceeded is raised when max_delay is exceeded."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, raise_on_limit=False, max_delay=0.5)

    rl.acquire()
    rl.acquire()
    # Third call would need to wait ~1s, but max_delay is 0.5s
    with pytest.raises(RateLimitDelayExceeded) as e:
        rl.acquire()
    assert e.value.actual_delay > e.value.max_delay
    assert "exceeds maximum allowed delay" in str(e.value)


@pytest.mark.unit
def test_rate_limiter_with_zero_max_delay() -> None:
    """Test rate limiter with max_delay=0 (raise immediately)."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, raise_on_limit=False, max_delay=0.0)

    rl.acquire()
    rl.acquire()

    # Should raise immediately (max_delay=0 means don't wait)
    with pytest.raises(RateLimitDelayExceeded):
        rl.acquire()


@pytest.mark.unit
def test_rate_limiter_with_none_max_delay() -> None:
    """Test rate limiter with max_delay=None (wait forever)."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, raise_on_limit=False, max_delay=None)

    rl.acquire()
    rl.acquire()

    start_time = time.monotonic()
    rl.acquire()  # Should wait without limit
    elapsed = time.monotonic() - start_time

    # Should have waited (no max_delay restriction)
    assert elapsed >= 0.9


@pytest.mark.unit
def test_rate_limiter_context_manager() -> None:
    """Test rate limiter as context manager."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)

    # First call
    with rl:
        pass

    # Second call
    with rl:
        pass

    # Third call should raise
    with pytest.raises(RateLimitError), rl:
        pass


@pytest.mark.unit
def test_async_rate_limiter_context_manager() -> None:
    """Test async rate limiter as context manager."""

    async def run() -> None:
        quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
        arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)

        # First call
        async with arl:
            pass

        # Second call
        async with arl:
            pass

        # Third call should raise
        with pytest.raises(RateLimitError):
            async with arl:
                pass

    asyncio.run(run())


@pytest.mark.unit
def test_rate_limit_decorator() -> None:
    """Test rate_limit decorator."""
    from pybdl.api.utils.rate_limiter import rate_limit

    call_count = 0

    @rate_limit(quotas={1: 2}, is_registered=False)
    def test_function() -> int:
        nonlocal call_count
        call_count += 1
        return call_count

    assert test_function() == 1
    assert test_function() == 2
    # Third call should raise
    with pytest.raises(RateLimitError):
        test_function()


@pytest.mark.unit
def test_async_rate_limit_decorator() -> None:
    """Test async_rate_limit decorator."""

    async def run() -> None:
        from pybdl.api.utils.rate_limiter import async_rate_limit

        call_count = 0

        @async_rate_limit(quotas={1: 2}, is_registered=False)
        async def test_function() -> int:
            nonlocal call_count
            call_count += 1
            return call_count

        assert await test_function() == 1
        assert await test_function() == 2
        # Third call should raise
        with pytest.raises(RateLimitError):
            await test_function()

    asyncio.run(run())


@pytest.mark.unit
def test_decorator_preserves_function_metadata() -> None:
    """Test that decorators preserve function metadata."""
    from pybdl.api.utils.rate_limiter import rate_limit

    @rate_limit(quotas={1: 10}, is_registered=False)
    def documented_function() -> int:
        """This is a test function."""
        return 42

    assert documented_function.__name__ == "documented_function"
    assert documented_function.__doc__ == "This is a test function."


@pytest.mark.unit
def test_get_remaining_quota() -> None:
    """Test get_remaining_quota helper method."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 5, 60: 10}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)

    # Initially should have full quota
    remaining = rl.get_remaining_quota()
    assert remaining[1] == 5
    assert remaining[60] == 10

    # Make 2 calls
    rl.acquire()
    rl.acquire()

    remaining = rl.get_remaining_quota()
    assert remaining[1] == 3
    assert remaining[60] == 8


@pytest.mark.unit
def test_reset_quota() -> None:
    """Test reset helper method."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 3}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)

    rl.acquire()
    rl.acquire()
    assert rl.get_remaining_quota()[1] == 1

    rl.reset()
    assert rl.get_remaining_quota()[1] == 3

    # Should be able to make 3 calls again
    rl.acquire()
    rl.acquire()
    rl.acquire()
    with pytest.raises(RateLimitError):
        rl.acquire()


@pytest.mark.unit
def test_async_get_remaining_quota() -> None:
    """Test async get_remaining_quota helper method."""

    async def run() -> None:
        quotas: dict[int, int | tuple[Any, ...]] = {1: 5}
        arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)

        # Initially should have full quota
        remaining = await arl.get_remaining_quota_async()
        assert remaining[1] == 5

        # Make 2 calls
        await arl.acquire()
        await arl.acquire()

        remaining = await arl.get_remaining_quota_async()
        assert remaining[1] == 3

    asyncio.run(run())


@pytest.mark.unit
def test_async_reset_quota() -> None:
    """Test async reset helper method."""

    async def run() -> None:
        quotas: dict[int, int | tuple[Any, ...]] = {1: 3}
        arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)

        await arl.acquire()
        await arl.acquire()
        remaining = await arl.get_remaining_quota_async()
        assert remaining[1] == 1

        await arl.reset_async()
        remaining = await arl.get_remaining_quota_async()
        assert remaining[1] == 3

        # Should be able to make 3 calls again
        await arl.acquire()
        await arl.acquire()
        await arl.acquire()
        with pytest.raises(RateLimitError):
            await arl.acquire()

    asyncio.run(run())


@pytest.mark.unit
def test_get_limit_info() -> None:
    """Test _get_limit_info helper method."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 5, 60: 100}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)

    info = rl._get_limit_info()
    assert "quotas" in info
    assert "is_registered" in info
    assert info["is_registered"] is False
    assert len(info["quotas"]) == 2

    rl.acquire()
    info = rl._get_limit_info()
    # Check that current count is updated
    period_1_info = next(q for q in info["quotas"] if q["period"] == 1)
    assert period_1_info["current"] == 1


@pytest.mark.unit
def test_rate_limiter_buffer() -> None:
    """Test that buffer_seconds adds safety margin."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    # Use very small buffer to test
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, raise_on_limit=False, buffer_seconds=0.1)

    rl.acquire()
    rl.acquire()

    start_time = time.monotonic()
    rl.acquire()  # Should wait ~1s + 0.1s buffer
    elapsed = time.monotonic() - start_time

    # Should wait at least 1 second + buffer
    assert elapsed >= 1.0


@pytest.mark.unit
def test_rate_limit_error_attributes() -> None:
    """Test RateLimitError has proper attributes."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)

    rl.acquire()
    rl.acquire()

    with pytest.raises(RateLimitError) as e:
        rl.acquire()

    assert hasattr(e.value, "retry_after")
    assert hasattr(e.value, "limit_info")
    assert e.value.retry_after > 0
    assert "quotas" in e.value.limit_info
