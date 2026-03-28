"""Lifecycle tests: reset, release, context managers, remaining quota, max_delay, empty quotas."""

from typing import Any

import pytest

from pybdl.api.exceptions import RateLimitDelayExceeded
from pybdl.utils import rate_limiter


@pytest.mark.unit
def test_sync_rate_limiter_reset_clears_counters() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 1}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)
    rl.acquire()
    with pytest.raises(rate_limiter.RateLimitError):
        rl.acquire()
    rl.reset()
    rl.acquire()


@pytest.mark.unit
def test_sync_rate_limiter_release_refunds_slot() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False, cache=None)
    t1 = rl.acquire()
    t2 = rl.acquire()
    assert t1 is not None and t2 is not None
    with pytest.raises(rate_limiter.RateLimitError):
        rl.acquire()
    rl.release(t1)
    rl.acquire()


@pytest.mark.unit
def test_sync_rate_limiter_context_manager_acquires() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 3}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)
    with rl:
        pass
    # One slot consumed by __enter__; two more fit before limit
    rl.acquire()
    rl.acquire()
    with pytest.raises(rate_limiter.RateLimitError):
        rl.acquire()


@pytest.mark.unit
def test_sync_rate_limiter_max_delay_exceeded() -> None:
    """When waiting would exceed max_delay, raise RateLimitDelayExceeded (no immediate raise)."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 1}
    rl = rate_limiter.RateLimiter(
        quotas,
        is_registered=False,
        raise_on_limit=False,
        max_delay=0.01,
    )
    rl.acquire()
    with pytest.raises(RateLimitDelayExceeded):
        rl.acquire()


@pytest.mark.unit
def test_sync_rate_limiter_empty_quotas_returns_none() -> None:
    rl = rate_limiter.RateLimiter({}, is_registered=False)
    assert rl.acquire() is None


@pytest.mark.unit
def test_sync_get_remaining_quota() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)
    assert rl.get_remaining_quota()[1] == 2
    rl.acquire()
    assert rl.get_remaining_quota()[1] == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_rate_limiter_empty_quotas_returns_none() -> None:
    arl = rate_limiter.AsyncRateLimiter({}, is_registered=False)
    assert await arl.acquire() is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_get_remaining_quota_and_async() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 3}
    arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)
    assert arl.get_remaining_quota()[1] == 3
    await arl.acquire()
    rem = await arl.get_remaining_quota_async()
    assert rem[1] == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_reset_and_reset_async() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 1}
    arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)
    await arl.acquire()
    with pytest.raises(rate_limiter.RateLimitError):
        await arl.acquire()
    arl.reset()
    await arl.acquire()

    with pytest.raises(rate_limiter.RateLimitError):
        await arl.acquire()
    await arl.reset_async()
    await arl.acquire()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_release_refunds() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)

    t1 = await arl.acquire()
    assert t1 is not None
    await arl.acquire()
    with pytest.raises(rate_limiter.RateLimitError):
        await arl.acquire()
    await arl.release(t1)
    await arl.acquire()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_release_noop_when_none() -> None:
    arl = rate_limiter.AsyncRateLimiter({1: 1}, is_registered=False)
    await arl.release(None)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_context_manager() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 3}
    arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)

    async with arl:
        pass
    await arl.acquire()
    await arl.acquire()
    with pytest.raises(rate_limiter.RateLimitError):
        await arl.acquire()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_async_max_delay_exceeded() -> None:
    quotas: dict[int, int | tuple[Any, ...]] = {1: 1}
    arl = rate_limiter.AsyncRateLimiter(
        quotas,
        is_registered=False,
        raise_on_limit=False,
        max_delay=0.01,
    )
    await arl.acquire()
    with pytest.raises(RateLimitDelayExceeded):
        await arl.acquire()
