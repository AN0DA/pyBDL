"""Tests for core rate limiter functionality."""

import asyncio
import time
from typing import Any

import pytest

from pybdl.api.utils import rate_limiter
from pybdl.api.utils.rate_limiter import RateLimitError


@pytest.mark.unit
def test_rate_limiter_basic() -> None:
    """Test basic rate limiting functionality."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2, 5: 3}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)
    rl.acquire()
    rl.acquire()
    # Should raise on third call within 1s
    with pytest.raises(RateLimitError) as e:
        rl.acquire()
    assert "Rate limit exceeded" in str(e.value)


@pytest.mark.unit
def test_rate_limiter_respects_period() -> None:
    """Test that rate limiter respects time periods."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)
    rl.acquire()
    rl.acquire()
    with pytest.raises(RateLimitError):
        rl.acquire()
    time.sleep(1.1)
    rl.acquire()  # Should not raise after period expires


@pytest.mark.unit
def test_rate_limiter_tuple_quota() -> None:
    """Test rate limiter with tuple quotas (anon vs registered)."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: (1, 2)}
    rl_anon = rate_limiter.RateLimiter(quotas, is_registered=False)
    rl_reg = rate_limiter.RateLimiter(quotas, is_registered=True)
    rl_anon.acquire()
    with pytest.raises(RateLimitError):
        rl_anon.acquire()
    rl_reg.acquire()
    rl_reg.acquire()
    with pytest.raises(RateLimitError):
        rl_reg.acquire()


@pytest.mark.unit
def test_async_rate_limiter_basic() -> None:
    """Test basic async rate limiting functionality."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 2}
    arl = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)

    async def run() -> None:
        await arl.acquire()
        await arl.acquire()
        with pytest.raises(RateLimitError):
            await arl.acquire()

    asyncio.run(run())


@pytest.mark.unit
def test_async_rate_limiter_tuple_quota() -> None:
    """Test async rate limiter with tuple quotas."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: (1, 2)}
    arl_anon = rate_limiter.AsyncRateLimiter(quotas, is_registered=False)
    arl_reg = rate_limiter.AsyncRateLimiter(quotas, is_registered=True)

    async def run() -> None:
        await arl_anon.acquire()
        with pytest.raises(RateLimitError):
            await arl_anon.acquire()
        await arl_reg.acquire()
        await arl_reg.acquire()
        with pytest.raises(RateLimitError):
            await arl_reg.acquire()

    asyncio.run(run())


@pytest.mark.unit
def test_multiple_periods() -> None:
    """Test rate limiter with multiple quota periods."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: 3, 5: 10}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)

    # Make 3 calls - should hit 1-second limit
    rl.acquire()
    rl.acquire()
    rl.acquire()
    with pytest.raises(RateLimitError):
        rl.acquire()

    # Wait for 1-second period to expire
    time.sleep(1.1)

    # Should be able to make more calls (5-second limit not hit)
    rl.acquire()
    rl.acquire()
    rl.acquire()
    with pytest.raises(RateLimitError):
        rl.acquire()


@pytest.mark.unit
def test_registered_vs_anonymous_quotas() -> None:
    """Test that registered and anonymous users have separate quotas."""
    quotas: dict[int, int | tuple[Any, ...]] = {1: (2, 5)}  # anon: 2, reg: 5

    rl_anon = rate_limiter.RateLimiter(quotas, is_registered=False)
    rl_reg = rate_limiter.RateLimiter(quotas, is_registered=True)

    # Anonymous: can make 2 calls
    rl_anon.acquire()
    rl_anon.acquire()
    with pytest.raises(RateLimitError):
        rl_anon.acquire()

    # Registered: can make 5 calls
    rl_reg.acquire()
    rl_reg.acquire()
    rl_reg.acquire()
    rl_reg.acquire()
    rl_reg.acquire()
    with pytest.raises(RateLimitError):
        rl_reg.acquire()


@pytest.mark.unit
def test_rate_limiter_edge_case_empty_quotas() -> None:
    """Test edge case with empty quotas dict."""
    quotas: dict[int, int | tuple[Any, ...]] = {}
    rl = rate_limiter.RateLimiter(quotas, is_registered=False)

    # Should not raise - no limits to enforce
    rl.acquire()
    rl.acquire()
    rl.acquire()
