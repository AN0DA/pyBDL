"""Tests for the new utils.rate_limiter package surface."""

import pytest


@pytest.mark.unit
def test_utils_rate_limiter_public_api_imports() -> None:
    from pybdl.utils.rate_limiter import (
        AsyncRateLimiter,
        PersistentQuotaCache,
        RateLimiter,
        async_rate_limit,
        rate_limit,
    )

    assert RateLimiter is not None
    assert AsyncRateLimiter is not None
    assert PersistentQuotaCache is not None
    assert rate_limit is not None
    assert async_rate_limit is not None


@pytest.mark.unit
def test_try_record_all_periods_is_atomic() -> None:
    from pybdl.utils.rate_limiter import PersistentQuotaCache

    cache = PersistentQuotaCache(enabled=True)
    cache._data = {
        "anon_1": [1.0],
        "anon_60": [],
    }

    recorded = cache.try_record_all_periods(
        [
            ("anon_1", 1, 0.0),
            ("anon_60", 5, 0.0),
        ],
        2.0,
    )

    assert recorded is False
    assert cache.get("anon_1") == [1.0]
    assert cache.get("anon_60") == []
