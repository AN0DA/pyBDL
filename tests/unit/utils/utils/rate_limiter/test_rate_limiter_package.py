"""Tests for the new utils.rate_limiter package surface."""

import time
from typing import Any

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
def test_try_record_all_periods_is_atomic(tmp_path: Any) -> None:
    from pybdl.utils.rate_limiter import PersistentQuotaCache

    cache = PersistentQuotaCache(enabled=True, cache_file=tmp_path / "quota_cache.json")
    now = time.time()
    cache._data = {
        "anon_1": [now - 0.1],
        "anon_60": [now - 30] * 5,
    }

    recorded = cache.try_record_all_periods(
        [
            ("anon_1", 1, 1),
            ("anon_60", 5, 60),
        ],
        now,
    )

    assert recorded is False
    assert cache.get("anon_1") == [now - 0.1]
    assert cache.get("anon_60") == [now - 30] * 5
