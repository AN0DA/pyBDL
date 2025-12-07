"""
Rate limiting utilities for pyBDL API client.

This module provides thread-safe rate limiting for both synchronous and asynchronous
API requests. It enforces multiple quota periods simultaneously and supports persistent
quota tracking across process restarts.

Key Components:
    - RateLimiter: Thread-safe synchronous rate limiter
    - AsyncRateLimiter: Asyncio-compatible asynchronous rate limiter
    - PersistentQuotaCache: Thread-safe persistent storage for quota usage
    - rate_limit: Decorator for rate-limiting synchronous functions
    - async_rate_limit: Decorator for rate-limiting asynchronous functions

Exceptions:
    - GUSBDLError: Base exception for all GUS BDL API errors
    - RateLimitError: Raised when rate limit is exceeded
    - RateLimitDelayExceeded: Raised when required delay exceeds max_delay

Example:
    Basic usage with automatic rate limiting::

        from pybdl import BDL, BDLConfig
        bdl = BDL(BDLConfig(api_key="your-api-key"))
        data = bdl.api.data.get_data_by_variable(variable_id="3643", year=2021)

    Using a custom rate limiter with wait behavior::

        from pybdl.api.utils.rate_limiter import RateLimiter, PersistentQuotaCache
        from pybdl.config import DEFAULT_QUOTAS

        cache = PersistentQuotaCache(enabled=True)
        quotas = {k: v[1] for k, v in DEFAULT_QUOTAS.items()}
        limiter = RateLimiter(
            quotas=quotas,
            is_registered=True,
            cache=cache,
            raise_on_limit=False,
            max_delay=30.0
        )

        with limiter:
            # Make API call here
            pass

    Using decorators::

        from pybdl.api.utils.rate_limiter import rate_limit

        @rate_limit(quotas={1: 10, 900: 500}, is_registered=True)
        def fetch_data():
            return api_call()

See Also:
    - :doc:`rate_limiting` for comprehensive documentation
    - :doc:`config` for configuration options
"""

# Re-export all public API for backward compatibility
from pybdl.api.exceptions import (
    GUSBDLError,
    RateLimitDelayExceeded,
    RateLimitError,
)
from pybdl.api.utils.quota_cache import PersistentQuotaCache
from pybdl.api.utils.rate_limiter_async import AsyncRateLimiter
from pybdl.api.utils.rate_limiter_decorators import (
    async_rate_limit,
    rate_limit,
)
from pybdl.api.utils.rate_limiter_sync import RateLimiter

__all__ = [
    "GUSBDLError",
    "RateLimitError",
    "RateLimitDelayExceeded",
    "PersistentQuotaCache",
    "RateLimiter",
    "AsyncRateLimiter",
    "rate_limit",
    "async_rate_limit",
]
