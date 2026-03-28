"""Rate limiting utilities."""

from pybdl.api.exceptions import GUSBDLError, RateLimitDelayExceeded, RateLimitError
from pybdl.utils.rate_limiter._async import AsyncRateLimiter
from pybdl.utils.rate_limiter._cache import PersistentQuotaCache
from pybdl.utils.rate_limiter._decorators import async_rate_limit, rate_limit
from pybdl.utils.rate_limiter._sync import RateLimiter

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
