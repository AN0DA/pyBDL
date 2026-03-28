"""Decorators for rate-limiting functions."""

import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from pybdl.utils.rate_limiter._async import AsyncRateLimiter
from pybdl.utils.rate_limiter._sync import RateLimiter

T = TypeVar("T")


def rate_limit(
    quotas: dict[int, int | tuple[int, int]],
    is_registered: bool,
    **limiter_kwargs: Any,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    limiter = RateLimiter(quotas, is_registered, **limiter_kwargs)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            limiter.acquire()
            return func(*args, **kwargs)

        return wrapper

    return decorator


def async_rate_limit(
    quotas: dict[int, int | tuple[int, int]],
    is_registered: bool,
    **limiter_kwargs: Any,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    limiter = AsyncRateLimiter(quotas, is_registered, **limiter_kwargs)

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            await limiter.acquire()
            return await func(*args, **kwargs)

        return wrapper

    return decorator
