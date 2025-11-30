"""Decorators for rate-limiting functions."""

import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from pyldb.api.utils.rate_limiter_async import AsyncRateLimiter
from pyldb.api.utils.rate_limiter_sync import RateLimiter

T = TypeVar("T")
R = TypeVar("R")


def rate_limit(
    quotas: dict[int, int | tuple],
    is_registered: bool,
    **limiter_kwargs: Any,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for rate-limiting functions.

    Args:
        quotas: Dictionary of {period_seconds: limit or (anon_limit, reg_limit)}.
        is_registered: Whether the user is registered (affects quota).
        ``**limiter_kwargs``: Additional arguments to pass to RateLimiter (e.g., max_delay, raise_on_limit).

    Returns:
        Decorator function.

    Example::

        @rate_limit(quotas={60: 100}, is_registered=True, max_delay=30)
        def fetch_dataset(dataset_id):
            return api.get(f"/datasets/{dataset_id}")
    """
    limiter = RateLimiter(quotas, is_registered, **limiter_kwargs)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            limiter.acquire()
            return func(*args, **kwargs)

        return wrapper

    return decorator


def async_rate_limit(
    quotas: dict[int, int | tuple],
    is_registered: bool,
    **limiter_kwargs: Any,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """
    Decorator for rate-limiting async functions.

    Args:
        quotas: Dictionary of {period_seconds: limit or (anon_limit, reg_limit)}.
        is_registered: Whether the user is registered (affects quota).
        ``**limiter_kwargs``: Additional arguments to pass to AsyncRateLimiter (e.g., max_delay, raise_on_limit).

    Returns:
        Decorator function.

    Example::

        @async_rate_limit(quotas={60: 100}, is_registered=True)
        async def async_fetch_dataset(dataset_id):
            return await api.get(f"/datasets/{dataset_id}")
    """
    limiter = AsyncRateLimiter(quotas, is_registered, **limiter_kwargs)

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            await limiter.acquire()
            return await func(*args, **kwargs)

        return wrapper

    return decorator
