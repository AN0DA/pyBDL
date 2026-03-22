from collections.abc import Generator
from typing import Any
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def disable_rate_limiting(request: pytest.FixtureRequest) -> Generator[Any, Any, Any]:
    """Globally disable rate limiting for all tests."""
    if request.node.get_closest_marker("real_rate_limiting"):
        yield
        return

    async def _noop_async_acquire(self: Any) -> None:
        return None

    with (
        patch("pybdl.api.utils.rate_limiter.RateLimiter.acquire", lambda self: None),
        patch("pybdl.api.utils.rate_limiter.AsyncRateLimiter.acquire", new=_noop_async_acquire),
    ):
        yield
