from collections.abc import Generator
from typing import Any
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def disable_rate_limiting() -> Generator[Any, Any, Any]:
    """Globally disable rate limiting for all tests."""
    with (
        patch("pyldb.api.utils.rate_limiter.RateLimiter.acquire", lambda self: None),
        patch("pyldb.api.utils.rate_limiter.AsyncRateLimiter.acquire", new=lambda self: None),
    ):
        yield
