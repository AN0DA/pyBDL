from collections.abc import AsyncIterator, Generator
from typing import Any
from unittest.mock import patch

import pytest

from pybdl.api.data import DataAPI
from pybdl.config import BDLConfig


@pytest.fixture(autouse=True)
def disable_rate_limiting(request: pytest.FixtureRequest) -> Generator[Any, Any, Any]:
    """Globally disable rate limiting for all tests."""
    if request.node.get_closest_marker("real_rate_limiting"):
        yield
        return

    async def _noop_async_acquire(self: Any) -> None:
        return None

    with (
        patch("pybdl.utils.rate_limiter._sync.RateLimiter.acquire", lambda self: None),
        patch("pybdl.utils.rate_limiter._async.AsyncRateLimiter.acquire", new=_noop_async_acquire),
    ):
        yield


@pytest.fixture
def data_api(dummy_config: BDLConfig) -> Generator[DataAPI, None, None]:
    api = DataAPI(dummy_config)
    yield api
    api.close()


@pytest.fixture
async def data_api_async(dummy_config: BDLConfig) -> AsyncIterator[DataAPI]:
    api = DataAPI(dummy_config)
    yield api
    await api.aclose()
