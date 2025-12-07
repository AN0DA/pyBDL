import os
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlencode

import pytest
import responses

from pybdl.config import BDLConfig, Language


@pytest.fixture
def dummy_config() -> BDLConfig:
    """Provide a dummy BDLConfig for testing."""
    return BDLConfig(api_key="dummy-api-key", language=Language.EN, use_cache=False, cache_expire_after=100)


@pytest.fixture
def api_url() -> str:
    return "https://bdl.stat.gov.pl/api/v1"


@pytest.fixture
def mock_api_client() -> MagicMock:
    """Provide a mock API client for testing access classes."""
    return MagicMock()


@pytest.fixture
def api_key() -> str | None:
    """Get API key from environment for e2e tests."""
    return os.getenv("BDL_API_KEY")


def paginated_mock(
    base_url: str, data: list[dict[str, Any]], page_size: int = 100, extra_params: dict[str, Any] | None = None
) -> None:
    """
    Mocks two paginated responses using the `responses` library:
    - First page returns the supplied data and a links.next to the next page
    - Second page returns an empty result list and a links object with navigation fields but no next
    Accepts extra_params dict for additional query params (e.g. lang).
    """
    params = extra_params.copy() if extra_params else {}
    params["page-size"] = str(page_size)
    params["lang"] = "en"
    params["format"] = "json"

    # First page: no 'page' param
    url_0 = f"{base_url}?{urlencode(params)}"
    params_next = params.copy()
    params_next["page"] = "1"
    url_1 = f"{base_url}?{urlencode(params_next)}"
    responses.add(
        responses.GET,
        url_0,
        json={
            "results": data,
            "totalRecords": len(data) + 1,
            "links": {"next": url_1},
        },
        status=200,
    )
    # Second page: with 'page=1', links has navigation fields but no 'next'
    responses.add(
        responses.GET,
        url_1,
        json={
            "results": [],
            "links": {
                "first": url_0,
                "prev": url_0,
                "self": url_1,
                "last": url_1,
            },
        },
        status=200,
    )
