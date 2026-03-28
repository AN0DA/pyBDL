from typing import Any

import httpx
import pytest
import respx

from pybdl.api.client import BaseAPIClient
from pybdl.api.exceptions import BDLHTTPError, BDLResponseError
from pybdl.config import DEFAULT_QUOTAS, BDLConfig, Language


@pytest.fixture
def base_client(dummy_config: BDLConfig) -> BaseAPIClient:
    """Fixture for BaseAPIClient."""
    return BaseAPIClient(dummy_config)


@pytest.mark.unit
def test_rate_limiter_raise_on_limit_follows_config(dummy_config: BDLConfig) -> None:
    client = BaseAPIClient(dummy_config)
    assert client._sync_limiter.raise_on_limit is False
    assert client._async_limiter.raise_on_limit is False

    strict_config = BDLConfig(
        api_key="dummy-api-key",
        language=Language.EN,
        use_cache=False,
        cache_expire_after=100,
        raise_on_rate_limit=True,
    )
    strict_client = BaseAPIClient(strict_config)
    assert strict_client._sync_limiter.raise_on_limit is True
    assert strict_client._async_limiter.raise_on_limit is True


@pytest.mark.unit
def test_build_url(base_client: BaseAPIClient) -> None:
    assert base_client._build_url("data/xyz") == "https://bdl.stat.gov.pl/api/v1/data/xyz"
    assert base_client._build_url("/data/xyz/") == "https://bdl.stat.gov.pl/api/v1/data/xyz"


@pytest.mark.unit
def test_make_request_success(respx_mock: respx.MockRouter, base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/test"
    url = f"{api_url}/data/test"
    expected = {"results": [{"id": 1}, {"id": 2}, {"id": 3}]}
    respx_mock.get(url + "?lang=en").mock(return_value=httpx.Response(200, json=expected))
    result = base_client._request_sync(endpoint)
    assert result == expected


@pytest.mark.unit
def test_make_request_includes_language(respx_mock: respx.MockRouter, base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/lang"
    url = f"{api_url}/data/lang"
    respx_mock.get(url + "?lang=en").mock(return_value=httpx.Response(200, json={"results": []}))
    base_client._request_sync(endpoint)
    request_url = respx_mock.calls[0].request.url
    assert request_url is not None and "lang=en" in str(request_url)


@pytest.mark.unit
def test_make_request_with_params(respx_mock: respx.MockRouter, base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/params"
    url = f"{api_url}/data/params"
    full_url = url + "?foo=bar&lang=en"
    respx_mock.get(full_url).mock(return_value=httpx.Response(200, json={"results": []}))
    base_client._request_sync(endpoint, params={"foo": "bar"})
    request_url = respx_mock.calls[0].request.url
    assert request_url is not None and "foo=bar" in str(request_url)


@pytest.mark.unit
def test_make_request_http_error(respx_mock: respx.MockRouter, base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/fail"
    url = f"{api_url}/data/fail?lang=en"
    respx_mock.get(url).mock(return_value=httpx.Response(404, json={"detail": "Not found"}))
    with pytest.raises(BDLHTTPError) as excinfo:
        base_client._request_sync(endpoint)
    assert excinfo.value.status_code == 404


@pytest.mark.unit
def test_make_request_api_error_field(respx_mock: respx.MockRouter, base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/api_error"
    url = f"{api_url}/data/api_error?lang=en"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"error": "Oops"}))
    with pytest.raises(BDLResponseError) as excinfo:
        base_client._request_sync(endpoint)
    assert excinfo.value.payload == {"error": "Oops"}


@pytest.mark.unit
def test_make_request_with_headers(respx_mock: respx.MockRouter, base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/headers"
    url = f"{api_url}/data/headers?lang=en"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": []}))
    base_client._request_sync(endpoint, headers={"X-Test-Header": "foo"})
    req_headers = respx_mock.calls[0].request.headers
    assert req_headers["X-Test-Header"] == "foo"
    assert req_headers["X-ClientId"] == "dummy-api-key"


@pytest.mark.unit
def test_paginated_request_all_pages(respx_mock: respx.MockRouter, base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/paged"
    url = f"{api_url}/data/paged"
    # Page 0
    url0 = url + "?lang=en&page-size=2"
    url1 = url + "?lang=en&page-size=2&page=1"
    respx_mock.get(url0).mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"id": 1}, {"id": 2}],
                "totalRecords": 4,
                "links": {"next": url1},
            },
        )
    )
    # Page 1 (last): links has navigation fields but no 'next'
    respx_mock.get(url1).mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"id": 3}, {"id": 4}],
                "totalRecords": 4,
                "links": {
                    "first": url0,
                    "prev": url0,
                    "self": url1,
                    "last": url1,
                },
            },
        )
    )
    pages = list(base_client._paginated_request_sync(endpoint, results_key="results", page_size=2, return_all=True))
    assert len(pages) == 2
    assert pages[0]["results"] == [{"id": 1}, {"id": 2}]
    assert pages[1]["results"] == [{"id": 3}, {"id": 4}]
    # Ensure the correct URLs were called
    assert respx_mock.calls[0].request.url is not None
    assert str(respx_mock.calls[0].request.url).startswith(url0)
    assert respx_mock.calls[1].request.url is not None
    assert str(respx_mock.calls[1].request.url).startswith(url1)


@pytest.mark.unit
def test_fetch_all_results(respx_mock: respx.MockRouter, base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/paged"
    url = f"{api_url}/data/paged"
    url0 = url + "?lang=en&page-size=2"
    url1 = url + "?lang=en&page-size=2&page=1"
    respx_mock.get(url0).mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"id": 1}, {"id": 2}],
                "totalRecords": 3,
                "links": {"next": url1},
            },
        )
    )
    respx_mock.get(url1).mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"id": 3}],
                "totalRecords": 3,
                "links": {
                    "first": url0,
                    "prev": url0,
                    "self": url1,
                    "last": url1,
                },
            },
        )
    )
    results = base_client.fetch_all_results(endpoint, results_key="results", page_size=2)
    assert results == [{"id": 1}, {"id": 2}, {"id": 3}]
    # Ensure the correct URLs were called
    assert respx_mock.calls[0].request.url is not None
    assert str(respx_mock.calls[0].request.url).startswith(url0)
    assert respx_mock.calls[1].request.url is not None
    assert str(respx_mock.calls[1].request.url).startswith(url1)


@pytest.mark.unit
def test_client_with_proxy() -> None:
    config = BDLConfig(api_key="dummy-api-key", proxy_url="http://proxy.example.com:8080")
    client = BaseAPIClient(config)
    assert client._proxy_url == "http://proxy.example.com:8080"


@pytest.mark.unit
def test_client_with_authenticated_proxy() -> None:
    config = BDLConfig(
        api_key="dummy-api-key", proxy_url="http://proxy.example.com:8080", proxy_username="user", proxy_password="pass"
    )
    client = BaseAPIClient(config)
    expected_proxy = "http://user:pass@proxy.example.com:8080"
    assert client._proxy_url == expected_proxy


@pytest.mark.unit
def test_make_request_with_proxy(respx_mock: respx.MockRouter, base_client: BaseAPIClient, api_url: str) -> None:
    # Configure client with proxy
    config = BDLConfig(
        api_key="dummy-api-key",
        proxy_url="http://proxy.example.com:8080",
        language=Language.EN,
        use_cache=False,
    )
    client = BaseAPIClient(config)

    endpoint = "data/proxy"
    url = f"{api_url}/data/proxy?lang=en"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": []}))

    client._request_sync(endpoint)
    assert client._proxy_url == "http://proxy.example.com:8080"
    assert str(respx_mock.calls[0].request.url) == url


@pytest.mark.unit
def test_fetch_all_results_return_metadata(respx_mock: respx.MockRouter, base_client: BaseAPIClient) -> None:
    endpoint = "data/meta"
    url = "https://bdl.stat.gov.pl/api/v1/data/meta"
    url0 = url + "?lang=en&page-size=2"
    url1 = url + "?lang=en&page-size=2&page=1"
    respx_mock.get(url0).mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"id": 1}],
                "totalRecords": 2,
                "meta": {"foo": "bar"},
                "links": {"next": url1},
            },
        )
    )
    respx_mock.get(url1).mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [{"id": 2}],
                "totalRecords": 2,
                "meta": {"foo": "baz"},
                "links": {},
            },
        )
    )
    results, metadata = base_client.fetch_all_results(
        endpoint, results_key="results", page_size=2, return_metadata=True
    )
    assert results == [{"id": 1}, {"id": 2}]
    assert metadata == {"meta": {"foo": "bar"}, "totalRecords": 2}


@pytest.mark.unit
def test_fetch_all_results_missing_results_key_raises(respx_mock: respx.MockRouter, base_client: BaseAPIClient) -> None:
    endpoint = "data/bad"
    url = "https://bdl.stat.gov.pl/api/v1/data/bad?lang=en&page-size=2"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"notresults": []}))
    with pytest.raises(BDLResponseError):
        base_client.fetch_all_results(endpoint, results_key="results", page_size=2)


@pytest.mark.unit
def test_extra_headers_and_none_values(respx_mock: respx.MockRouter) -> None:
    config = BDLConfig(api_key="dummy-api-key", use_cache=False)
    # All values must be str for headers
    client = BaseAPIClient(config, extra_headers={"X-Int": "123", "X-None": ""})
    endpoint = "data/headers"
    url = "https://bdl.stat.gov.pl/api/v1/data/headers?lang=en"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": []}))
    client._request_sync(endpoint)
    req_headers = respx_mock.calls[0].request.headers
    assert req_headers["X-Int"] == "123"
    assert "X-None" in req_headers  # Now present as empty string


@pytest.mark.unit
def test_process_response_text_fallback(monkeypatch: Any, base_client: BaseAPIClient) -> None:
    response = httpx.Response(
        500,
        text="plain text error",
        request=httpx.Request("GET", "https://bdl.stat.gov.pl/api/v1/data/fail"),
    )

    def _bad_json() -> Any:
        raise ValueError("bad json")

    monkeypatch.setattr(response, "json", _bad_json)
    with pytest.raises(BDLHTTPError) as e:
        base_client._process_response(response)
    assert "plain text error" in str(e.value)


@pytest.mark.unit
def test_paginated_request_sync_missing_results_key(respx_mock: respx.MockRouter, base_client: BaseAPIClient) -> None:
    endpoint = "data/badpage"
    url = "https://bdl.stat.gov.pl/api/v1/data/badpage?lang=en&page-size=2"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"notresults": []}))
    it = base_client._paginated_request_sync(endpoint, results_key="results", page_size=2)
    with pytest.raises(BDLResponseError):
        next(it)


@pytest.mark.unit
def test_paginated_request_sync_progress_bar(
    monkeypatch: Any, respx_mock: respx.MockRouter, base_client: BaseAPIClient
) -> None:
    class DummyBar:
        def __init__(self, *a: Any, **k: Any) -> None:
            self.total: int | None = None
            self.closed = False

        def update(self, n: int) -> None:
            self.total = n

        def set_postfix(self, d: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr("pybdl.api.client.tqdm", DummyBar)
    endpoint = "data/progress"
    url = "https://bdl.stat.gov.pl/api/v1/data/progress?lang=en&page-size=2"
    respx_mock.get(url).mock(
        return_value=httpx.Response(200, json={"results": [{"id": 1}], "totalCount": 1, "links": {}})
    )
    results = base_client.fetch_all_results(endpoint, results_key="results", page_size=2, show_progress=True)
    assert results == [{"id": 1}]


@pytest.mark.unit
def test_fetch_single_result_metadata_and_error(respx_mock: respx.MockRouter, base_client: BaseAPIClient) -> None:
    endpoint = "data/single"
    url = "https://bdl.stat.gov.pl/api/v1/data/single?lang=en"
    route = respx_mock.get(url).mock(
        return_value=httpx.Response(200, json={"results": [{"id": 1}], "meta": {"foo": "bar"}})
    )
    # With metadata
    results, meta = base_client.fetch_single_result(endpoint, results_key="results", return_metadata=True)
    assert results == [{"id": 1}]
    assert meta == {"meta": {"foo": "bar"}}
    # Without metadata
    results2 = base_client.fetch_single_result(endpoint, results_key="results")
    assert results2 == [{"id": 1}]
    # Missing results_key
    route.mock(return_value=httpx.Response(200, json={"notresults": []}))
    with pytest.raises(BDLResponseError):
        base_client.fetch_single_result(endpoint, results_key="results")


@pytest.mark.unit
def test_client_anonymous_user_quotas() -> None:
    """Test that anonymous user (None api_key) uses anonymous quotas."""
    config = BDLConfig(api_key=None)
    client = BaseAPIClient(config)

    # Anonymous user should use anonymous limits from DEFAULT_QUOTAS
    assert client._sync_limiter.is_registered is False
    assert client._async_limiter.is_registered is False

    # Check that quotas are DEFAULT_QUOTAS (tuple format)
    assert client._sync_limiter.quotas == DEFAULT_QUOTAS
    assert client._async_limiter.quotas == DEFAULT_QUOTAS

    # Verify anonymous limit is used (first element of tuple)
    period_1s = 1
    limit = client._sync_limiter._get_limit(period_1s)
    entry = DEFAULT_QUOTAS[period_1s]
    assert isinstance(entry, tuple)
    assert limit == entry[0]  # Anonymous limit


@pytest.mark.unit
def test_client_registered_user_quotas() -> None:
    """Test that registered user (with api_key) uses registered quotas."""
    config = BDLConfig(api_key="test-api-key")
    client = BaseAPIClient(config)

    # Registered user should use registered limits
    assert client._sync_limiter.is_registered is True
    assert client._async_limiter.is_registered is True

    # Check that quotas are DEFAULT_QUOTAS (tuple format)
    assert client._sync_limiter.quotas == DEFAULT_QUOTAS
    assert client._async_limiter.quotas == DEFAULT_QUOTAS

    # Verify registered limit is used (second element of tuple)
    period_1s = 1
    limit = client._sync_limiter._get_limit(period_1s)
    entry = DEFAULT_QUOTAS[period_1s]
    assert isinstance(entry, tuple)
    assert limit == entry[1]  # Registered limit


@pytest.mark.unit
def test_client_custom_quotas_override() -> None:
    """Test that custom_quotas override DEFAULT_QUOTAS."""
    custom_quotas = {1: 20, 900: 300}
    config = BDLConfig(api_key="test-api-key", custom_quotas=custom_quotas)
    client = BaseAPIClient(config)

    # Custom quotas should be used directly (single int values)
    assert client._sync_limiter.quotas == custom_quotas
    assert client._async_limiter.quotas == custom_quotas

    # Verify custom limit is used
    limit = client._sync_limiter._get_limit(1)
    assert limit == 20


@pytest.mark.unit
def test_client_separate_limiters_for_registered_and_anonymous() -> None:
    """Test that registered and anonymous users get separate limiter instances."""

    # Create anonymous client
    config_anon = BDLConfig(api_key=None)
    client_anon = BaseAPIClient(config_anon)

    # Create registered client
    config_reg = BDLConfig(api_key="test-key")
    client_reg = BaseAPIClient(config_reg)

    # They should have different limiters
    assert client_anon._sync_limiter is not client_reg._sync_limiter
    assert client_anon._async_limiter is not client_reg._async_limiter

    # New clients also get isolated limiter instances.
    client_anon2 = BaseAPIClient(config_anon)
    assert client_anon._sync_limiter is not client_anon2._sync_limiter
    assert client_anon._async_limiter is not client_anon2._async_limiter


@pytest.mark.unit
def test_make_request_retries_transient_http_errors(
    respx_mock: respx.MockRouter, base_client: BaseAPIClient, api_url: str
) -> None:
    endpoint = "data/retry"
    url = f"{api_url}/data/retry?lang=en"
    respx_mock.get(url).mock(
        side_effect=[
            httpx.Response(503, json={"detail": "temporary"}),
            httpx.Response(200, json={"results": [{"id": 1}]}),
        ]
    )

    result = base_client._request_sync(endpoint)

    assert result == {"results": [{"id": 1}]}
    assert len(respx_mock.calls) == 2


@pytest.mark.unit
def test_make_request_retries_http_429_then_success(
    respx_mock: respx.MockRouter, monkeypatch: pytest.MonkeyPatch, api_url: str
) -> None:
    monkeypatch.setattr("time.sleep", lambda _s: None)
    config = BDLConfig(api_key="dummy-api-key", language=Language.EN, use_cache=False, cache_expire_after=100)
    client = BaseAPIClient(config)
    endpoint = "data/retry429"
    url = f"{api_url}/data/retry429?lang=en"
    respx_mock.get(url).mock(
        side_effect=[
            httpx.Response(429, json={"errorResult": "quota"}, headers={"Retry-After": "1"}),
            httpx.Response(200, json={"results": [{"id": 1}]}),
        ]
    )

    result = client._request_sync(endpoint)

    assert result == {"results": [{"id": 1}]}
    assert len(respx_mock.calls) == 2


@pytest.mark.unit
def test_retry_delay_after_429_exponential_without_retry_after(dummy_config: BDLConfig) -> None:
    client = BaseAPIClient(dummy_config)
    resp = httpx.Response(429, json={"error": "x"}, headers={})
    d0 = client._retry_delay_after_429(0, resp)
    d1 = client._retry_delay_after_429(1, resp)
    d2 = client._retry_delay_after_429(2, resp)
    assert d0 == pytest.approx(dummy_config.retry_backoff_factor)
    assert d1 == pytest.approx(dummy_config.retry_backoff_factor * 2)
    assert d2 == pytest.approx(dummy_config.retry_backoff_factor * 4)


@pytest.mark.unit
def test_make_request_http_429_exhausts_dedicated_budget(
    respx_mock: respx.MockRouter, monkeypatch: pytest.MonkeyPatch, api_url: str
) -> None:
    monkeypatch.setattr("time.sleep", lambda _s: None)
    config = BDLConfig(
        api_key="dummy-api-key",
        language=Language.EN,
        use_cache=False,
        cache_expire_after=100,
        http_429_max_retries=2,
    )
    client = BaseAPIClient(config)
    endpoint = "data/retry429ex"
    url = f"{api_url}/data/retry429ex?lang=en"
    respx_mock.get(url).mock(return_value=httpx.Response(429, json={"errorResult": "quota"}))

    with pytest.raises(BDLHTTPError) as excinfo:
        client._request_sync(endpoint)

    assert excinfo.value.status_code == 429
    assert len(respx_mock.calls) == 3


@pytest.mark.unit
def test_client_anonymous_no_api_key_header() -> None:
    """Test that anonymous user doesn't send X-ClientId header."""
    config = BDLConfig(api_key=None)
    client = BaseAPIClient(config)

    # X-ClientId header should not be present
    assert "X-ClientId" not in client.session.headers


@pytest.mark.unit
def test_client_registered_has_api_key_header() -> None:
    """Test that registered user sends X-ClientId header."""
    api_key = "test-api-key-123"
    config = BDLConfig(api_key=api_key)
    client = BaseAPIClient(config)

    # X-ClientId header should be present with the api_key
    assert client.session.headers["X-ClientId"] == api_key
