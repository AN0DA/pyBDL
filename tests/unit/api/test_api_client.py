from typing import Any, cast

import pytest
import responses
from requests import HTTPError, PreparedRequest, Response

from pyldb.api.client import BaseAPIClient
from pyldb.config import DEFAULT_QUOTAS, Language, LDBConfig


# Type for PreparedRequest with req_kwargs added by responses
class ResponsesPreparedRequest(PreparedRequest):
    req_kwargs: dict[str, Any]


@pytest.fixture
def base_client(dummy_config: LDBConfig) -> BaseAPIClient:
    """Fixture for BaseAPIClient."""
    return BaseAPIClient(dummy_config)


@pytest.fixture
def api_url() -> str:
    return "https://bdl.stat.gov.pl/api/v1"


@pytest.mark.unit
def test_build_url(base_client: BaseAPIClient) -> None:
    assert base_client._build_url("data/xyz") == "https://bdl.stat.gov.pl/api/v1/data/xyz"
    assert base_client._build_url("/data/xyz/") == "https://bdl.stat.gov.pl/api/v1/data/xyz"


@responses.activate
@pytest.mark.unit
def test_make_request_success(base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/test"
    url = f"{api_url}/data/test"
    expected = {"results": [{"id": 1}, {"id": 2}, {"id": 3}]}
    responses.add(responses.GET, url + "?lang=en", json=expected, status=200)
    result = base_client._request_sync(endpoint)
    assert result == expected


@responses.activate
@pytest.mark.unit
def test_make_request_includes_language(base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/lang"
    url = f"{api_url}/data/lang"
    responses.add(responses.GET, url + "?lang=en", json={"results": []}, status=200)
    base_client._request_sync(endpoint)
    request_url = responses.calls[0].request.url
    assert request_url is not None and "lang=en" in request_url


@responses.activate
@pytest.mark.unit
def test_make_request_with_params(base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/params"
    url = f"{api_url}/data/params"
    full_url = url + "?foo=bar&lang=en"
    responses.add(responses.GET, full_url, json={"results": []}, status=200)
    base_client._request_sync(endpoint, params={"foo": "bar"})
    request_url = responses.calls[0].request.url
    assert request_url is not None and "foo=bar" in request_url


@responses.activate
@pytest.mark.unit
def test_make_request_http_error(base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/fail"
    url = f"{api_url}/data/fail?lang=en"
    responses.add(responses.GET, url, json={"detail": "Not found"}, status=404)
    with pytest.raises(RuntimeError) as excinfo:
        base_client._request_sync(endpoint)
    assert "HTTP error" in str(excinfo.value)


@responses.activate
@pytest.mark.unit
def test_make_request_api_error_field(base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/api_error"
    url = f"{api_url}/data/api_error?lang=en"
    responses.add(responses.GET, url, json={"error": "Oops"}, status=200)
    with pytest.raises(ValueError) as excinfo:
        base_client._request_sync(endpoint)
    assert "API Error" in str(excinfo.value)


@responses.activate
@pytest.mark.unit
def test_make_request_with_headers(base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/headers"
    url = f"{api_url}/data/headers?lang=en"
    responses.add(responses.GET, url, json={"results": []}, status=200)
    base_client._request_sync(endpoint, headers={"X-Test-Header": "foo"})
    req_headers = responses.calls[0].request.headers
    assert req_headers["X-Test-Header"] == "foo"
    assert req_headers["X-ClientId"] == "dummy-api-key"


@responses.activate
@pytest.mark.unit
def test_paginated_request_all_pages(base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/paged"
    url = f"{api_url}/data/paged"
    # Page 0
    url0 = url + "?lang=en&page-size=2"
    url1 = url + "?lang=en&page-size=2&page=1"
    responses.add(
        responses.GET,
        url0,
        json={
            "results": [{"id": 1}, {"id": 2}],
            "totalRecords": 4,
            "links": {"next": url1},
        },
        status=200,
    )
    # Page 1 (last): links has navigation fields but no 'next'
    responses.add(
        responses.GET,
        url1,
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
        status=200,
    )
    pages = list(base_client._paginated_request_sync(endpoint, results_key="results", page_size=2, return_all=True))
    assert len(pages) == 2
    assert pages[0]["results"] == [{"id": 1}, {"id": 2}]
    assert pages[1]["results"] == [{"id": 3}, {"id": 4}]
    # Ensure the correct URLs were called
    assert responses.calls[0].request.url is not None
    assert responses.calls[0].request.url.startswith(url0)
    assert responses.calls[1].request.url is not None
    assert responses.calls[1].request.url.startswith(url1)


@responses.activate
@pytest.mark.unit
def test_fetch_all_results(base_client: BaseAPIClient, api_url: str) -> None:
    endpoint = "data/paged"
    url = f"{api_url}/data/paged"
    url0 = url + "?lang=en&page-size=2"
    url1 = url + "?lang=en&page-size=2&page=1"
    responses.add(
        responses.GET,
        url0,
        json={
            "results": [{"id": 1}, {"id": 2}],
            "totalRecords": 3,
            "links": {"next": url1},
        },
        status=200,
    )
    responses.add(
        responses.GET,
        url1,
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
        status=200,
    )
    results = base_client.fetch_all_results(endpoint, results_key="results", page_size=2)
    assert results == [{"id": 1}, {"id": 2}, {"id": 3}]
    # Ensure the correct URLs were called
    assert responses.calls[0].request.url is not None
    assert responses.calls[0].request.url.startswith(url0)
    assert responses.calls[1].request.url is not None
    assert responses.calls[1].request.url.startswith(url1)


@pytest.mark.unit
def test_client_with_proxy() -> None:
    config = LDBConfig(api_key="dummy-api-key", proxy_url="http://proxy.example.com:8080")
    client = BaseAPIClient(config)
    assert client.session.proxies["http"] == "http://proxy.example.com:8080"
    assert client.session.proxies["https"] == "http://proxy.example.com:8080"


@pytest.mark.unit
def test_client_with_authenticated_proxy() -> None:
    config = LDBConfig(
        api_key="dummy-api-key", proxy_url="http://proxy.example.com:8080", proxy_username="user", proxy_password="pass"
    )
    client = BaseAPIClient(config)
    expected_proxy = "http://user:pass@proxy.example.com:8080"
    assert client.session.proxies["http"] == expected_proxy
    assert client.session.proxies["https"] == expected_proxy


@responses.activate
@pytest.mark.unit
def test_make_request_with_proxy(base_client: BaseAPIClient, api_url: str) -> None:
    # Configure client with proxy
    config = LDBConfig(
        api_key="dummy-api-key",
        proxy_url="http://proxy.example.com:8080",
        language=Language.EN,
    )
    client = BaseAPIClient(config)

    endpoint = "data/proxy"
    url = f"{api_url}/data/proxy?lang=en"
    responses.add(responses.GET, url, json={"results": []}, status=200)

    client._request_sync(endpoint)
    request = cast(ResponsesPreparedRequest, responses.calls[0].request)
    # Verify proxy settings in the request kwargs
    assert request.req_kwargs["proxies"]["http"] == "http://proxy.example.com:8080"
    assert request.req_kwargs["proxies"]["https"] == "http://proxy.example.com:8080"


@responses.activate
@pytest.mark.unit
def test_fetch_all_results_return_metadata(base_client: BaseAPIClient) -> None:
    endpoint = "data/meta"
    url = "https://bdl.stat.gov.pl/api/v1/data/meta"
    url0 = url + "?lang=en&page-size=2"
    url1 = url + "?lang=en&page-size=2&page=1"
    responses.add(
        responses.GET,
        url0,
        json={
            "results": [{"id": 1}],
            "totalRecords": 2,
            "meta": {"foo": "bar"},
            "links": {"next": url1},
        },
        status=200,
    )
    responses.add(
        responses.GET,
        url1,
        json={
            "results": [{"id": 2}],
            "totalRecords": 2,
            "meta": {"foo": "baz"},
            "links": {},
        },
        status=200,
    )
    results, metadata = base_client.fetch_all_results(
        endpoint, results_key="results", page_size=2, return_metadata=True
    )
    assert results == [{"id": 1}, {"id": 2}]
    assert metadata == {"meta": {"foo": "bar"}, "totalRecords": 2}


@responses.activate
@pytest.mark.unit
def test_fetch_all_results_missing_results_key_raises(base_client: BaseAPIClient) -> None:
    endpoint = "data/bad"
    url = "https://bdl.stat.gov.pl/api/v1/data/bad?lang=en&page-size=2"
    responses.add(responses.GET, url, json={"notresults": []}, status=200)
    with pytest.raises(ValueError):
        base_client.fetch_all_results(endpoint, results_key="results", page_size=2)


@responses.activate
@pytest.mark.unit
def test_extra_headers_and_none_values() -> None:
    config = LDBConfig(api_key="dummy-api-key")
    # All values must be str for headers
    client = BaseAPIClient(config, extra_headers={"X-Int": "123", "X-None": ""})
    endpoint = "data/headers"
    url = "https://bdl.stat.gov.pl/api/v1/data/headers?lang=en"
    responses.add(responses.GET, url, json={"results": []}, status=200)
    client._request_sync(endpoint)
    req_headers = responses.calls[0].request.headers
    assert req_headers["X-Int"] == "123"
    assert "X-None" in req_headers  # Now present as empty string


@responses.activate
@pytest.mark.unit
def test_process_response_text_fallback(monkeypatch: Any, base_client: BaseAPIClient) -> None:
    class DummyResponse(Response):
        def __init__(self) -> None:
            super().__init__()
            self.status_code = 500

        def raise_for_status(self) -> None:
            raise HTTPError("fail", response=self)

        def json(
            self,
            *,
            cls: type | None = None,
            object_hook: Any = None,
            parse_float: Any = None,
            parse_int: Any = None,
            parse_constant: Any = None,
            object_pairs_hook: Any = None,
            **kwds: Any,
        ) -> Any:
            raise Exception("bad json")

        @property
        def text(self) -> str:
            return "plain text error"

    with pytest.raises(RuntimeError) as e:
        base_client._process_response(DummyResponse())
    assert "plain text error" in str(e.value)


@responses.activate
@pytest.mark.unit
def test_paginated_request_sync_missing_results_key(base_client: BaseAPIClient) -> None:
    endpoint = "data/badpage"
    url = "https://bdl.stat.gov.pl/api/v1/data/badpage?lang=en&page-size=2"
    responses.add(responses.GET, url, json={"notresults": []}, status=200)
    it = base_client._paginated_request_sync(endpoint, results_key="results", page_size=2)
    with pytest.raises(ValueError):
        next(it)


@responses.activate
@pytest.mark.unit
def test_paginated_request_sync_progress_bar(monkeypatch: Any, base_client: BaseAPIClient) -> None:
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

    monkeypatch.setattr("pyldb.api.client.tqdm", DummyBar)
    endpoint = "data/progress"
    url = "https://bdl.stat.gov.pl/api/v1/data/progress?lang=en&page-size=2"
    responses.add(responses.GET, url, json={"results": [{"id": 1}], "totalCount": 1, "links": {}}, status=200)
    results = base_client.fetch_all_results(endpoint, results_key="results", page_size=2, show_progress=True)
    assert results == [{"id": 1}]


@responses.activate
@pytest.mark.unit
def test_fetch_single_result_metadata_and_error(base_client: BaseAPIClient) -> None:
    endpoint = "data/single"
    url = "https://bdl.stat.gov.pl/api/v1/data/single?lang=en"
    responses.add(responses.GET, url, json={"results": [{"id": 1}], "meta": {"foo": "bar"}}, status=200)
    # With metadata
    results, meta = base_client.fetch_single_result(endpoint, results_key="results", return_metadata=True)
    assert results == [{"id": 1}]
    assert meta == {"meta": {"foo": "bar"}}
    # Without metadata
    results2 = base_client.fetch_single_result(endpoint, results_key="results")
    assert results2 == [{"id": 1}]
    # Missing results_key
    responses.replace(responses.GET, url, json={"notresults": []}, status=200)
    with pytest.raises(ValueError):
        base_client.fetch_single_result(endpoint, results_key="results")


@pytest.mark.unit
def test_client_anonymous_user_quotas() -> None:
    """Test that anonymous user (None api_key) uses anonymous quotas."""
    config = LDBConfig(api_key=None)
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
    assert limit == DEFAULT_QUOTAS[period_1s][0]  # Anonymous limit


@pytest.mark.unit
def test_client_registered_user_quotas() -> None:
    """Test that registered user (with api_key) uses registered quotas."""
    config = LDBConfig(api_key="test-api-key")
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
    assert limit == DEFAULT_QUOTAS[period_1s][1]  # Registered limit


@pytest.mark.unit
def test_client_custom_quotas_override() -> None:
    """Test that custom_quotas override DEFAULT_QUOTAS."""
    # Clear any existing limiters to ensure fresh limiter creation
    BaseAPIClient._global_sync_limiters.clear()
    BaseAPIClient._global_async_limiters.clear()

    custom_quotas = {1: 20, 900: 300}
    config = LDBConfig(api_key="test-api-key", custom_quotas=custom_quotas)
    client = BaseAPIClient(config)

    # Custom quotas should be used directly (single int values)
    assert client._sync_limiter.quotas == custom_quotas
    assert client._async_limiter.quotas == custom_quotas

    # Verify custom limit is used
    limit = client._sync_limiter._get_limit(1)
    assert limit == 20


@pytest.mark.unit
def test_client_separate_limiters_for_registered_and_anonymous() -> None:
    """Test that registered and anonymous users get separate limiters."""
    # Clear any existing limiters
    BaseAPIClient._global_sync_limiters.clear()
    BaseAPIClient._global_async_limiters.clear()

    # Create anonymous client
    config_anon = LDBConfig(api_key=None)
    client_anon = BaseAPIClient(config_anon)

    # Create registered client
    config_reg = LDBConfig(api_key="test-key")
    client_reg = BaseAPIClient(config_reg)

    # They should have different limiters
    assert client_anon._sync_limiter is not client_reg._sync_limiter
    assert client_anon._async_limiter is not client_reg._async_limiter

    # But same limiters should be reused for same registration status
    client_anon2 = BaseAPIClient(config_anon)
    assert client_anon._sync_limiter is client_anon2._sync_limiter
    assert client_anon._async_limiter is client_anon2._async_limiter


@pytest.mark.unit
def test_client_anonymous_no_api_key_header() -> None:
    """Test that anonymous user doesn't send X-ClientId header."""
    config = LDBConfig(api_key=None)
    client = BaseAPIClient(config)

    # X-ClientId header should not be present
    assert "X-ClientId" not in client.session.headers


@pytest.mark.unit
def test_client_registered_has_api_key_header() -> None:
    """Test that registered user sends X-ClientId header."""
    api_key = "test-api-key-123"
    config = LDBConfig(api_key=api_key)
    client = BaseAPIClient(config)

    # X-ClientId header should be present with the api_key
    assert client.session.headers["X-ClientId"] == api_key
