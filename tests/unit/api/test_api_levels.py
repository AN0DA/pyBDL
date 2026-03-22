from urllib.parse import urlencode

import httpx
import pytest
import respx

from pybdl.api.exceptions import BDLHTTPError, BDLResponseError
from pybdl.api.levels import LevelsAPI
from pybdl.config import BDLConfig


@pytest.fixture
def levels_api(dummy_config: BDLConfig) -> LevelsAPI:
    return LevelsAPI(dummy_config)


@pytest.mark.unit
def test_list_levels(respx_mock: respx.MockRouter, levels_api: LevelsAPI, api_url: str) -> None:
    url = f"{api_url}/levels?lang=en&format=json&page-size=100"
    payload = {"results": [{"id": 1, "name": "Country"}, {"id": 2, "name": "Region"}]}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    # Also register page 1 as empty for pagination to finish (if paginated)
    url1 = f"{api_url}/levels?lang=en&format=json&page=1&page-size=100"
    respx_mock.get(url1).mock(return_value=httpx.Response(200, json={"results": []}))
    result = levels_api.list_levels()
    assert isinstance(result, list)
    assert any(r["name"] == "Country" for r in result)
    called_url = respx_mock.calls[0].request.url
    assert called_url is not None and "lang=en" in str(called_url)
    assert "page-size=100" in str(called_url)


@pytest.mark.unit
def test_list_levels_with_sort(respx_mock: respx.MockRouter, levels_api: LevelsAPI, api_url: str) -> None:
    # The first request will be just with sort and lang
    params = {"sort": "Name", "lang": "en", "format": "json", "page-size": "100"}
    url = f"{api_url}/levels?{urlencode(params)}"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": []}))
    # Also register page 1 as empty (if paginated)
    url1 = f"{api_url}/levels?{urlencode({**params, 'page': '1'})}"
    respx_mock.get(url1).mock(return_value=httpx.Response(200, json={"results": []}))
    levels_api.list_levels(sort="Name")
    called_url = respx_mock.calls[0].request.url
    assert called_url is not None
    assert "sort=Name" in str(called_url)
    assert "lang=en" in str(called_url)
    assert "page-size=100" in str(called_url)


@pytest.mark.unit
def test_get_level(respx_mock: respx.MockRouter, levels_api: LevelsAPI, api_url: str) -> None:
    url = f"{api_url}/levels/3?lang=en&format=json"
    payload = {"id": 3, "name": "Powiat"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = levels_api.get_level(level_id=3)
    assert result["id"] == 3
    assert result["name"] == "Powiat"


@pytest.mark.unit
def test_get_levels_metadata(respx_mock: respx.MockRouter, levels_api: LevelsAPI, api_url: str) -> None:
    url = f"{api_url}/levels/metadata?lang=en&format=json"
    payload = {"version": "1.0"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = levels_api.get_levels_metadata()
    assert result["version"] == "1.0"


@pytest.mark.unit
def test_list_levels_extra_query(respx_mock: respx.MockRouter, levels_api: LevelsAPI, api_url: str) -> None:
    url = f"{api_url}/levels?foo=bar&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": 1}]}))
    result = levels_api.list_levels(extra_query={"foo": "bar"})
    assert result[0]["id"] == 1


@pytest.mark.unit
def test_get_level_extra_query(respx_mock: respx.MockRouter, levels_api: LevelsAPI, api_url: str) -> None:
    url = f"{api_url}/levels/5?foo=bar&lang=en&format=json"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"id": 5}))
    result = levels_api.get_level(level_id=5, extra_query={"foo": "bar"})
    assert result["id"] == 5


class DummyException(Exception):
    pass


@pytest.mark.unit
def test_list_levels_error(levels_api: LevelsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    levels_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        levels_api.list_levels()


@pytest.mark.unit
def test_get_level_error(levels_api: LevelsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    levels_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        levels_api.get_level(3)


@pytest.mark.unit
def test_get_levels_metadata_error(levels_api: LevelsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    levels_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        levels_api.get_levels_metadata()


@pytest.mark.unit
def test_list_levels_http_error(respx_mock: respx.MockRouter, levels_api: LevelsAPI, api_url: str) -> None:
    """Test handling of HTTP errors."""
    url = f"{api_url}/levels?lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(404, json={"error": "Not Found"}))
    with pytest.raises(BDLHTTPError):
        levels_api.list_levels()


@pytest.mark.unit
def test_get_level_invalid_id(respx_mock: respx.MockRouter, levels_api: LevelsAPI, api_url: str) -> None:
    """Test handling of invalid level ID."""
    url = f"{api_url}/levels/99999?lang=en&format=json"
    respx_mock.get(url).mock(return_value=httpx.Response(404, json={"error": "Not Found"}))
    with pytest.raises(BDLHTTPError):
        levels_api.get_level(99999)


@pytest.mark.unit
def test_list_levels_empty_pagination(respx_mock: respx.MockRouter, levels_api: LevelsAPI, api_url: str) -> None:
    """Test handling of empty pagination results."""
    url = f"{api_url}/levels?lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [], "links": {}}))
    result = levels_api.list_levels()
    assert result == []


@pytest.mark.unit
def test_get_level_malformed_response(respx_mock: respx.MockRouter, levels_api: LevelsAPI, api_url: str) -> None:
    """Test handling of malformed JSON response."""
    url = f"{api_url}/levels/3?lang=en&format=json"
    respx_mock.get(url).mock(
        return_value=httpx.Response(200, content=b"not json", headers={"Content-Type": "text/plain"})
    )
    with pytest.raises(BDLResponseError):
        levels_api.get_level(3)
