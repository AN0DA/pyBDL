from urllib.parse import urlencode

import pytest
import responses

from pyldb.api.levels import LevelsAPI
from pyldb.config import LDBConfig


@pytest.fixture
def levels_api(dummy_config: LDBConfig) -> LevelsAPI:
    return LevelsAPI(dummy_config)


@pytest.mark.unit
@responses.activate
def test_list_levels(levels_api: LevelsAPI, api_url: str) -> None:
    url = f"{api_url}/levels?lang=en&format=json&page-size=100"
    payload = {"results": [{"id": 1, "name": "Country"}, {"id": 2, "name": "Region"}]}
    responses.add(responses.GET, url, json=payload, status=200)
    # Also register page 1 as empty for pagination to finish (if paginated)
    url1 = f"{api_url}/levels?lang=en&format=json&page=1&page-size=100"
    responses.add(responses.GET, url1, json={"results": []}, status=200)
    result = levels_api.list_levels()
    assert isinstance(result, list)
    assert any(r["name"] == "Country" for r in result)
    called_url = responses.calls[0].request.url
    assert called_url is not None and "lang=en" in called_url
    assert "page-size=100" in called_url


@pytest.mark.unit
@responses.activate
def test_list_levels_with_sort(levels_api: LevelsAPI, api_url: str) -> None:
    # The first request will be just with sort and lang
    params = {"sort": "Name", "lang": "en", "format": "json", "page-size": "100"}
    url = f"{api_url}/levels?{urlencode(params)}"
    responses.add(responses.GET, url, json={"results": []}, status=200)
    # Also register page 1 as empty (if paginated)
    url1 = f"{api_url}/levels?{urlencode({**params, 'page': '1'})}"
    responses.add(responses.GET, url1, json={"results": []}, status=200)
    levels_api.list_levels(sort="Name")
    called_url = responses.calls[0].request.url
    assert called_url is not None
    assert "sort=Name" in called_url
    assert "lang=en" in called_url
    assert "page-size=100" in called_url


@pytest.mark.unit
@responses.activate
def test_get_level(levels_api: LevelsAPI, api_url: str) -> None:
    url = f"{api_url}/levels/3?lang=en&format=json"
    payload = {"id": 3, "name": "Powiat"}
    responses.add(responses.GET, url, json=payload, status=200)
    result = levels_api.get_level(level_id=3)
    assert result["id"] == 3
    assert result["name"] == "Powiat"


@pytest.mark.unit
@responses.activate
def test_get_levels_metadata(levels_api: LevelsAPI, api_url: str) -> None:
    url = f"{api_url}/levels/metadata?lang=en&format=json"
    payload = {"version": "1.0"}
    responses.add(responses.GET, url, json=payload, status=200)
    result = levels_api.get_levels_metadata()
    assert result["version"] == "1.0"


@pytest.mark.unit
@responses.activate
def test_list_levels_extra_query(levels_api: LevelsAPI, api_url: str) -> None:
    url = f"{api_url}/levels?foo=bar&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": 1}]}, status=200)
    result = levels_api.list_levels(extra_query={"foo": "bar"})
    assert result[0]["id"] == 1


@pytest.mark.unit
@responses.activate
def test_get_level_extra_query(levels_api: LevelsAPI, api_url: str) -> None:
    url = f"{api_url}/levels/5?foo=bar&lang=en&format=json"
    responses.add(responses.GET, url, json={"id": 5}, status=200)
    result = levels_api.get_level(level_id=5, extra_query={"foo": "bar"})
    assert result["id"] == 5


class DummyException(Exception):
    pass


@pytest.mark.unit
@responses.activate
def test_list_levels_error(levels_api: LevelsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    levels_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        levels_api.list_levels()


@pytest.mark.unit
@responses.activate
def test_get_level_error(levels_api: LevelsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    levels_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        levels_api.get_level(3)


@pytest.mark.unit
@responses.activate
def test_get_levels_metadata_error(levels_api: LevelsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    levels_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        levels_api.get_levels_metadata()


@pytest.mark.unit
@responses.activate
def test_list_levels_http_error(levels_api: LevelsAPI, api_url: str) -> None:
    """Test handling of HTTP errors."""
    url = f"{api_url}/levels?lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"error": "Not Found"}, status=404)
    with pytest.raises(RuntimeError):  # Should raise RuntimeError for HTTP error
        levels_api.list_levels()


@pytest.mark.unit
@responses.activate
def test_get_level_invalid_id(levels_api: LevelsAPI, api_url: str) -> None:
    """Test handling of invalid level ID."""
    url = f"{api_url}/levels/99999?lang=en&format=json"
    responses.add(responses.GET, url, json={"error": "Not Found"}, status=404)
    with pytest.raises(RuntimeError):
        levels_api.get_level(99999)


@pytest.mark.unit
@responses.activate
def test_list_levels_empty_pagination(levels_api: LevelsAPI, api_url: str) -> None:
    """Test handling of empty pagination results."""
    url = f"{api_url}/levels?lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [], "links": {}}, status=200)
    result = levels_api.list_levels()
    assert result == []


@pytest.mark.unit
@responses.activate
def test_get_level_malformed_response(levels_api: LevelsAPI, api_url: str) -> None:
    """Test handling of malformed JSON response."""
    url = f"{api_url}/levels/3?lang=en&format=json"
    responses.add(responses.GET, url, body="not json", status=200, content_type="text/plain")
    with pytest.raises(ValueError):  # Should raise ValueError for malformed JSON
        levels_api.get_level(3)
