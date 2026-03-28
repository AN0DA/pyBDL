from urllib.parse import urlencode

import httpx
import pytest
import respx

from pybdl.api.variables import VariablesAPI
from pybdl.config import BDLConfig
from tests.conftest import paginated_mock


@pytest.fixture
def variables_api(dummy_config: BDLConfig) -> VariablesAPI:
    return VariablesAPI(dummy_config)


@pytest.mark.unit
def test_list_variables(respx_mock: respx.MockRouter, variables_api: VariablesAPI, api_url: str) -> None:
    url = f"{api_url}/variables"
    paginated_mock(respx_mock, url, [{"id": "1", "name": "Population"}])
    result = variables_api.list_variables()
    assert isinstance(result, list)
    assert result[0]["name"] == "Population"


@pytest.mark.unit
def test_list_variables_with_filters(respx_mock: respx.MockRouter, variables_api: VariablesAPI, api_url: str) -> None:
    params = {
        "subject-id": "cat",
        "category-id": "cat",
        "aggregate-id": "agg",
        "name": "pop",
        "sort": "name",
        "lang": "en",
        "format": "json",
        "page-size": "100",
    }
    url = f"{api_url}/variables?{urlencode(params)}"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": []}))
    # Also add page 1 for completeness
    params["page"] = "1"
    url1 = f"{api_url}/variables?{urlencode(params)}"
    respx_mock.get(url1).mock(return_value=httpx.Response(200, json={"results": []}))
    variables_api.list_variables(
        subject_id="cat", sort="name", extra_query={"aggregate-id": "agg", "category-id": "cat", "name": "pop"}
    )
    called_url = respx_mock.calls[0].request.url
    assert called_url is not None
    assert "subject-id=cat" in str(called_url)
    assert "category-id=cat" in str(called_url)
    assert "aggregate-id=agg" in str(called_url)
    assert "name=pop" in str(called_url)
    assert "sort=name" in str(called_url)


@pytest.mark.unit
def test_get_variable(respx_mock: respx.MockRouter, variables_api: VariablesAPI, api_url: str) -> None:
    url = f"{api_url}/variables/1?lang=en&format=json"
    payload = {"id": "1", "name": "Population"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = variables_api.get_variable(variable_id="1")
    assert result["id"] == "1"
    assert result["name"] == "Population"


@pytest.mark.unit
def test_get_variables_metadata(respx_mock: respx.MockRouter, variables_api: VariablesAPI, api_url: str) -> None:
    url = f"{api_url}/variables/metadata?lang=en&format=json"
    payload = {"info": "Variables API"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = variables_api.get_variables_metadata()
    assert result["info"] == "Variables API"


@pytest.mark.unit
def test_list_variables_extra_query(respx_mock: respx.MockRouter, variables_api: VariablesAPI, api_url: str) -> None:
    url = f"{api_url}/variables?foo=bar&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "1"}]}))
    result = variables_api.list_variables(extra_query={"foo": "bar"})
    assert result[0]["id"] == "1"


@pytest.mark.unit
def test_get_variable_extra_query(respx_mock: respx.MockRouter, variables_api: VariablesAPI, api_url: str) -> None:
    url = f"{api_url}/variables/2?foo=bar&lang=en&format=json"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"id": "2"}))
    result = variables_api.get_variable(variable_id="2", extra_query={"foo": "bar"})
    assert result["id"] == "2"


@pytest.mark.unit
def test_search_variables_all_branches(respx_mock: respx.MockRouter, variables_api: VariablesAPI, api_url: str) -> None:
    # max_pages=None (default, all pages)
    url = f"{api_url}/variables/search?name=pop&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "1"}]}))
    result = variables_api.search_variables(name="pop")
    assert result[0]["id"] == "1"
    # max_pages=1 (single page)
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "2"}]}))
    variables_api.fetch_single_result = lambda *a, **k: [{"id": "2"}]  # type: ignore[assignment]
    result = variables_api.search_variables(name="pop", max_pages=1)
    assert result[0]["id"] == "2"


@pytest.mark.unit
def test_search_variables_with_filters(respx_mock: respx.MockRouter, variables_api: VariablesAPI, api_url: str) -> None:
    params = {
        "name": "pop",
        "subject-id": "cat",
        "category-id": "cat",
        "aggregate-id": "agg",
        "sort": "name",
        "foo": "bar",
        "lang": "en",
        "format": "json",
        "page-size": "100",
    }
    url = f"{api_url}/variables/search?{urlencode(params)}"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "3"}]}))
    result = variables_api.search_variables(
        name="pop",
        subject_id="cat",
        sort="name",
        extra_query={"category-id": "cat", "aggregate-id": "agg", "foo": "bar"},
    )
    assert result[0]["id"] == "3"


class DummyException(Exception):
    pass


@pytest.mark.unit
def test_list_variables_error(variables_api: VariablesAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    variables_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        variables_api.list_variables()


@pytest.mark.unit
def test_get_variable_error(variables_api: VariablesAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    variables_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        variables_api.get_variable("1")


@pytest.mark.unit
def test_search_variables_error(variables_api: VariablesAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    variables_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        variables_api.search_variables(name="pop")


@pytest.mark.unit
def test_get_variables_metadata_error(variables_api: VariablesAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    variables_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        variables_api.get_variables_metadata()


@pytest.mark.unit
def test_list_params_level_years_and_page() -> None:
    assert VariablesAPI._list_params(None, None, None, None, None, None) == {}
    p = VariablesAPI._list_params("sub", 3, [2020, 2021], 2, "name", {"f": "g"})
    assert p["subject-id"] == "sub"
    assert p["level"] == 3
    assert p["year"] == [2020, 2021]
    assert p["page"] == 2
    assert p["sort"] == "name"
    assert p["f"] == "g"
