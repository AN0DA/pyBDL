from urllib.parse import urlencode

import httpx
import pytest
import respx

from pybdl.api.attributes import AttributesAPI
from pybdl.config import BDLConfig
from tests.conftest import paginated_mock


@pytest.fixture
def attributes_api(dummy_config: BDLConfig) -> AttributesAPI:
    return AttributesAPI(dummy_config)


@pytest.mark.unit
def test_list_attributes(respx_mock: respx.MockRouter, attributes_api: AttributesAPI, api_url: str) -> None:
    url = f"{api_url}/attributes"
    paginated_mock(respx_mock, url, [{"id": 1, "name": "Attr1"}])
    result = attributes_api.list_attributes()
    assert isinstance(result, list)
    assert result[0]["name"] == "Attr1"


@pytest.mark.unit
def test_list_attributes_with_variable_id(
    respx_mock: respx.MockRouter, attributes_api: AttributesAPI, api_url: str
) -> None:
    base_url = f"{api_url}/attributes"
    query = urlencode({"lang": "en", "format": "json", "page-size": "100"})
    url = f"{base_url}?{query}"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": []}))
    attributes_api.list_attributes()

    called_url = respx_mock.calls[0].request.url
    assert called_url is not None
    assert "lang=en" in str(called_url)
    assert "page-size=100" in str(called_url)


@pytest.mark.unit
def test_get_attribute(respx_mock: respx.MockRouter, attributes_api: AttributesAPI, api_url: str) -> None:
    url = f"{api_url}/attributes/7?lang=en&format=json"
    expected = {"id": 7, "name": "Attr7"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=expected))
    result = attributes_api.get_attribute(attribute_id="7")
    assert result["id"] == 7


@pytest.mark.unit
def test_get_attributes_metadata(respx_mock: respx.MockRouter, attributes_api: AttributesAPI, api_url: str) -> None:
    url = f"{api_url}/attributes/metadata?lang=en&format=json"
    expected = {"info": "Metadata"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=expected))
    result = attributes_api.get_attributes_metadata()
    assert result["info"] == "Metadata"


@pytest.mark.unit
def test_list_params_sort_and_extra_query() -> None:
    assert AttributesAPI._list_params(None, None) == {}
    assert AttributesAPI._list_params("name", None) == {"sort": "name"}
    assert AttributesAPI._list_params(None, {"a": "b"}) == {"a": "b"}
    assert AttributesAPI._list_params("id", {"c": "d"}) == {"sort": "id", "c": "d"}
