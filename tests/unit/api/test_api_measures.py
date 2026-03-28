from urllib.parse import urlencode

import httpx
import pytest
import respx

from pybdl.api.measures import MeasuresAPI
from pybdl.config import BDLConfig


@pytest.fixture
def measures_api(dummy_config: BDLConfig) -> MeasuresAPI:
    return MeasuresAPI(dummy_config)


@pytest.mark.unit
def test_list_measures(respx_mock: respx.MockRouter, measures_api: MeasuresAPI, api_url: str) -> None:
    url = f"{api_url}/measures?lang=en&format=json&page-size=100"
    payload = {"results": [{"id": 1, "name": "kg"}]}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = measures_api.list_measures()
    assert isinstance(result, list)
    assert result[0]["name"] == "kg"


@pytest.mark.unit
def test_list_measures_with_sort(respx_mock: respx.MockRouter, measures_api: MeasuresAPI, api_url: str) -> None:
    params = {"sort": "Name", "lang": "en", "format": "json", "page-size": "100"}
    url = f"{api_url}/measures?{urlencode(params)}"
    payload = {"results": [{"id": 1, "name": "kg"}]}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    measures_api.list_measures(sort="Name")
    request_url = respx_mock.calls[0].request.url
    assert request_url is not None and "sort=Name" in str(request_url)
    assert "page-size=100" in str(request_url)


@pytest.mark.unit
def test_get_measure(respx_mock: respx.MockRouter, measures_api: MeasuresAPI, api_url: str) -> None:
    url = f"{api_url}/measures/11?lang=en&format=json"
    payload = {"id": 11, "name": "percent"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = measures_api.get_measure(measure_id=11)
    assert result["id"] == 11
    assert result["name"] == "percent"


@pytest.mark.unit
def test_get_measures_metadata(respx_mock: respx.MockRouter, measures_api: MeasuresAPI, api_url: str) -> None:
    url = f"{api_url}/measures/metadata?lang=en&format=json"
    payload = {"version": "1.0"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = measures_api.get_measures_metadata()
    assert result["version"] == "1.0"


@pytest.mark.unit
def test_list_measures_extra_query(respx_mock: respx.MockRouter, measures_api: MeasuresAPI, api_url: str) -> None:
    url = f"{api_url}/measures?foo=bar&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": 1}]}))
    result = measures_api.list_measures(extra_query={"foo": "bar"})
    assert result[0]["id"] == 1


@pytest.mark.unit
def test_get_measure_extra_query(respx_mock: respx.MockRouter, measures_api: MeasuresAPI, api_url: str) -> None:
    url = f"{api_url}/measures/5?foo=bar&lang=en&format=json"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"id": 5}))
    result = measures_api.get_measure(measure_id=5, extra_query={"foo": "bar"})
    assert result["id"] == 5


class DummyException(Exception):
    pass


@pytest.mark.unit
def test_list_measures_error(measures_api: MeasuresAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    measures_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        measures_api.list_measures()


@pytest.mark.unit
def test_get_measure_error(measures_api: MeasuresAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    measures_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        measures_api.get_measure(3)


@pytest.mark.unit
def test_get_measures_metadata_error(measures_api: MeasuresAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    measures_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        measures_api.get_measures_metadata()
