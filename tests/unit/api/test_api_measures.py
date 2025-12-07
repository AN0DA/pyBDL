from urllib.parse import urlencode

import pytest
import responses

from pyldb.api.measures import MeasuresAPI
from pyldb.config import LDBConfig


@pytest.fixture
def measures_api(dummy_config: LDBConfig) -> MeasuresAPI:
    return MeasuresAPI(dummy_config)


@responses.activate
@pytest.mark.unit
def test_list_measures(measures_api: MeasuresAPI, api_url: str) -> None:
    url = f"{api_url}/measures?lang=en&format=json&page-size=100"
    payload = {"results": [{"id": 1, "name": "kg"}]}
    responses.add(responses.GET, url, json=payload, status=200)
    result = measures_api.list_measures()
    assert isinstance(result, list)
    assert result[0]["name"] == "kg"


@responses.activate
@pytest.mark.unit
def test_list_measures_with_sort(measures_api: MeasuresAPI, api_url: str) -> None:
    params = {"sort": "Name", "lang": "en", "format": "json", "page-size": "100"}
    url = f"{api_url}/measures?{urlencode(params)}"
    payload = {"results": [{"id": 1, "name": "kg"}]}
    responses.add(responses.GET, url, json=payload, status=200)
    measures_api.list_measures(sort="Name")
    request_url = responses.calls[0].request.url
    assert request_url is not None and "sort=Name" in request_url
    assert "page-size=100" in request_url


@responses.activate
@pytest.mark.unit
def test_get_measure(measures_api: MeasuresAPI, api_url: str) -> None:
    url = f"{api_url}/measures/11?lang=en&format=json"
    payload = {"id": 11, "name": "percent"}
    responses.add(responses.GET, url, json=payload, status=200)
    result = measures_api.get_measure(measure_id=11)
    assert result["id"] == 11
    assert result["name"] == "percent"


@responses.activate
@pytest.mark.unit
def test_get_measures_metadata(measures_api: MeasuresAPI, api_url: str) -> None:
    url = f"{api_url}/measures/metadata?lang=en&format=json"
    payload = {"version": "1.0"}
    responses.add(responses.GET, url, json=payload, status=200)
    result = measures_api.get_measures_metadata()
    assert result["version"] == "1.0"


@responses.activate
@pytest.mark.unit
def test_list_measures_extra_query(measures_api: MeasuresAPI, api_url: str) -> None:
    url = f"{api_url}/measures?foo=bar&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": 1}]}, status=200)
    result = measures_api.list_measures(extra_query={"foo": "bar"})
    assert result[0]["id"] == 1


@responses.activate
@pytest.mark.unit
def test_get_measure_extra_query(measures_api: MeasuresAPI, api_url: str) -> None:
    url = f"{api_url}/measures/5?foo=bar&lang=en&format=json"
    responses.add(responses.GET, url, json={"id": 5}, status=200)
    result = measures_api.get_measure(measure_id=5, extra_query={"foo": "bar"})
    assert result["id"] == 5


class DummyException(Exception):
    pass


@responses.activate
@pytest.mark.unit
def test_list_measures_error(measures_api: MeasuresAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    measures_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        measures_api.list_measures()


@responses.activate
@pytest.mark.unit
def test_get_measure_error(measures_api: MeasuresAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    measures_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        measures_api.get_measure(3)


@responses.activate
@pytest.mark.unit
def test_get_measures_metadata_error(measures_api: MeasuresAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    measures_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        measures_api.get_measures_metadata()
