import httpx
import pytest
import respx

from pybdl.api.aggregates import AggregatesAPI
from pybdl.config import BDLConfig


@pytest.fixture
def aggregates_api(dummy_config: BDLConfig) -> AggregatesAPI:
    return AggregatesAPI(dummy_config)


@pytest.mark.unit
def test_list_aggregates(respx_mock: respx.MockRouter, aggregates_api: AggregatesAPI, api_url: str) -> None:
    url = f"{api_url}/aggregates?lang=en&format=json&page-size=100"
    expected = {"results": [{"id": 1, "name": "Agg1"}]}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=expected))
    result = aggregates_api.list_aggregates()
    assert isinstance(result, list)
    assert result[0]["name"] == "Agg1"


@pytest.mark.unit
def test_list_aggregates_with_sort(respx_mock: respx.MockRouter, aggregates_api: AggregatesAPI, api_url: str) -> None:
    url = f"{api_url}/aggregates?sort=Name&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": []}))
    aggregates_api.list_aggregates(sort="Name")
    called_url = respx_mock.calls[0].request.url
    assert called_url is not None
    assert "sort=Name" in str(called_url)
    assert "lang=en" in str(called_url)
    assert "page-size=100" in str(called_url)


@pytest.mark.unit
def test_list_aggregates_extra_query(respx_mock: respx.MockRouter, aggregates_api: AggregatesAPI, api_url: str) -> None:
    url = f"{api_url}/aggregates?foo=bar&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": 1}]}))
    result = aggregates_api.list_aggregates(extra_query={"foo": "bar"})
    assert result[0]["id"] == 1


@pytest.mark.unit
def test_get_aggregate(respx_mock: respx.MockRouter, aggregates_api: AggregatesAPI, api_url: str) -> None:
    url = f"{api_url}/aggregates/42?lang=en&format=json"
    expected = {"id": 42, "name": "Agg42"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=expected))
    result = aggregates_api.get_aggregate(aggregate_id="42")
    assert result["id"] == 42


@pytest.mark.unit
def test_get_aggregates_metadata(respx_mock: respx.MockRouter, aggregates_api: AggregatesAPI, api_url: str) -> None:
    url = f"{api_url}/aggregates/metadata?lang=en&format=json"
    expected = {"info": "Metadata"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=expected))
    result = aggregates_api.get_aggregates_metadata()
    assert result["info"] == "Metadata"


class DummyException(Exception):
    pass


@pytest.mark.unit
def test_list_aggregates_error(aggregates_api: AggregatesAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    aggregates_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        aggregates_api.list_aggregates()


@pytest.mark.unit
def test_get_aggregate_error(aggregates_api: AggregatesAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    aggregates_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        aggregates_api.get_aggregate("42")


@pytest.mark.unit
def test_get_aggregates_metadata_error(aggregates_api: AggregatesAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    aggregates_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        aggregates_api.get_aggregates_metadata()
