import pytest
import responses

from pyldb.api.aggregates import AggregatesAPI
from pyldb.config import LDBConfig


@pytest.fixture
def aggregates_api(dummy_config: LDBConfig) -> AggregatesAPI:
    return AggregatesAPI(dummy_config)


@responses.activate
@pytest.mark.unit
def test_list_aggregates(aggregates_api: AggregatesAPI, api_url: str) -> None:
    url = f"{api_url}/aggregates?lang=en&format=json&page-size=100"
    expected = {"results": [{"id": 1, "name": "Agg1"}]}
    responses.add(responses.GET, url, json=expected, status=200)
    result = aggregates_api.list_aggregates()
    assert isinstance(result, list)
    assert result[0]["name"] == "Agg1"


@responses.activate
@pytest.mark.unit
def test_list_aggregates_with_sort(aggregates_api: AggregatesAPI, api_url: str) -> None:
    url = f"{api_url}/aggregates?sort=Name&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": []}, status=200)
    aggregates_api.list_aggregates(sort="Name")
    # With responses, you can inspect the call arguments:
    called_url = responses.calls[0].request.url
    assert called_url is not None
    assert "sort=Name" in called_url
    assert "lang=en" in called_url
    assert "page-size=100" in called_url


@responses.activate
@pytest.mark.unit
def test_list_aggregates_extra_query(aggregates_api: AggregatesAPI, api_url: str) -> None:
    url = f"{api_url}/aggregates?foo=bar&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": 1}]}, status=200)
    result = aggregates_api.list_aggregates(extra_query={"foo": "bar"})
    assert result[0]["id"] == 1


@responses.activate
@pytest.mark.unit
def test_get_aggregate(aggregates_api: AggregatesAPI, api_url: str) -> None:
    url = f"{api_url}/aggregates/42?lang=en&format=json"
    expected = {"id": 42, "name": "Agg42"}
    responses.add(responses.GET, url, json=expected, status=200)
    result = aggregates_api.get_aggregate(aggregate_id="42")
    assert result["id"] == 42


@responses.activate
@pytest.mark.unit
def test_get_aggregates_metadata(aggregates_api: AggregatesAPI, api_url: str) -> None:
    url = f"{api_url}/aggregates/metadata?lang=en&format=json"
    expected = {"info": "Metadata"}
    responses.add(responses.GET, url, json=expected, status=200)
    result = aggregates_api.get_aggregates_metadata()
    assert result["info"] == "Metadata"


class DummyException(Exception):
    pass


@responses.activate
@pytest.mark.unit
def test_list_aggregates_error(aggregates_api: AggregatesAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    aggregates_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        aggregates_api.list_aggregates()


@responses.activate
@pytest.mark.unit
def test_get_aggregate_error(aggregates_api: AggregatesAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    aggregates_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        aggregates_api.get_aggregate("42")


@responses.activate
@pytest.mark.unit
def test_get_aggregates_metadata_error(aggregates_api: AggregatesAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    aggregates_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        aggregates_api.get_aggregates_metadata()
