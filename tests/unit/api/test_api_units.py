from urllib.parse import urlencode

import pytest
import responses

from pybdl.api.units import UnitsAPI
from pybdl.config import BDLConfig
from tests.conftest import paginated_mock


@pytest.fixture
def units_api(dummy_config: BDLConfig) -> UnitsAPI:
    return UnitsAPI(dummy_config)


@responses.activate
@pytest.mark.unit
def test_list_units(units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units"
    paginated_mock(url, [{"id": "PL", "name": "Poland"}])
    result = units_api.list_units()
    assert isinstance(result, list)
    assert result[0]["name"] == "Poland"


@responses.activate
@pytest.mark.unit
def test_list_units_with_filters(units_api: UnitsAPI, api_url: str) -> None:
    params = {
        "level": "2",
        "parent-id": "PL",
        "sort": "name",
        "lang": "en",
        "format": "json",
        "page-size": "100",
    }
    url = f"{api_url}/units?{urlencode(params)}"
    responses.add(responses.GET, url, json={"results": []}, status=200)
    # Also add page 1 for completeness
    params["page"] = "1"
    url1 = f"{api_url}/units?{urlencode(params)}"
    responses.add(responses.GET, url1, json={"results": []}, status=200)
    units_api.list_units(level=[2], parent_id="PL", sort="name")
    called_url = responses.calls[0].request.url
    assert called_url is not None
    assert "level=2" in called_url
    assert "parent-id=PL" in called_url
    assert "sort=name" in called_url


@responses.activate
@pytest.mark.unit
def test_get_unit(units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/PL?lang=en&format=json"
    payload = {"id": "PL", "name": "Poland"}
    responses.add(responses.GET, url, json=payload, status=200)
    result = units_api.get_unit(unit_id="PL")
    assert result["id"] == "PL"
    assert result["name"] == "Poland"


@responses.activate
@pytest.mark.unit
def test_get_units_metadata(units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/metadata?lang=en&format=json"
    payload = {"info": "Units API"}
    responses.add(responses.GET, url, json=payload, status=200)
    result = units_api.get_units_metadata()
    assert result["info"] == "Units API"


@responses.activate
@pytest.mark.unit
def test_search_units(units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/search?name=Warsaw&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": "WAW", "name": "Warsaw"}]}, status=200)
    result = units_api.search_units(name="Warsaw")
    assert result[0]["name"] == "Warsaw"


@responses.activate
@pytest.mark.unit
def test_search_units_single_page(units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/search?name=Krakow&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": "KRK", "name": "Krakow"}]}, status=200)
    result = units_api.search_units(name="Krakow", max_pages=1)
    assert result[0]["name"] == "Krakow"


@responses.activate
@pytest.mark.unit
def test_list_localities(units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/localities?parent-id=PL&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": "L1", "name": "Loc1"}]}, status=200)
    result = units_api.list_localities(parent_id="PL")
    assert result[0]["id"] == "L1"


@responses.activate
@pytest.mark.unit
def test_list_localities_single_page(units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/localities?parent-id=PL&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": "L2", "name": "Loc2"}]}, status=200)
    result = units_api.list_localities(parent_id="PL", max_pages=1)
    assert result[0]["id"] == "L2"


@responses.activate
@pytest.mark.unit
def test_get_locality(units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/localities/L1?lang=en&format=json"
    responses.add(responses.GET, url, json={"id": "L1", "name": "Loc1"}, status=200)
    result = units_api.get_locality(locality_id="L1")
    assert result["id"] == "L1"


@responses.activate
@pytest.mark.unit
def test_search_localities(units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/localities/search?name=Loc&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": "L1", "name": "Loc"}]}, status=200)
    result = units_api.search_localities(name="Loc")
    assert result[0]["id"] == "L1"


@responses.activate
@pytest.mark.unit
def test_search_localities_single_page(units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/localities/search?name=Loc2&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": "L2", "name": "Loc2"}]}, status=200)
    result = units_api.search_localities(name="Loc2", max_pages=1)
    assert result[0]["id"] == "L2"
