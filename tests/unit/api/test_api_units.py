from urllib.parse import urlencode

import httpx
import pytest
import respx

from pybdl.api.units import UnitsAPI
from pybdl.config import BDLConfig
from tests.conftest import paginated_mock


@pytest.fixture
def units_api(dummy_config: BDLConfig) -> UnitsAPI:
    return UnitsAPI(dummy_config)


@pytest.mark.unit
def test_list_units(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units"
    paginated_mock(respx_mock, url, [{"id": "PL", "name": "Poland"}])
    result = units_api.list_units()
    assert isinstance(result, list)
    assert result[0]["name"] == "Poland"


@pytest.mark.unit
def test_list_units_with_filters(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    params = {
        "level": "2",
        "parent-id": "PL",
        "sort": "name",
        "lang": "en",
        "format": "json",
        "page-size": "100",
    }
    url = f"{api_url}/units?{urlencode(params)}"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": []}))
    # Also add page 1 for completeness
    params["page"] = "1"
    url1 = f"{api_url}/units?{urlencode(params)}"
    respx_mock.get(url1).mock(return_value=httpx.Response(200, json={"results": []}))
    units_api.list_units(level=[2], parent_id="PL", sort="name")
    called_url = respx_mock.calls[0].request.url
    assert called_url is not None
    assert "level=2" in str(called_url)
    assert "parent-id=PL" in str(called_url)
    assert "sort=name" in str(called_url)


@pytest.mark.unit
def test_get_unit(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/PL?lang=en&format=json"
    payload = {"id": "PL", "name": "Poland"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = units_api.get_unit(unit_id="PL")
    assert result["id"] == "PL"
    assert result["name"] == "Poland"


@pytest.mark.unit
def test_get_units_metadata(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/metadata?lang=en&format=json"
    payload = {"info": "Units API"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = units_api.get_units_metadata()
    assert result["info"] == "Units API"


@pytest.mark.unit
def test_search_units(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/search?name=Warsaw&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "WAW", "name": "Warsaw"}]}))
    result = units_api.search_units(name="Warsaw")
    assert result[0]["name"] == "Warsaw"


@pytest.mark.unit
def test_search_units_single_page(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/search?name=Krakow&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "KRK", "name": "Krakow"}]}))
    result = units_api.search_units(name="Krakow", max_pages=1)
    assert result[0]["name"] == "Krakow"


@pytest.mark.unit
def test_list_localities(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/localities?parent-id=PL&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "L1", "name": "Loc1"}]}))
    result = units_api.list_localities(parent_id="PL")
    assert result[0]["id"] == "L1"


@pytest.mark.unit
def test_list_localities_single_page(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/localities?parent-id=PL&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "L2", "name": "Loc2"}]}))
    result = units_api.list_localities(parent_id="PL", max_pages=1)
    assert result[0]["id"] == "L2"


@pytest.mark.unit
def test_get_locality(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/localities/L1?lang=en&format=json"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"id": "L1", "name": "Loc1"}))
    result = units_api.get_locality(locality_id="L1")
    assert result["id"] == "L1"


@pytest.mark.unit
def test_search_localities(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/localities/search?name=Loc&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "L1", "name": "Loc"}]}))
    result = units_api.search_localities(name="Loc")
    assert result[0]["id"] == "L1"


@pytest.mark.unit
def test_search_localities_single_page(respx_mock: respx.MockRouter, units_api: UnitsAPI, api_url: str) -> None:
    url = f"{api_url}/units/localities/search?name=Loc2&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "L2", "name": "Loc2"}]}))
    result = units_api.search_localities(name="Loc2", max_pages=1)
    assert result[0]["id"] == "L2"


@pytest.mark.unit
def test_search_units_params_optional_fields() -> None:
    assert UnitsAPI._search_units_params(None, None, None, None, None, None, None) == {}
    p = UnitsAPI._search_units_params(
        name="Warsaw",
        level=[2],
        years=[2020],
        kind="all",
        page=3,
        sort="id",
        extra_query={"foo": "bar"},
    )
    assert p["name"] == "Warsaw"
    assert p["level"] == [2]
    assert p["year"] == [2020]
    assert p["kind"] == "all"
    assert p["page"] == 3
    assert p["sort"] == "id"
    assert p["foo"] == "bar"


@pytest.mark.unit
def test_list_localities_params() -> None:
    assert UnitsAPI._list_localities_params("PL", 2, "name", {"x": "y"}) == {
        "parent-id": "PL",
        "page": 2,
        "sort": "name",
        "x": "y",
    }


@pytest.mark.unit
def test_search_localities_params_optional_fields() -> None:
    assert UnitsAPI._search_localities_params(None, None, None, None, None) == {}
    p = UnitsAPI._search_localities_params("Loc", [2019, 2020], 1, "id", {"z": "q"})
    assert p["name"] == "Loc"
    assert p["year"] == [2019, 2020]
    assert p["page"] == 1
    assert p["sort"] == "id"
    assert p["z"] == "q"
