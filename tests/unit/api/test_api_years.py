import httpx
import pytest
import respx

from pybdl.api.years import YearsAPI
from pybdl.config import BDLConfig


@pytest.fixture
def years_api(dummy_config: BDLConfig) -> YearsAPI:
    return YearsAPI(dummy_config)


@pytest.mark.unit
def test_list_years(respx_mock: respx.MockRouter, years_api: YearsAPI, api_url: str) -> None:
    url = f"{api_url}/years?lang=en&format=json&page-size=100"
    payload = {"results": [{"id": 2020, "name": "2020"}, {"id": 2021, "name": "2021"}]}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = years_api.list_years()
    assert isinstance(result, list)
    assert result[0]["id"] == 2020


@pytest.mark.unit
def test_get_year(respx_mock: respx.MockRouter, years_api: YearsAPI, api_url: str) -> None:
    url = f"{api_url}/years/2021?lang=en&format=json"
    payload = {"id": 2021, "name": "2021"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = years_api.get_year(year_id=2021)
    assert result["id"] == 2021


@pytest.mark.unit
def test_get_years_metadata(respx_mock: respx.MockRouter, years_api: YearsAPI, api_url: str) -> None:
    url = f"{api_url}/years/metadata?lang=en&format=json"
    payload = {"info": "Years API"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = years_api.get_years_metadata()
    assert result["info"] == "Years API"


@pytest.mark.unit
def test_list_years_extra_query(respx_mock: respx.MockRouter, years_api: YearsAPI, api_url: str) -> None:
    url = f"{api_url}/years?foo=bar&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": 2020}]}))
    result = years_api.list_years(extra_query={"foo": "bar"})
    assert result[0]["id"] == 2020


@pytest.mark.unit
def test_get_year_extra_query(respx_mock: respx.MockRouter, years_api: YearsAPI, api_url: str) -> None:
    url = f"{api_url}/years/2022?foo=bar&lang=en&format=json"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"id": 2022}))
    result = years_api.get_year(year_id=2022, extra_query={"foo": "bar"})
    assert result["id"] == 2022


class DummyException(Exception):
    pass


@pytest.mark.unit
def test_list_years_error(years_api: YearsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    years_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        years_api.list_years()


@pytest.mark.unit
def test_get_year_error(years_api: YearsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    years_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        years_api.get_year(2021)


@pytest.mark.unit
def test_get_years_metadata_error(years_api: YearsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    years_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        years_api.get_years_metadata()
