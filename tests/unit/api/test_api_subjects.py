from urllib.parse import urlencode

import httpx
import pytest
import respx

from pybdl.api.subjects import SubjectsAPI
from pybdl.config import BDLConfig
from tests.conftest import paginated_mock


@pytest.fixture
def subjects_api(dummy_config: BDLConfig) -> SubjectsAPI:
    return SubjectsAPI(dummy_config)


@pytest.mark.unit
def test_list_subjects(respx_mock: respx.MockRouter, subjects_api: SubjectsAPI, api_url: str) -> None:
    url = f"{api_url}/subjects"
    paginated_mock(respx_mock, url, [{"id": "A", "name": "Demography"}])
    result = subjects_api.list_subjects()
    assert isinstance(result, list)
    assert result[0]["name"] == "Demography"


@pytest.mark.unit
def test_list_subjects_with_parent_and_sort(
    respx_mock: respx.MockRouter, subjects_api: SubjectsAPI, api_url: str
) -> None:
    params = {"parent-id": "A", "sort": "name", "lang": "en", "format": "json", "page-size": "100"}
    url = f"{api_url}/subjects?{urlencode(params)}"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": []}))
    # Also add page 1 for completeness
    params["page"] = "1"
    url1 = f"{api_url}/subjects?{urlencode(params)}"
    respx_mock.get(url1).mock(return_value=httpx.Response(200, json={"results": []}))
    subjects_api.list_subjects(parent_id="A", sort="name")
    called_url = respx_mock.calls[0].request.url
    assert called_url is not None
    assert "parent-id=A" in str(called_url)
    assert "sort=name" in str(called_url)


@pytest.mark.unit
def test_get_subject(respx_mock: respx.MockRouter, subjects_api: SubjectsAPI, api_url: str) -> None:
    url = f"{api_url}/subjects/B?lang=en&format=json"
    payload = {"id": "B", "name": "Labour market"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = subjects_api.get_subject(subject_id="B")
    assert result["id"] == "B"
    assert result["name"] == "Labour market"


@pytest.mark.unit
def test_get_subjects_metadata(respx_mock: respx.MockRouter, subjects_api: SubjectsAPI, api_url: str) -> None:
    url = f"{api_url}/subjects/metadata?lang=en&format=json"
    payload = {"info": "Subjects API"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = subjects_api.get_subjects_metadata()
    assert result["info"] == "Subjects API"


@pytest.mark.unit
def test_list_subjects_extra_query(respx_mock: respx.MockRouter, subjects_api: SubjectsAPI, api_url: str) -> None:
    url = f"{api_url}/subjects?foo=bar&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "A"}]}))
    result = subjects_api.list_subjects(extra_query={"foo": "bar"})
    assert result[0]["id"] == "A"


@pytest.mark.unit
def test_search_subjects_all_branches(respx_mock: respx.MockRouter, subjects_api: SubjectsAPI, api_url: str) -> None:
    # With only name
    url = f"{api_url}/subjects/search?name=foo&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "A"}]}))
    result = subjects_api.search_subjects(name="foo")
    assert result[0]["id"] == "A"
    # With all filters
    url = f"{api_url}/subjects/search?name=foo&bar=baz&lang=en&format=json&page-size=100"
    respx_mock.get(url).mock(return_value=httpx.Response(200, json={"results": [{"id": "B"}]}))
    result = subjects_api.search_subjects(name="foo", extra_query={"bar": "baz"})
    assert result[0]["id"] == "B"


class DummyException(Exception):
    pass


@pytest.mark.unit
def test_list_subjects_error(subjects_api: SubjectsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    subjects_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        subjects_api.list_subjects()


@pytest.mark.unit
def test_get_subject_error(subjects_api: SubjectsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    subjects_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        subjects_api.get_subject("B")


@pytest.mark.unit
def test_search_subjects_error(subjects_api: SubjectsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    subjects_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        subjects_api.search_subjects(name="foo")


@pytest.mark.unit
def test_get_subjects_metadata_error(subjects_api: SubjectsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    subjects_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        subjects_api.get_subjects_metadata()


@pytest.mark.unit
def test_list_params_page_and_parent() -> None:
    assert SubjectsAPI._list_params(None, None, None, None) == {}
    p = SubjectsAPI._list_params("A", "name", 2, {"extra": "1"})
    assert p == {"parent-id": "A", "sort": "name", "page": 2, "extra": "1"}


@pytest.mark.unit
def test_search_params_page_and_sort() -> None:
    assert SubjectsAPI._search_params("x", 5, "id", {"q": "v"}) == {
        "name": "x",
        "page": 5,
        "sort": "id",
        "q": "v",
    }
