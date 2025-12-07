from urllib.parse import urlencode

import pytest
import responses

from pyldb.api.subjects import SubjectsAPI
from pyldb.config import LDBConfig
from tests.conftest import paginated_mock


@pytest.fixture
def subjects_api(dummy_config: LDBConfig) -> SubjectsAPI:
    return SubjectsAPI(dummy_config)


@responses.activate
@pytest.mark.unit
def test_list_subjects(subjects_api: SubjectsAPI, api_url: str) -> None:
    url = f"{api_url}/subjects"
    paginated_mock(url, [{"id": "A", "name": "Demography"}])
    result = subjects_api.list_subjects()
    assert isinstance(result, list)
    assert result[0]["name"] == "Demography"


@responses.activate
@pytest.mark.unit
def test_list_subjects_with_parent_and_sort(subjects_api: SubjectsAPI, api_url: str) -> None:
    params = {"parent-id": "A", "sort": "name", "lang": "en", "format": "json", "page-size": "100"}
    url = f"{api_url}/subjects?{urlencode(params)}"
    responses.add(responses.GET, url, json={"results": []}, status=200)
    # Also add page 1 for completeness
    params["page"] = "1"
    url1 = f"{api_url}/subjects?{urlencode(params)}"
    responses.add(responses.GET, url1, json={"results": []}, status=200)
    subjects_api.list_subjects(parent_id="A", sort="name")
    called_url = responses.calls[0].request.url
    assert called_url is not None
    assert "parent-id=A" in called_url
    assert "sort=name" in called_url


@responses.activate
@pytest.mark.unit
def test_get_subject(subjects_api: SubjectsAPI, api_url: str) -> None:
    url = f"{api_url}/subjects/B?lang=en&format=json"
    payload = {"id": "B", "name": "Labour market"}
    responses.add(responses.GET, url, json=payload, status=200)
    result = subjects_api.get_subject(subject_id="B")
    assert result["id"] == "B"
    assert result["name"] == "Labour market"


@responses.activate
@pytest.mark.unit
def test_get_subjects_metadata(subjects_api: SubjectsAPI, api_url: str) -> None:
    url = f"{api_url}/subjects/metadata?lang=en&format=json"
    payload = {"info": "Subjects API"}
    responses.add(responses.GET, url, json=payload, status=200)
    result = subjects_api.get_subjects_metadata()
    assert result["info"] == "Subjects API"


@responses.activate
@pytest.mark.unit
def test_list_subjects_extra_query(subjects_api: SubjectsAPI, api_url: str) -> None:
    url = f"{api_url}/subjects?foo=bar&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": "A"}]}, status=200)
    result = subjects_api.list_subjects(extra_query={"foo": "bar"})
    assert result[0]["id"] == "A"


@responses.activate
@pytest.mark.unit
def test_search_subjects_all_branches(subjects_api: SubjectsAPI, api_url: str) -> None:
    # With only name
    url = f"{api_url}/subjects/search?name=foo&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": "A"}]}, status=200)
    result = subjects_api.search_subjects(name="foo")
    assert result[0]["id"] == "A"
    # With all filters
    url = f"{api_url}/subjects/search?name=foo&bar=baz&lang=en&format=json&page-size=100"
    responses.add(responses.GET, url, json={"results": [{"id": "B"}]}, status=200)
    result = subjects_api.search_subjects(name="foo", extra_query={"bar": "baz"})
    assert result[0]["id"] == "B"


class DummyException(Exception):
    pass


@responses.activate
@pytest.mark.unit
def test_list_subjects_error(subjects_api: SubjectsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    subjects_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        subjects_api.list_subjects()


@responses.activate
@pytest.mark.unit
def test_get_subject_error(subjects_api: SubjectsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    subjects_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        subjects_api.get_subject("B")


@responses.activate
@pytest.mark.unit
def test_search_subjects_error(subjects_api: SubjectsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    subjects_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        subjects_api.search_subjects(name="foo")


@responses.activate
@pytest.mark.unit
def test_get_subjects_metadata_error(subjects_api: SubjectsAPI) -> None:
    def raise_exc(*a: object, **k: object) -> None:
        raise DummyException("fail")

    subjects_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        subjects_api.get_subjects_metadata()
