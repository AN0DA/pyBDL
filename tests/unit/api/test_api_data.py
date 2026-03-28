# type: ignore
from typing import Any
from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode

import httpx
import pytest
import respx

from pybdl.api.data import DataAPI
from pybdl.config import BDLConfig


@pytest.fixture
def data_api(dummy_config: BDLConfig) -> DataAPI:
    return DataAPI(dummy_config)


@pytest.mark.unit
def test_get_data_by_variable(respx_mock: respx.MockRouter, data_api: DataAPI, api_url: str) -> None:
    url = f"{api_url}/data/by-variable/3643?lang=en&format=json&page-size=100"
    payload = {"results": [{"id": "A", "value": 123}]}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    response = data_api.get_data_by_variable(variable_id="3643", max_pages=1)
    assert isinstance(response, list)
    assert response[0]["id"] == "A"


@pytest.mark.unit
def test_get_data_by_unit(respx_mock: respx.MockRouter, data_api: DataAPI, api_url: str) -> None:
    params = {"var-id": "3643", "lang": "en", "format": "json", "page-size": "100"}
    url = f"{api_url}/data/by-unit/999?{urlencode(params)}"
    payload = {"results": [{"id": "B", "value": 555}]}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    response = data_api.get_data_by_unit(unit_id="999", variable_ids=[3643])
    assert isinstance(response, list)
    assert response[0]["id"] == "B"
    request_url = respx_mock.calls[0].request.url
    assert request_url is not None and "var-id=3643" in str(request_url)


@pytest.mark.unit
def test_get_data_by_variable_locality(respx_mock: respx.MockRouter, data_api: DataAPI, api_url: str) -> None:
    url = f"{api_url}/data/localities/by-variable/7?lang=en&format=json&unit-parent-id=2&page-size=100"
    payload = {"results": [{"id": "C", "value": 42}]}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    response = data_api.get_data_by_variable_locality(variable_id="7", unit_parent_id="2", max_pages=1)
    assert isinstance(response, list)
    assert response[0]["id"] == "C"


@pytest.mark.unit
def test_get_data_locality_by_unit(respx_mock: respx.MockRouter, data_api: DataAPI, api_url: str) -> None:
    url = f"{api_url}/data/localities/by-unit/44?lang=en&format=json&var-id=3643&page-size=100"
    payload = {"results": [{"id": "D", "value": 10}]}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    response = data_api.get_data_by_unit_locality(unit_id="44", variable_ids=[3643], max_pages=1)
    assert isinstance(response, list)
    assert response[0]["id"] == "D"


@pytest.mark.unit
def test_get_data_metadata(respx_mock: respx.MockRouter, data_api: DataAPI, api_url: str) -> None:
    url = f"{api_url}/data/metadata?lang=en&format=json"
    payload = {"info": "data metadata"}
    respx_mock.get(url).mock(return_value=httpx.Response(200, json=payload))
    result = data_api.get_data_metadata()
    assert result["info"] == "data metadata"


@pytest.mark.unit
def test_get_data_metadata_error(data_api: DataAPI) -> None:
    # Simulate error in fetch_single_result
    class DummyException(Exception):
        pass

    def raise_exc(*a: Any, **k: Any) -> None:
        raise DummyException("fail")

    data_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        data_api.get_data_metadata()


@pytest.mark.unit
def test_get_data_by_variable_all_branches(data_api: DataAPI) -> None:
    # max_pages=None (default, all pages), return_metadata True
    def mock_fetch_all_results(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return ([{"id": "A", "value": 123}], {"meta": 1})

    data_api.fetch_all_results = mock_fetch_all_results  # type: ignore[assignment]
    result = data_api.get_data_by_variable(variable_id="3643", return_metadata=True)
    assert result == ([{"id": "A", "value": 123}], {"meta": 1})

    # all_pages True, return_metadata False
    def mock_fetch_all_results_no_meta(*a: Any, **k: Any) -> list[dict[str, Any]]:
        return [{"id": "B"}]

    data_api.fetch_all_results = mock_fetch_all_results_no_meta  # type: ignore[assignment]
    result_no_meta = data_api.get_data_by_variable(variable_id="3643", return_metadata=False)
    assert result_no_meta == [{"id": "B"}]

    # max_pages=1 (single page), return_metadata False
    def mock_fetch_single_result(*a: Any, **k: Any) -> list[dict[str, Any]]:
        return [{"id": "C"}]

    data_api.fetch_single_result = mock_fetch_single_result  # type: ignore[assignment]
    result_single = data_api.get_data_by_variable(variable_id="3643", max_pages=1, return_metadata=False)
    assert result_single == [{"id": "C"}]


@pytest.mark.unit
def test_get_data_by_unit_all_branches(data_api: DataAPI, api_url: str) -> None:
    # return_metadata True
    def mock_fetch_single_result_with_meta(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return ([{"id": "A"}], {"meta": 1})

    data_api.fetch_single_result = mock_fetch_single_result_with_meta  # type: ignore[assignment]
    result = data_api.get_data_by_unit(unit_id="1", variable_id=[1], return_metadata=True)
    assert result == ([{"id": "A"}], {"meta": 1})

    # return_metadata False
    def mock_fetch_single_result_no_meta(*a: Any, **k: Any) -> list[dict[str, Any]]:
        return [{"id": "B"}]

    data_api.fetch_single_result = mock_fetch_single_result_no_meta  # type: ignore[assignment]
    result_no_meta = data_api.get_data_by_unit(unit_id="1", variable_id=[1], return_metadata=False)
    assert result_no_meta == [{"id": "B"}]


@pytest.mark.unit
def test_get_data_by_variable_locality_all_branches(data_api: DataAPI, api_url: str) -> None:
    # max_pages=None (default, all pages), return_metadata True
    def mock_fetch_all_results_with_meta(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return ([{"id": "A"}], {"meta": 1})

    data_api.fetch_all_results = mock_fetch_all_results_with_meta  # type: ignore[assignment]
    result = data_api.get_data_by_variable_locality(variable_id="v", unit_parent_id="l", return_metadata=True)
    assert result == ([{"id": "A"}], {"meta": 1})

    # all_pages True, return_metadata False
    def mock_fetch_all_results_no_meta(*a: Any, **k: Any) -> list[dict[str, Any]]:
        return [{"id": "B"}]

    data_api.fetch_all_results = mock_fetch_all_results_no_meta  # type: ignore[assignment]
    result_no_meta = data_api.get_data_by_variable_locality(variable_id="v", unit_parent_id="l", return_metadata=False)
    assert result_no_meta == [{"id": "B"}]

    # all_pages False, return_metadata True
    def mock_fetch_single_result_with_meta(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return ([{"id": "C"}], {"meta": 2})

    data_api.fetch_single_result = mock_fetch_single_result_with_meta  # type: ignore[assignment]
    result_single_meta = data_api.get_data_by_variable_locality(
        variable_id="v", unit_parent_id="l", max_pages=1, return_metadata=True
    )
    assert result_single_meta == ([{"id": "C"}], {"meta": 2})

    # max_pages=1 (single page), return_metadata False
    def mock_fetch_single_result_no_meta(*a: Any, **k: Any) -> list[dict[str, Any]]:
        return [{"id": "D"}]

    data_api.fetch_single_result = mock_fetch_single_result_no_meta  # type: ignore[assignment]
    result_single_no_meta = data_api.get_data_by_variable_locality(
        variable_id="v", unit_parent_id="l", max_pages=1, return_metadata=False
    )
    assert result_single_no_meta == [{"id": "D"}]


@pytest.mark.unit
def test_get_data_by_unit_locality_all_branches(data_api: DataAPI, api_url: str) -> None:
    # max_pages=None (default, all pages), return_metadata True
    def mock_fetch_all_results_with_meta(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return ([{"id": "A"}], {"meta": 1})

    data_api.fetch_all_results = mock_fetch_all_results_with_meta  # type: ignore[assignment]
    result = data_api.get_data_by_unit_locality(unit_id="u", variable_id=[1], return_metadata=True)
    assert result == ([{"id": "A"}], {"meta": 1})

    # all_pages True, return_metadata False
    def mock_fetch_all_results_no_meta(*a: Any, **k: Any) -> list[dict[str, Any]]:
        return [{"id": "B"}]

    data_api.fetch_all_results = mock_fetch_all_results_no_meta  # type: ignore[assignment]
    result_no_meta = data_api.get_data_by_unit_locality(unit_id="u", variable_id=[1], return_metadata=False)
    assert result_no_meta == [{"id": "B"}]

    # all_pages False, return_metadata True
    def mock_fetch_single_result_with_meta(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return ([{"id": "C"}], {"meta": 2})

    data_api.fetch_single_result = mock_fetch_single_result_with_meta  # type: ignore[assignment]
    result_single_meta = data_api.get_data_by_unit_locality(
        unit_id="u", variable_id=[1], max_pages=1, return_metadata=True
    )
    assert result_single_meta == ([{"id": "C"}], {"meta": 2})

    # max_pages=1 (single page), return_metadata False
    def mock_fetch_single_result_no_meta(*a: Any, **k: Any) -> list[dict[str, Any]]:
        return [{"id": "D"}]

    data_api.fetch_single_result = mock_fetch_single_result_no_meta  # type: ignore[assignment]
    result_single_no_meta = data_api.get_data_by_unit_locality(
        unit_id="u", variable_id=[1], max_pages=1, return_metadata=False
    )
    assert result_single_no_meta == [{"id": "D"}]


@pytest.mark.unit
def test_get_data_by_variable_params(data_api: DataAPI, api_url: str) -> None:
    # Test all optional params: year, unit_level, parent_id, format, extra_query
    def mock_fetch_all_results(
        endpoint: str, params: dict[str, Any], **kwargs: Any
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return (params, {"meta": 1})

    data_api.fetch_all_results = mock_fetch_all_results  # type: ignore[assignment]
    result, meta = data_api.get_data_by_variable(
        variable_id="v",
        years=[2020],
        unit_level=2,
        unit_parent_id="pid",
        format="csv",
        extra_query={"foo": "bar"},
        return_metadata=True,
    )
    # result is the params dictionary returned by the mock
    assert result["year"] == [2020]
    assert result["unit-level"] == 2
    assert result["unit-parent-id"] == "pid"
    assert result["format"] == "csv"
    assert result["foo"] == "bar"


@pytest.mark.unit
def test_get_data_by_unit_params(data_api: DataAPI, api_url: str) -> None:
    def mock_fetch_single_result(
        endpoint: str, results_key: str, params: dict[str, Any], **kwargs: Any
    ) -> dict[str, Any]:
        return params

    data_api.fetch_single_result = mock_fetch_single_result  # type: ignore[assignment]
    result = data_api.get_data_by_unit(
        unit_id="u",
        variable_id=[1],
        years=[2021],
        format="csv",
        extra_query={"bar": "baz"},
        return_metadata=True,
    )
    # result is the params dictionary returned by the mock
    assert result["year"] == [2021]
    assert result["format"] == "csv"
    assert result["bar"] == "baz"


@pytest.mark.unit
def test_get_data_by_variable_locality_params(data_api: DataAPI, api_url: str) -> None:
    def mock_fetch_all_results(
        endpoint: str, params: dict[str, Any], **kwargs: Any
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return (params, {"meta": 1})

    data_api.fetch_all_results = mock_fetch_all_results  # type: ignore[assignment]
    result, meta = data_api.get_data_by_variable_locality(
        variable_id="v",
        unit_parent_id="l",
        years=[2022],
        format="csv",
        extra_query={"baz": "qux"},
        return_metadata=True,
    )
    # result is the params dictionary returned by the mock
    assert result["year"] == [2022]
    assert result["format"] == "csv"
    assert result["baz"] == "qux"


@pytest.mark.unit
def test_get_data_by_unit_locality_params(data_api: DataAPI, api_url: str) -> None:
    def mock_fetch_all_results(
        endpoint: str, params: dict[str, Any], **kwargs: Any
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return (params, {"meta": 1})

    data_api.fetch_all_results = mock_fetch_all_results  # type: ignore[assignment]
    result, meta = data_api.get_data_by_unit_locality(
        unit_id="u",
        variable_id=[1],
        years=[2023],
        format="csv",
        extra_query={"qux": "quux"},
        return_metadata=True,
    )
    # result is the params dictionary returned by the mock
    assert result["year"] == [2023]
    assert result["format"] == "csv"
    assert result["qux"] == "quux"


@pytest.mark.unit
def test_get_data_by_variable_edge_cases(data_api: DataAPI, api_url: str) -> None:
    # Empty results
    def mock_fetch_all_results_empty(*a: Any, **k: Any) -> tuple[list[Any], dict[str, Any]]:
        return ([], {"meta": 1})

    data_api.fetch_all_results = mock_fetch_all_results_empty  # type: ignore[assignment]
    result = data_api.get_data_by_variable(variable_id="v", return_metadata=True)
    assert result == ([], {"meta": 1})

    # Missing metadata
    def mock_fetch_all_results_no_meta(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], None]:
        return ([{"id": 1}], None)

    data_api.fetch_all_results = mock_fetch_all_results_no_meta  # type: ignore[assignment]
    result_no_meta = data_api.get_data_by_variable(variable_id="v", return_metadata=True)
    # Check each element of the tuple separately to avoid type issues
    assert result_no_meta[0] == [{"id": 1}]
    assert result_no_meta[1] is None


@pytest.mark.unit
def test_get_data_by_unit_locality_edge_cases(data_api: DataAPI, api_url: str) -> None:
    # Empty results
    def mock_fetch_all_results_empty(*a: Any, **k: Any) -> tuple[list[Any], dict[str, Any]]:
        return ([], {"meta": 1})

    data_api.fetch_all_results = mock_fetch_all_results_empty  # type: ignore[assignment]
    result = data_api.get_data_by_unit_locality(unit_id="u", variable_id=[1], return_metadata=True)
    assert result == ([], {"meta": 1})

    # Missing metadata
    def mock_fetch_all_results_no_meta(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], None]:
        return ([{"id": 1}], None)

    data_api.fetch_all_results = mock_fetch_all_results_no_meta  # type: ignore[assignment]
    result_no_meta = data_api.get_data_by_unit_locality(unit_id="u", variable_id=[1], return_metadata=True)
    # Check each element of the tuple separately to avoid type issues
    assert result_no_meta[0] == [{"id": 1}]
    assert result_no_meta[1] is None


class DummyException(Exception):
    pass


@pytest.mark.asyncio
@patch.object(DataAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_async_get_data_metadata_error(afetch_single_result: AsyncMock, data_api: DataAPI) -> None:
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await data_api.aget_data_metadata()


@pytest.mark.asyncio
@patch.object(DataAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_async_get_data_by_variable_error(afetch_all_results: AsyncMock, data_api: DataAPI) -> None:
    afetch_all_results.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await data_api.aget_data_by_variable(variable_id="v", return_metadata=True)


@pytest.mark.asyncio
@patch.object(DataAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_async_get_data_by_unit_error(afetch_single_result: AsyncMock, data_api: DataAPI) -> None:
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await data_api.aget_data_by_unit(unit_id="u", variable_id=[1], return_metadata=True)


@pytest.mark.unit
def test_get_data_by_variable_error(data_api: DataAPI) -> None:
    def raise_exc(*a: Any, **k: Any) -> None:
        raise DummyException("fail")

    data_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        data_api.get_data_by_variable(variable_id="v", return_metadata=True)


@pytest.mark.unit
def test_get_data_by_unit_error(data_api: DataAPI) -> None:
    def raise_exc(*a: Any, **k: Any) -> None:
        raise DummyException("fail")

    data_api.fetch_single_result = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        data_api.get_data_by_unit(unit_id="u", variable_id=[1], return_metadata=True)


@pytest.mark.unit
def test_get_data_by_variable_locality_error(data_api: DataAPI) -> None:
    def raise_exc(*a: Any, **k: Any) -> None:
        raise DummyException("fail")

    data_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        data_api.get_data_by_variable_locality(variable_id="v", unit_parent_id="l", return_metadata=True)


@pytest.mark.unit
def test_get_data_by_unit_locality_error(data_api: DataAPI) -> None:
    def raise_exc(*a: Any, **k: Any) -> None:
        raise DummyException("fail")

    data_api.fetch_all_results = raise_exc  # type: ignore[assignment]
    with pytest.raises(DummyException):
        data_api.get_data_by_unit_locality(unit_id="u", variable_id=[1], return_metadata=True)


@pytest.mark.asyncio
@patch.object(DataAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_async_get_data_by_variable_locality_error(afetch_all_results: AsyncMock, data_api: DataAPI) -> None:
    afetch_all_results.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await data_api.aget_data_by_variable_locality(variable_id="v", unit_parent_id="l", return_metadata=True)


@pytest.mark.asyncio
@patch.object(DataAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_async_get_data_by_unit_locality_error(afetch_all_results: AsyncMock, data_api: DataAPI) -> None:
    afetch_all_results.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await data_api.aget_data_by_unit_locality(unit_id="u", variable_id=[1], return_metadata=True)


@pytest.mark.unit
def test_get_data_by_variable_pagination(data_api: DataAPI) -> None:
    # Test max_pages and page_size are passed through
    def fetch_all_results(
        endpoint: str, params: dict[str, Any], page_size: int, max_pages: int, **kwargs: Any
    ) -> tuple[int, int]:
        return page_size, max_pages

    data_api.fetch_all_results = fetch_all_results  # type: ignore[assignment]
    result = data_api.get_data_by_variable(variable_id="v", page_size=55, max_pages=3)
    # result is a tuple (page_size, max_pages)
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert result[0] == 55  # page_size
    assert result[1] == 3  # max_pages


@pytest.mark.asyncio
@patch.object(DataAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_async_get_data_by_variable_pagination(afetch_all_results: AsyncMock, data_api: DataAPI) -> None:
    afetch_all_results.return_value = (55, 3)
    result = await data_api.aget_data_by_variable(variable_id="v", page_size=55, max_pages=3)
    # result is a tuple (page_size, max_pages)
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert result[0] == 55  # page_size
    assert result[1] == 3  # max_pages


@pytest.mark.unit
def test_normalize_variable_ids_both_parameters_rejected() -> None:
    with pytest.raises(TypeError, match="not both"):
        DataAPI._normalize_variable_ids([1], [2])  # type: ignore[arg-type]


@pytest.mark.unit
def test_normalize_variable_ids_neither_parameter_rejected() -> None:
    with pytest.raises(TypeError, match="required"):
        DataAPI._normalize_variable_ids(None, None)


@pytest.mark.unit
def test_normalize_variable_ids_single_int_and_string() -> None:
    assert DataAPI._normalize_variable_ids(42, None) == [42]
    assert DataAPI._normalize_variable_ids("99", None) == [99]


@pytest.mark.unit
def test_normalize_variable_ids_sequence() -> None:
    assert DataAPI._normalize_variable_ids([1, "2"], None) == [1, 2]


@pytest.mark.unit
def test_get_data_by_variable_with_metadata_sets_flag(data_api: DataAPI) -> None:
    captured: dict[str, Any] = {}

    def capture(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        captured.update(k)
        return ([{"x": 1}], {"m": 2})

    data_api.get_data_by_variable = capture  # type: ignore[assignment]
    rows, meta = data_api.get_data_by_variable_with_metadata(variable_id="3643")
    assert rows == [{"x": 1}]
    assert meta == {"m": 2}
    assert captured.get("return_metadata") is True


@pytest.mark.unit
def test_get_data_by_unit_with_metadata_sets_flag(data_api: DataAPI) -> None:
    captured: dict[str, Any] = {}

    def capture(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        captured.update(k)
        return ([{"x": 1}], {"m": 2})

    data_api.get_data_by_unit = capture  # type: ignore[assignment]
    rows, meta = data_api.get_data_by_unit_with_metadata(unit_id="1", variable_ids=[1])
    assert captured.get("return_metadata") is True
    assert rows == [{"x": 1}]


@pytest.mark.unit
def test_get_data_by_variable_locality_with_metadata_sets_flag(data_api: DataAPI) -> None:
    captured: dict[str, Any] = {}

    def capture(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        captured.update(k)
        return ([], {})

    data_api.get_data_by_variable_locality = capture  # type: ignore[assignment]
    data_api.get_data_by_variable_locality_with_metadata(variable_id="v", unit_parent_id="l")
    assert captured.get("return_metadata") is True


@pytest.mark.unit
def test_get_data_by_unit_locality_with_metadata_sets_flag(data_api: DataAPI) -> None:
    captured: dict[str, Any] = {}

    def capture(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        captured.update(k)
        return ([], {})

    data_api.get_data_by_unit_locality = capture  # type: ignore[assignment]
    data_api.get_data_by_unit_locality_with_metadata(unit_id="u", variable_ids=[1])
    assert captured.get("return_metadata") is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_aget_data_by_variable_with_metadata_sets_flag(data_api: DataAPI) -> None:
    captured: dict[str, Any] = {}

    async def capture(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        captured.update(k)
        return ([{"a": 1}], {"b": 2})

    data_api.aget_data_by_variable = capture  # type: ignore[assignment]
    rows, meta = await data_api.aget_data_by_variable_with_metadata(variable_id="v")
    assert captured.get("return_metadata") is True
    assert rows == [{"a": 1}]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_aget_data_by_unit_with_metadata_sets_flag(data_api: DataAPI) -> None:
    captured: dict[str, Any] = {}

    async def capture(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        captured.update(k)
        return ([], {})

    data_api.aget_data_by_unit = capture  # type: ignore[assignment]
    await data_api.aget_data_by_unit_with_metadata(unit_id="u", variable_ids=[1])
    assert captured.get("return_metadata") is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_aget_data_by_variable_locality_with_metadata_sets_flag(data_api: DataAPI) -> None:
    captured: dict[str, Any] = {}

    async def capture(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        captured.update(k)
        return ([], {})

    data_api.aget_data_by_variable_locality = capture  # type: ignore[assignment]
    await data_api.aget_data_by_variable_locality_with_metadata(variable_id="v", unit_parent_id="l")
    assert captured.get("return_metadata") is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_aget_data_by_unit_locality_with_metadata_sets_flag(data_api: DataAPI) -> None:
    captured: dict[str, Any] = {}

    async def capture(*a: Any, **k: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        captured.update(k)
        return ([], {})

    data_api.aget_data_by_unit_locality = capture  # type: ignore[assignment]
    await data_api.aget_data_by_unit_locality_with_metadata(unit_id="u", variable_ids=[1])
    assert captured.get("return_metadata") is True
