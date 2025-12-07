from unittest.mock import AsyncMock, patch

import pytest

from pyldb.api.years import YearsAPI
from pyldb.config import LDBConfig


@pytest.fixture
def years_api(dummy_config: LDBConfig) -> YearsAPI:
    return YearsAPI(dummy_config)


@pytest.mark.asyncio
@patch.object(YearsAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_alist_years_all_branches(afetch_all_results: AsyncMock, years_api: YearsAPI) -> None:
    # No params
    afetch_all_results.return_value = [{"id": 2020}]
    result = await years_api.alist_years()
    assert result == [{"id": 2020}]
    # With sort
    afetch_all_results.return_value = [{"id": 2021}]
    result = await years_api.alist_years(sort="Name")
    assert result == [{"id": 2021}]
    # With extra_query
    afetch_all_results.return_value = [{"id": 2022}]
    result = await years_api.alist_years(extra_query={"foo": "bar"})
    assert result == [{"id": 2022}]


@pytest.mark.asyncio
@patch.object(YearsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_year_all_branches(afetch_single_result: AsyncMock, years_api: YearsAPI) -> None:
    afetch_single_result.return_value = {"id": 2021}
    result = await years_api.aget_year(2021)
    assert result["id"] == 2021
    afetch_single_result.return_value = {"id": 2022}
    result = await years_api.aget_year(2022, extra_query={"foo": "bar"})
    assert result["id"] == 2022


@pytest.mark.asyncio
@patch.object(YearsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_years_metadata(afetch_single_result: AsyncMock, years_api: YearsAPI) -> None:
    afetch_single_result.return_value = {"info": "meta"}
    result = await years_api.aget_years_metadata()
    assert result["info"] == "meta"


class DummyException(Exception):
    pass


@pytest.mark.asyncio
@patch.object(YearsAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_alist_years_error(afetch_all_results: AsyncMock, years_api: YearsAPI) -> None:
    afetch_all_results.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await years_api.alist_years()


@pytest.mark.asyncio
@patch.object(YearsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_year_error(afetch_single_result: AsyncMock, years_api: YearsAPI) -> None:
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await years_api.aget_year(2021)


@pytest.mark.asyncio
@patch.object(YearsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_years_metadata_error(afetch_single_result: AsyncMock, years_api: YearsAPI) -> None:
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await years_api.aget_years_metadata()
