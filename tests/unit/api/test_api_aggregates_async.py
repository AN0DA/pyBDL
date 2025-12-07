from unittest.mock import AsyncMock, patch

import pytest

from pyldb.api.aggregates import AggregatesAPI
from pyldb.config import LDBConfig


@pytest.fixture
def aggregates_api(dummy_config: LDBConfig) -> AggregatesAPI:
    return AggregatesAPI(dummy_config)


@pytest.mark.asyncio
@patch.object(AggregatesAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_alist_aggregates_all_branches(afetch_all_results: AsyncMock, aggregates_api: AggregatesAPI) -> None:
    # No params
    afetch_all_results.return_value = [{"id": 1}]
    result = await aggregates_api.alist_aggregates()
    assert result == [{"id": 1}]
    # With sort
    afetch_all_results.return_value = [{"id": 2}]
    result = await aggregates_api.alist_aggregates(sort="Name")
    assert result == [{"id": 2}]
    # With extra_query
    afetch_all_results.return_value = [{"id": 3}]
    result = await aggregates_api.alist_aggregates(extra_query={"foo": "bar"})
    assert result == [{"id": 3}]


@pytest.mark.asyncio
@patch.object(AggregatesAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_aggregate(afetch_single_result: AsyncMock, aggregates_api: AggregatesAPI) -> None:
    afetch_single_result.return_value = {"id": 42}
    result = await aggregates_api.aget_aggregate("42")
    assert result["id"] == 42


@pytest.mark.asyncio
@patch.object(AggregatesAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_aggregates_metadata(afetch_single_result: AsyncMock, aggregates_api: AggregatesAPI) -> None:
    afetch_single_result.return_value = {"info": "meta"}
    result = await aggregates_api.aget_aggregates_metadata()
    assert result["info"] == "meta"


class DummyException(Exception):
    pass


@pytest.mark.asyncio
@patch.object(AggregatesAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_alist_aggregates_error(afetch_all_results: AsyncMock, aggregates_api: AggregatesAPI) -> None:
    afetch_all_results.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await aggregates_api.alist_aggregates()


@pytest.mark.asyncio
@patch.object(AggregatesAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_aggregate_error(afetch_single_result: AsyncMock, aggregates_api: AggregatesAPI) -> None:
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await aggregates_api.aget_aggregate("42")


@pytest.mark.asyncio
@patch.object(AggregatesAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_aggregates_metadata_error(afetch_single_result: AsyncMock, aggregates_api: AggregatesAPI) -> None:
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await aggregates_api.aget_aggregates_metadata()
