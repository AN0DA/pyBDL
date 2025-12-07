from unittest.mock import AsyncMock, patch

import pytest

from pybdl.api.levels import LevelsAPI
from pybdl.config import BDLConfig


@pytest.fixture
def levels_api(dummy_config: BDLConfig) -> LevelsAPI:
    return LevelsAPI(dummy_config)


@pytest.mark.asyncio
@patch.object(LevelsAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_alist_levels_all_branches(afetch_all_results: AsyncMock, levels_api: LevelsAPI) -> None:
    # No params
    afetch_all_results.return_value = [{"id": 1}]
    result = await levels_api.alist_levels()
    assert result == [{"id": 1}]
    # With sort
    afetch_all_results.return_value = [{"id": 2}]
    result = await levels_api.alist_levels(sort="Name")
    assert result == [{"id": 2}]
    # With extra_query
    afetch_all_results.return_value = [{"id": 3}]
    result = await levels_api.alist_levels(extra_query={"foo": "bar"})
    assert result == [{"id": 3}]


@pytest.mark.asyncio
@patch.object(LevelsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_level_all_branches(afetch_single_result: AsyncMock, levels_api: LevelsAPI) -> None:
    afetch_single_result.return_value = {"id": 3}
    result = await levels_api.aget_level(3)
    assert result["id"] == 3
    afetch_single_result.return_value = {"id": 4}
    result = await levels_api.aget_level(4, extra_query={"foo": "bar"})
    assert result["id"] == 4


@pytest.mark.asyncio
@patch.object(LevelsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_levels_metadata(afetch_single_result: AsyncMock, levels_api: LevelsAPI) -> None:
    afetch_single_result.return_value = {"version": "1.0"}
    result = await levels_api.aget_levels_metadata()
    assert result["version"] == "1.0"


class DummyException(Exception):
    pass


@pytest.mark.asyncio
@patch.object(LevelsAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_alist_levels_error(afetch_all_results: AsyncMock, levels_api: LevelsAPI) -> None:
    afetch_all_results.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await levels_api.alist_levels()


@pytest.mark.asyncio
@patch.object(LevelsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_level_error(afetch_single_result: AsyncMock, levels_api: LevelsAPI) -> None:
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await levels_api.aget_level(3)


@pytest.mark.asyncio
@patch.object(LevelsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_levels_metadata_error(afetch_single_result: AsyncMock, levels_api: LevelsAPI) -> None:
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await levels_api.aget_levels_metadata()
