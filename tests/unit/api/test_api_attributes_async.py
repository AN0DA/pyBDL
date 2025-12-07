from unittest.mock import AsyncMock, patch

import pytest

from pyldb.api.attributes import AttributesAPI
from pyldb.config import LDBConfig


@pytest.fixture
def attributes_api(dummy_config: LDBConfig) -> AttributesAPI:
    return AttributesAPI(dummy_config)


@pytest.mark.asyncio
@patch.object(AttributesAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_alist_attributes(afetch_all_results: AsyncMock, attributes_api: AttributesAPI) -> None:
    afetch_all_results.return_value = [{"id": 1}]
    result = await attributes_api.alist_attributes()
    assert result == [{"id": 1}]


@pytest.mark.asyncio
@patch.object(AttributesAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_attribute(afetch_single_result: AsyncMock, attributes_api: AttributesAPI) -> None:
    afetch_single_result.return_value = {"id": 7}
    result = await attributes_api.aget_attribute("7")
    assert result["id"] == 7


@pytest.mark.asyncio
@patch.object(AttributesAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_attributes_metadata(afetch_single_result: AsyncMock, attributes_api: AttributesAPI) -> None:
    afetch_single_result.return_value = {"info": "meta"}
    result = await attributes_api.aget_attributes_metadata()
    assert result["info"] == "meta"
