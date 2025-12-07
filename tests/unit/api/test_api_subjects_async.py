from unittest.mock import AsyncMock, patch

import pytest

from pybdl.api.subjects import SubjectsAPI
from pybdl.config import BDLConfig


@pytest.fixture
def subjects_api(dummy_config: BDLConfig) -> SubjectsAPI:
    return SubjectsAPI(dummy_config)


@pytest.mark.asyncio
@patch.object(SubjectsAPI, "afetch_all_results", new_callable=AsyncMock)
@patch.object(SubjectsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_alist_subjects_all_branches(
    afetch_single_result: AsyncMock, afetch_all_results: AsyncMock, subjects_api: SubjectsAPI
) -> None:
    # max_pages=None (default, all pages)
    afetch_all_results.return_value = [{"id": "A"}]
    result = await subjects_api.alist_subjects()
    assert result == [{"id": "A"}]
    # max_pages=1 (single page)
    afetch_single_result.return_value = [{"id": "B"}]
    result = await subjects_api.alist_subjects(max_pages=1)
    assert result == [{"id": "B"}]
    # With all filters
    afetch_all_results.return_value = [{"id": "C"}]
    result = await subjects_api.alist_subjects(
        parent_id="P", sort="name", page_size=10, max_pages=2, extra_query={"foo": "bar"}
    )
    assert result == [{"id": "C"}]


@pytest.mark.asyncio
@patch.object(SubjectsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_subject(afetch_single_result: AsyncMock, subjects_api: SubjectsAPI) -> None:
    afetch_single_result.return_value = {"id": "B"}
    result = await subjects_api.aget_subject("B")
    assert result["id"] == "B"


@pytest.mark.asyncio
@patch.object(SubjectsAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_asearch_subjects_all_branches(afetch_all_results: AsyncMock, subjects_api: SubjectsAPI) -> None:
    # With only name
    afetch_all_results.return_value = [{"id": "A"}]
    result = await subjects_api.asearch_subjects(name="foo")
    assert result == [{"id": "A"}]
    # With all filters
    afetch_all_results.return_value = [{"id": "B"}]
    result = await subjects_api.asearch_subjects(name="foo", page_size=10, max_pages=2, extra_query={"bar": "baz"})
    assert result == [{"id": "B"}]


@pytest.mark.asyncio
@patch.object(SubjectsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_subjects_metadata(afetch_single_result: AsyncMock, subjects_api: SubjectsAPI) -> None:
    afetch_single_result.return_value = {"info": "meta"}
    result = await subjects_api.aget_subjects_metadata()
    assert result["info"] == "meta"


class DummyException(Exception):
    pass


@pytest.mark.asyncio
@patch.object(SubjectsAPI, "afetch_all_results", new_callable=AsyncMock)
@patch.object(SubjectsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_alist_subjects_error(
    afetch_single_result: AsyncMock, afetch_all_results: AsyncMock, subjects_api: SubjectsAPI
) -> None:
    afetch_all_results.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await subjects_api.alist_subjects()
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await subjects_api.alist_subjects(max_pages=1)


@pytest.mark.asyncio
@patch.object(SubjectsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_subject_error(afetch_single_result: AsyncMock, subjects_api: SubjectsAPI) -> None:
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await subjects_api.aget_subject("B")


@pytest.mark.asyncio
@patch.object(SubjectsAPI, "afetch_all_results", new_callable=AsyncMock)
async def test_asearch_subjects_error(afetch_all_results: AsyncMock, subjects_api: SubjectsAPI) -> None:
    afetch_all_results.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await subjects_api.asearch_subjects(name="foo")


@pytest.mark.asyncio
@patch.object(SubjectsAPI, "afetch_single_result", new_callable=AsyncMock)
async def test_aget_subjects_metadata_error(afetch_single_result: AsyncMock, subjects_api: SubjectsAPI) -> None:
    afetch_single_result.side_effect = DummyException("fail")
    with pytest.raises(DummyException):
        await subjects_api.aget_subjects_metadata()
