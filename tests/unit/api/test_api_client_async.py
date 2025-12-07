import pytest

from pyldb.api.client import BaseAPIClient
from pyldb.config import LDBConfig


@pytest.fixture
def async_client() -> BaseAPIClient:
    config = LDBConfig(api_key="dummy-api-key")
    return BaseAPIClient(config)


@pytest.mark.asyncio
async def test_async_fetch_all_results_return_metadata(
    async_client: BaseAPIClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Patch _paginated_request_async to yield two pages with metadata
    async def fake_paginated(*args: object, **kwargs: object) -> object:
        yield {"results": [{"id": 1}], "meta": {"foo": "bar"}, "totalCount": 2}
        yield {"results": [{"id": 2}], "meta": {"foo": "baz"}, "totalCount": 2}

    monkeypatch.setattr(async_client, "_paginated_request_async", fake_paginated)
    results, metadata = await async_client.afetch_all_results(
        "data/meta", results_key="results", page_size=2, return_metadata=True, show_progress=False
    )
    assert results == [{"id": 1}, {"id": 2}]
    assert metadata == {"meta": {"foo": "bar"}, "totalCount": 2}


@pytest.mark.asyncio
async def test_async_fetch_all_results_missing_results_key_raises(
    async_client: BaseAPIClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_paginated(*args: object, **kwargs: object) -> object:
        yield {"notresults": []}

    monkeypatch.setattr(async_client, "_paginated_request_async", fake_paginated)
    with pytest.raises(ValueError):
        await async_client.afetch_all_results("data/bad", results_key="results", page_size=2, show_progress=False)


@pytest.mark.asyncio
async def test_async_request_error(monkeypatch: pytest.MonkeyPatch, async_client: BaseAPIClient) -> None:
    class DummyResponse:
        status_code = 500

        def raise_for_status(self) -> None:
            raise Exception("fail")

        def json(self) -> None:
            raise Exception("bad json")

        @property
        def text(self) -> str:
            return "plain text error"

    class DummyException(Exception):
        pass

    async def fake_request_async(*a: object, **k: object) -> object:
        raise DummyException("fail")

    monkeypatch.setattr(async_client, "_request_async", fake_request_async)
    with pytest.raises(DummyException):
        await async_client._request_async("endpoint")


@pytest.mark.asyncio
async def test_async_paginated_request_missing_results_key(
    monkeypatch: pytest.MonkeyPatch, async_client: BaseAPIClient
) -> None:
    async def fake_request_async(*a: object, **k: object) -> dict[str, object]:
        return {"notresults": []}

    monkeypatch.setattr(async_client, "_request_async", fake_request_async)
    it = async_client._paginated_request_async("endpoint", results_key="results")
    with pytest.raises(StopAsyncIteration):
        await it.__anext__()


@pytest.mark.asyncio
async def test_afetch_all_results_metadata_and_error(
    monkeypatch: pytest.MonkeyPatch, async_client: BaseAPIClient
) -> None:
    async def fake_paginated(*args: object, **kwargs: object) -> object:
        yield {"results": [{"id": 1}], "meta": {"foo": "bar"}, "totalCount": 1}

    monkeypatch.setattr(async_client, "_paginated_request_async", fake_paginated)
    # With metadata
    results, meta = await async_client.afetch_all_results(
        "endpoint", results_key="results", return_metadata=True, show_progress=False
    )
    assert results == [{"id": 1}]
    assert meta == {"meta": {"foo": "bar"}, "totalCount": 1}
    # Without metadata
    results2 = await async_client.afetch_all_results("endpoint", results_key="results", show_progress=False)
    assert results2 == [{"id": 1}]

    # Missing results_key
    async def fake_bad(*args: object, **kwargs: object) -> object:
        yield {"notresults": []}

    monkeypatch.setattr(async_client, "_paginated_request_async", fake_bad)
    with pytest.raises(ValueError):
        await async_client.afetch_all_results("endpoint", results_key="results", show_progress=False)


@pytest.mark.asyncio
async def test_afetch_all_results_progress_bar(monkeypatch: pytest.MonkeyPatch, async_client: BaseAPIClient) -> None:
    class DummyBar:
        def __init__(self, *a: object, **k: object):
            self.total: int | None = None
            self.closed = False

        def update(self, n: int) -> None:
            self.total = n

        def set_postfix(self, d: dict) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr("pyldb.api.client.tqdm", DummyBar)

    async def fake_paginated(*args: object, **kwargs: object) -> object:
        yield {"results": [{"id": 1}], "totalCount": 1}

    monkeypatch.setattr(async_client, "_paginated_request_async", fake_paginated)
    results = await async_client.afetch_all_results("endpoint", results_key="results", show_progress=True)
    assert results == [{"id": 1}]


@pytest.mark.asyncio
async def test_afetch_single_result_metadata_and_error(
    monkeypatch: pytest.MonkeyPatch, async_client: BaseAPIClient
) -> None:
    async def fake_request_async(*args: object, **kwargs: object) -> dict[str, object]:
        return {"results": [{"id": 1}], "meta": {"foo": "bar"}}

    monkeypatch.setattr(async_client, "_request_async", fake_request_async)
    # With metadata
    results, meta = await async_client.afetch_single_result("endpoint", results_key="results", return_metadata=True)
    assert results == [{"id": 1}]
    assert meta == {"meta": {"foo": "bar"}}
    # Without metadata
    results2 = await async_client.afetch_single_result("endpoint", results_key="results")
    assert results2 == [{"id": 1}]

    # Missing results_key
    async def fake_bad(*args: object, **kwargs: object) -> dict[str, object]:
        return {"notresults": []}

    monkeypatch.setattr(async_client, "_request_async", fake_bad)
    with pytest.raises(ValueError):
        await async_client.afetch_single_result("endpoint", results_key="results")
