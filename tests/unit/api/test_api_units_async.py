import pytest

from pyldb.api.units import UnitsAPI
from pyldb.config import LDBConfig


@pytest.fixture
def async_units_api() -> UnitsAPI:
    return UnitsAPI(LDBConfig(api_key="dummy"))


@pytest.mark.asyncio
async def test_alist_units(monkeypatch: pytest.MonkeyPatch, async_units_api: UnitsAPI) -> None:
    async def fake(*args: object, **kwargs: object) -> object:
        yield {"results": [{"id": "PL", "name": "Poland"}]}

    monkeypatch.setattr(async_units_api, "_paginated_request_async", fake)
    results = await async_units_api.alist_units()
    assert results[0]["id"] == "PL"


@pytest.mark.asyncio
async def test_aget_unit(monkeypatch: pytest.MonkeyPatch, async_units_api: UnitsAPI) -> None:
    async def fake(*args: object, **kwargs: object) -> dict[str, str]:
        return {"id": "PL", "name": "Poland"}

    monkeypatch.setattr(async_units_api, "_request_async", fake)
    result = await async_units_api.aget_unit("PL")
    assert result["id"] == "PL"


@pytest.mark.asyncio
async def test_asearch_units(monkeypatch: pytest.MonkeyPatch, async_units_api: UnitsAPI) -> None:
    async def fake(*args: object, **kwargs: object) -> object:
        yield {"results": [{"id": "WAW", "name": "Warsaw"}]}

    monkeypatch.setattr(async_units_api, "_paginated_request_async", fake)
    results = await async_units_api.asearch_units(name="Warsaw")
    assert results[0]["id"] == "WAW"


@pytest.mark.asyncio
async def test_alist_localities(monkeypatch: pytest.MonkeyPatch, async_units_api: UnitsAPI) -> None:
    async def fake(*args: object, **kwargs: object) -> object:
        yield {"results": [{"id": "L1", "name": "Loc1"}]}

    monkeypatch.setattr(async_units_api, "_paginated_request_async", fake)
    results = await async_units_api.alist_localities(parent_id="PL")
    assert results[0]["id"] == "L1"


@pytest.mark.asyncio
async def test_aget_locality(monkeypatch: pytest.MonkeyPatch, async_units_api: UnitsAPI) -> None:
    async def fake(*args: object, **kwargs: object) -> dict[str, str]:
        return {"id": "L1", "name": "Loc1"}

    monkeypatch.setattr(async_units_api, "_request_async", fake)
    result = await async_units_api.aget_locality("L1")
    assert result["id"] == "L1"


@pytest.mark.asyncio
async def test_asearch_localities(monkeypatch: pytest.MonkeyPatch, async_units_api: UnitsAPI) -> None:
    async def fake(*args: object, **kwargs: object) -> object:
        yield {"results": [{"id": "L2", "name": "Loc2"}]}

    monkeypatch.setattr(async_units_api, "_paginated_request_async", fake)
    results = await async_units_api.asearch_localities(name="Loc2")
    assert results[0]["id"] == "L2"


@pytest.mark.asyncio
async def test_aget_units_metadata(monkeypatch: pytest.MonkeyPatch, async_units_api: UnitsAPI) -> None:
    async def fake(*args: object, **kwargs: object) -> dict[str, str]:
        return {"info": "Units API"}

    monkeypatch.setattr(async_units_api, "_request_async", fake)
    result = await async_units_api.aget_units_metadata()
    assert result["info"] == "Units API"
