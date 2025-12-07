from typing import Any

import pytest
import responses

from pyldb.api.version import VersionAPI
from pyldb.config import LDBConfig


@pytest.fixture
def version_api(dummy_config: LDBConfig) -> VersionAPI:
    return VersionAPI(dummy_config)


@responses.activate
@pytest.mark.unit
def test_get_version(version_api: VersionAPI, api_url: str) -> None:
    url = f"{api_url}/version?lang=en&format=json"
    payload = {"version": "1.2.3", "build": "2025-06-11"}
    responses.add(responses.GET, url, json=payload, status=200)
    result = version_api.get_version()
    assert result["version"] == "1.2.3"
    assert result["build"] == "2025-06-11"


@pytest.mark.asyncio
async def test_aget_version(monkeypatch: pytest.MonkeyPatch) -> None:
    api = VersionAPI(LDBConfig(api_key="dummy"))

    async def fake_afetch_single_result(endpoint: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None, results_key: str | None = None) -> dict[str, str]:
        assert endpoint == "version"
        return {"version": "2.0.0", "build": "future"}

    monkeypatch.setattr(api, "afetch_single_result", fake_afetch_single_result)
    result = await api.aget_version()
    assert result["version"] == "2.0.0"
    assert result["build"] == "future"
