from pathlib import Path

import httpx
import pytest
import respx
from hishel.httpx import AsyncCacheClient, SyncCacheClient

from pybdl.api.client import BaseAPIClient
from pybdl.config import BDLConfig, Language
from pybdl.utils.rate_limiter import PersistentQuotaCache


def _build_config(
    *,
    cache_backend: str | None,
    quota_cache_file: Path | None = None,
) -> BDLConfig:
    return BDLConfig(
        api_key="dummy-api-key",
        language=Language.EN,
        cache_backend=cache_backend,
        cache_expire_after=600,
        custom_quotas={1: 2},
        quota_cache_enabled=False,
        quota_cache_file=str(quota_cache_file) if quota_cache_file is not None else None,
    )


@pytest.mark.unit
@pytest.mark.real_rate_limiting
@respx.mock
def test_cache_backend_memory_no_file_created(tmp_path: Path) -> None:
    respx.get("https://bdl.stat.gov.pl/api/v1/data/cache-memory?lang=en").mock(
        return_value=httpx.Response(200, json={"results": [{"id": 1}]})
    )
    client = BaseAPIClient(_build_config(cache_backend="memory", quota_cache_file=tmp_path / "quota_cache.json"))

    assert client._http_cache_path is None
    assert isinstance(client.session, SyncCacheClient)
    assert isinstance(client._async_client, AsyncCacheClient)

    first = client._request_sync("data/cache-memory")
    second = client._request_sync("data/cache-memory")

    assert first == second == {"results": [{"id": 1}]}
    assert not (tmp_path / "http_cache.db").exists()


@pytest.mark.unit
@pytest.mark.real_rate_limiting
@pytest.mark.asyncio
@respx.mock
async def test_cache_backend_file_shared_between_sync_and_async(tmp_path: Path) -> None:
    quota_cache_file = tmp_path / "quota_cache.json"
    respx.get("https://bdl.stat.gov.pl/api/v1/data/cache-file?lang=en").mock(
        return_value=httpx.Response(200, json={"results": [{"id": 1}]})
    )
    client = BaseAPIClient(_build_config(cache_backend="file", quota_cache_file=quota_cache_file))

    sync_result = client._request_sync("data/cache-file")
    async_result = await client._request_async("data/cache-file")

    assert sync_result == async_result == {"results": [{"id": 1}]}
    assert isinstance(client.session, SyncCacheClient)
    assert isinstance(client._async_client, AsyncCacheClient)
    assert client._http_cache_path == quota_cache_file.with_name("http_cache.db")
    assert client._http_cache_path.exists()


@pytest.mark.unit
@pytest.mark.real_rate_limiting
def test_quota_not_consumed_on_cache_hit_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    client = BaseAPIClient(_build_config(cache_backend=None))

    responses = [
        httpx.Response(
            200,
            json={"results": [{"id": 1}]},
            request=httpx.Request("GET", "https://bdl.stat.gov.pl/api/v1/data/cache-quota-sync?lang=en"),
            extensions={"hishel_from_cache": False},
        ),
        httpx.Response(
            200,
            json={"results": [{"id": 1}]},
            request=httpx.Request("GET", "https://bdl.stat.gov.pl/api/v1/data/cache-quota-sync?lang=en"),
            extensions={"hishel_from_cache": True},
        ),
    ]

    def _request(*args: object, **kwargs: object) -> httpx.Response:
        return responses.pop(0)

    monkeypatch.setattr(client.session, "request", _request)

    assert client._sync_limiter.get_remaining_quota()[1] == 2
    client._request_sync("data/cache-quota-sync")
    after_miss = client._sync_limiter.get_remaining_quota()[1]
    client._request_sync("data/cache-quota-sync")
    after_hit = client._sync_limiter.get_remaining_quota()[1]

    assert after_miss == 1
    assert after_hit == 1


@pytest.mark.unit
@pytest.mark.real_rate_limiting
@pytest.mark.asyncio
async def test_quota_not_consumed_on_cache_hit_async(monkeypatch: pytest.MonkeyPatch) -> None:
    client = BaseAPIClient(_build_config(cache_backend=None))

    responses = [
        httpx.Response(
            200,
            json={"results": [{"id": 1}]},
            request=httpx.Request("GET", "https://bdl.stat.gov.pl/api/v1/data/cache-quota-async?lang=en"),
            extensions={"hishel_from_cache": False},
        ),
        httpx.Response(
            200,
            json={"results": [{"id": 1}]},
            request=httpx.Request("GET", "https://bdl.stat.gov.pl/api/v1/data/cache-quota-async?lang=en"),
            extensions={"hishel_from_cache": True},
        ),
    ]

    async def _request(*args: object, **kwargs: object) -> httpx.Response:
        return responses.pop(0)

    monkeypatch.setattr(client._async_client, "request", _request)

    assert (await client._async_limiter.get_remaining_quota_async())[1] == 2
    await client._request_async("data/cache-quota-async")
    after_miss = (await client._async_limiter.get_remaining_quota_async())[1]
    await client._request_async("data/cache-quota-async")
    after_hit = (await client._async_limiter.get_remaining_quota_async())[1]

    assert after_miss == 1
    assert after_hit == 1


@pytest.mark.unit
def test_persistent_quota_cache_corrupt_json_starts_empty(tmp_path: Path) -> None:
    """Corrupt cache file is ignored; in-memory state is empty."""
    cache_file = tmp_path / "quota_cache.json"
    cache_file.write_text("not valid json {{{", encoding="utf-8")
    cache = PersistentQuotaCache(enabled=True, cache_file=cache_file)
    assert cache._data == {}


@pytest.mark.unit
def test_try_append_cleanup_drops_stale_timestamps(tmp_path: Path) -> None:
    """cleanup_older_than removes expired entries before length check."""
    cache_file = tmp_path / "quota_cache.json"
    cache = PersistentQuotaCache(enabled=True, cache_file=cache_file)
    key = "k1"
    cache.set(key, [1.0, 2.0, 100.0])
    # After cleanup (keep only > 50), two entries remain; max_length=2 allows one more
    assert cache.try_append_if_under_limit(key, 200.0, max_length=2, cleanup_older_than=50.0) is True
    with cache._lock:
        assert 200.0 in cache._data[key]


@pytest.mark.unit
def test_remove_last_if_matches_found_and_not_found(tmp_path: Path) -> None:
    cache_file = tmp_path / "quota_cache.json"
    cache = PersistentQuotaCache(enabled=True, cache_file=cache_file)
    key = "k2"
    cache.set(key, [1.0, 2.0, 3.0])
    assert cache.remove_last_if_matches(key, 2.0) is True
    with cache._lock:
        assert cache._data[key] == [1.0, 3.0]
    assert cache.remove_last_if_matches(key, 99.0) is False


@pytest.mark.unit
def test_remove_last_if_matches_disabled_returns_false(tmp_path: Path) -> None:
    cache_file = tmp_path / "quota_cache.json"
    cache = PersistentQuotaCache(enabled=False, cache_file=cache_file)
    cache._data = {"k": [1.0]}
    assert cache.remove_last_if_matches("k", 1.0) is False
