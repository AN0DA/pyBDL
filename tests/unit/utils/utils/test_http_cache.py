from pathlib import Path

import httpx
import pytest
from hishel.httpx import AsyncCacheClient, SyncCacheClient

from pybdl.utils.http_cache import (
    build_async_http_client,
    build_sync_http_client,
    is_from_http_cache,
    resolve_http_cache_db_path,
)

_DEFAULT_HEADERS: dict[str, str] = {"Content-Type": "application/json"}


@pytest.mark.unit
def test_resolve_http_cache_db_path_file_backend(tmp_path: Path) -> None:
    quota = tmp_path / "quota_cache.json"
    resolved = resolve_http_cache_db_path("file", quota)
    assert resolved == tmp_path / "http_cache.db"


@pytest.mark.unit
def test_resolve_http_cache_db_path_non_file_returns_none(tmp_path: Path) -> None:
    quota = tmp_path / "quota_cache.json"
    assert resolve_http_cache_db_path(None, quota) is None
    assert resolve_http_cache_db_path("memory", quota) is None


@pytest.mark.unit
def test_build_sync_http_client_no_cache() -> None:
    client = build_sync_http_client(
        cache_backend=None,
        http_cache_db_path=None,
        default_headers=_DEFAULT_HEADERS,
        proxy=None,
    )
    try:
        assert type(client) is httpx.Client
    finally:
        client.close()


@pytest.mark.unit
def test_build_sync_http_client_memory() -> None:
    client = build_sync_http_client(
        cache_backend="memory",
        http_cache_db_path=None,
        default_headers=_DEFAULT_HEADERS,
        proxy=None,
    )
    try:
        assert isinstance(client, SyncCacheClient)
    finally:
        client.close()


@pytest.mark.unit
def test_build_sync_http_client_file(tmp_path: Path) -> None:
    db_path = tmp_path / "http_cache.db"
    client = build_sync_http_client(
        cache_backend="file",
        http_cache_db_path=db_path,
        default_headers=_DEFAULT_HEADERS,
        proxy=None,
    )
    try:
        assert isinstance(client, SyncCacheClient)
    finally:
        client.close()


@pytest.mark.unit
def test_build_sync_http_client_file_without_path_falls_back_to_plain_httpx() -> None:
    client = build_sync_http_client(
        cache_backend="file",
        http_cache_db_path=None,
        default_headers=_DEFAULT_HEADERS,
        proxy=None,
    )
    try:
        assert type(client) is httpx.Client
    finally:
        client.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_build_async_http_client_no_cache() -> None:
    client = build_async_http_client(
        cache_backend=None,
        http_cache_db_path=None,
        default_headers=_DEFAULT_HEADERS,
        proxy=None,
    )
    try:
        assert type(client) is httpx.AsyncClient
    finally:
        await client.aclose()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_build_async_http_client_memory() -> None:
    client = build_async_http_client(
        cache_backend="memory",
        http_cache_db_path=None,
        default_headers=_DEFAULT_HEADERS,
        proxy=None,
    )
    try:
        assert isinstance(client, AsyncCacheClient)
    finally:
        await client.aclose()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_build_async_http_client_file(tmp_path: Path) -> None:
    db_path = tmp_path / "http_cache.db"
    client = build_async_http_client(
        cache_backend="file",
        http_cache_db_path=db_path,
        default_headers=_DEFAULT_HEADERS,
        proxy=None,
    )
    try:
        assert isinstance(client, AsyncCacheClient)
    finally:
        await client.aclose()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_build_async_http_client_file_without_path_falls_back_to_plain_httpx() -> None:
    client = build_async_http_client(
        cache_backend="file",
        http_cache_db_path=None,
        default_headers=_DEFAULT_HEADERS,
        proxy=None,
    )
    try:
        assert type(client) is httpx.AsyncClient
    finally:
        await client.aclose()


@pytest.mark.unit
def test_is_from_http_cache_true_and_false() -> None:
    req = httpx.Request("GET", "https://example.test/")
    hit = httpx.Response(200, request=req, extensions={"hishel_from_cache": True})
    miss = httpx.Response(200, request=req, extensions={"hishel_from_cache": False})
    assert is_from_http_cache(hit) is True
    assert is_from_http_cache(miss) is False


@pytest.mark.unit
def test_is_from_http_cache_missing_extension() -> None:
    req = httpx.Request("GET", "https://example.test/")
    response = httpx.Response(200, request=req)
    assert is_from_http_cache(response) is False
