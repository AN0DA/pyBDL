"""Construct httpx clients with optional hishel HTTP caching."""

from collections.abc import Mapping
from pathlib import Path

import httpx
from hishel import AsyncSqliteStorage, FilterPolicy, SyncSqliteStorage
from hishel.httpx import AsyncCacheClient, SyncCacheClient

from pybdl.config import CacheBackend


def _cache_policy() -> FilterPolicy:
    return FilterPolicy()


def build_sync_http_client(
    *,
    cache_backend: CacheBackend | None,
    http_cache_db_path: Path | None,
    default_headers: Mapping[str, str],
    proxy: str | None,
) -> httpx.Client:
    if cache_backend == "memory":
        return SyncCacheClient(
            headers=default_headers,
            proxy=proxy,
            storage=SyncSqliteStorage(database_path=":memory:"),
            policy=_cache_policy(),
        )
    if cache_backend == "file" and http_cache_db_path is not None:
        return SyncCacheClient(
            headers=default_headers,
            proxy=proxy,
            storage=SyncSqliteStorage(database_path=str(http_cache_db_path)),
            policy=_cache_policy(),
        )
    return httpx.Client(headers=default_headers, proxy=proxy)


def build_async_http_client(
    *,
    cache_backend: CacheBackend | None,
    http_cache_db_path: Path | None,
    default_headers: Mapping[str, str],
    proxy: str | None,
) -> httpx.AsyncClient:
    if cache_backend == "memory":
        return AsyncCacheClient(
            headers=default_headers,
            proxy=proxy,
            storage=AsyncSqliteStorage(database_path=":memory:"),
            policy=_cache_policy(),
        )
    if cache_backend == "file" and http_cache_db_path is not None:
        return AsyncCacheClient(
            headers=default_headers,
            proxy=proxy,
            storage=AsyncSqliteStorage(database_path=str(http_cache_db_path)),
            policy=_cache_policy(),
        )
    return httpx.AsyncClient(headers=default_headers, proxy=proxy)
