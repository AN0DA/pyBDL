"""HTTP cache file location next to the rate-limit quota cache."""

from pathlib import Path

from pybdl.config import CacheBackend


def resolve_http_cache_db_path(cache_backend: CacheBackend | None, quota_cache_file: Path) -> Path | None:
    """Return path to shared SQLite DB for hishel when using the file backend."""
    if cache_backend != "file":
        return None
    return quota_cache_file.with_name("http_cache.db")
