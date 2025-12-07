"""Persistent cache for API quota usage."""

import asyncio
import json
import threading
from pathlib import Path
from typing import Any

from pybdl.utils.cache import get_cache_file_path


class PersistentQuotaCache:
    """
    Persistent cache for API quota usage, stored on disk.

    This class provides thread-safe, persistent storage for quota usage data,
    allowing rate limiters to survive process restarts and share state between sessions.
    """

    def __init__(self, enabled: bool = True) -> None:
        """
        Initialize the persistent quota cache.

        Args:
            enabled: Whether to enable persistent caching.
        """
        self.enabled = enabled
        cache_path = get_cache_file_path("quota_cache.json")
        self.cache_file: Path | str = Path(cache_path) if isinstance(cache_path, str) else cache_path
        self._lock = threading.Lock()
        self._async_lock: asyncio.Lock | None = None  # Lazy initialization for async
        self._data: dict[str, Any] = {}
        if self.enabled:
            self._ensure_cache_dir()
            self._load()

    def _ensure_cache_dir(self) -> None:
        """Ensure cache directory exists."""
        # Handle case where cache_file might be set as string (e.g., in tests)
        cache_path = Path(self.cache_file) if isinstance(self.cache_file, str) else self.cache_file
        cache_path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> None:
        """
        Load quota data from the cache file.
        """
        try:
            # Handle case where cache_file might be set as string (e.g., in tests)
            cache_path = Path(self.cache_file) if isinstance(self.cache_file, str) else self.cache_file
            if cache_path.exists():
                with open(cache_path) as f:
                    self._data = json.load(f)
        except (json.JSONDecodeError, OSError):
            # Log warning but continue with empty cache
            self._data = {}

    def _save(self) -> None:
        """
        Save quota data to the cache file.
        """
        if not self.enabled:
            return

        try:
            # Handle case where cache_file might be set as string (e.g., in tests)
            cache_path = Path(self.cache_file) if isinstance(self.cache_file, str) else self.cache_file
            # Write to temp file first, then rename (atomic operation)
            temp_file = cache_path.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(self._data, f)
            temp_file.replace(cache_path)
        except OSError:
            # Non-fatal: log but don't crash
            pass

    def get(self, key: str) -> Any:
        """
        Retrieve a cached value by key.

        Args:
            key: Cache key.
        Returns:
            Cached value, or [] if not found or disabled.
        """
        if not self.enabled:
            return []
        with self._lock:
            return self._data.get(key, [])

    def set(self, key: str, value: Any) -> None:
        """
        Set a cached value by key and persist it.

        Args:
            key: Cache key.
            value: Value to store.
        """
        if not self.enabled:
            return
        with self._lock:
            self._data[key] = value
            self._save()

    def try_append_if_under_limit(
        self, key: str, value: float, max_length: int, cleanup_older_than: float | None = None
    ) -> bool:
        """
        Atomically try to append a value to a cached list if it wouldn't exceed max_length.

        This prevents race conditions when multiple limiters try to record calls simultaneously.
        The entire operation (get, check, append, save) happens atomically under the cache lock.

        Args:
            key: Cache key.
            value: Value to append (typically a timestamp).
            max_length: Maximum length allowed.
            cleanup_older_than: If provided, remove values older than this timestamp.

        Returns:
            True if append succeeded, False if it would exceed the limit.
        """
        if not self.enabled:
            return True  # If cache disabled, allow the append
        with self._lock:
            current = list(self._data.get(key, []))

            # Cleanup old values if requested
            if cleanup_older_than is not None:
                current = [v for v in current if isinstance(v, (int, float)) and v > cleanup_older_than]

            # Check if we can append
            if len(current) >= max_length:
                return False

            # Append and save atomically
            current.append(value)
            self._data[key] = current
            self._save()
            return True
