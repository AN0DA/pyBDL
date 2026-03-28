"""Persistent cache for rate limiter quota usage."""

import json
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from pybdl.utils.cache import resolve_cache_file_path


class PersistentQuotaCache:
    """Thread-safe persistent storage for rate limiter timestamps."""

    def __init__(
        self,
        enabled: bool = True,
        *,
        cache_file: str | Path | None = None,
        use_global_cache: bool = False,
    ) -> None:
        self.enabled = enabled
        self.cache_file = Path(
            resolve_cache_file_path(
                "quota_cache.json",
                use_global_cache=use_global_cache,
                custom_file=str(cache_file) if cache_file is not None else None,
            )
        )
        self._lock = threading.Lock()
        self._data: dict[str, Any] = {}
        if self.enabled:
            self._ensure_cache_dir()
            self._load()

    def _cache_path(self) -> Path:
        return Path(self.cache_file)

    def _ensure_cache_dir(self) -> None:
        self._cache_path().parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> None:
        try:
            cache_path = self._cache_path()
            if cache_path.exists():
                with cache_path.open(encoding="utf-8") as handle:
                    self._data = json.load(handle)
        except (json.JSONDecodeError, OSError):
            self._data = {}

    def _save(self) -> None:
        if not self.enabled:
            return

        try:
            cache_path = self._cache_path()
            temp_file = cache_path.with_suffix(".tmp")
            with temp_file.open("w", encoding="utf-8") as handle:
                json.dump(self._data, handle)
            temp_file.replace(cache_path)
        except OSError:
            pass

    def get(self, key: str) -> Any:
        if not self.enabled:
            return []
        with self._lock:
            value = self._data.get(key, [])
            return list(value) if isinstance(value, list) else value

    def set(self, key: str, value: Any) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._data[key] = value
            self._save()

    def _clean_list(self, key: str, cleanup_older_than: float | None) -> list[float]:
        current = list(self._data.get(key, []))
        if cleanup_older_than is None:
            return current
        return [v for v in current if isinstance(v, int | float) and v > cleanup_older_than]

    def try_append_if_under_limit(
        self,
        key: str,
        value: float,
        max_length: int,
        cleanup_older_than: float | None = None,
    ) -> bool:
        if not self.enabled:
            return True
        with self._lock:
            current = self._clean_list(key, cleanup_older_than)
            if len(current) >= max_length:
                return False
            current.append(value)
            self._data[key] = current
            self._save()
            return True

    def try_record_all_periods(
        self,
        periods_config: Sequence[tuple[str, int, float | None]],
        value: float,
    ) -> bool:
        """Atomically record one timestamp for every tracked period."""
        if not self.enabled:
            return True

        with self._lock:
            staged: dict[str, list[float]] = {}
            for key, max_length, cleanup_older_than in periods_config:
                current = self._clean_list(key, cleanup_older_than)
                if len(current) >= max_length:
                    return False
                staged[key] = current

            for key, _, _ in periods_config:
                staged[key].append(value)
                self._data[key] = staged[key]

            self._save()
            return True

    def remove_last_if_matches(self, key: str, value: float) -> bool:
        if not self.enabled:
            return False
        with self._lock:
            current = list(self._data.get(key, []))
            for index in range(len(current) - 1, -1, -1):
                if current[index] == value:
                    del current[index]
                    self._data[key] = current
                    self._save()
                    return True
            return False

    def remove_from_all_periods(self, keys: Sequence[str], value: float) -> bool:
        """Atomically remove a timestamp from all tracked periods."""
        if not self.enabled:
            return False

        changed = False
        with self._lock:
            for key in keys:
                current = list(self._data.get(key, []))
                for index in range(len(current) - 1, -1, -1):
                    if current[index] == value:
                        del current[index]
                        self._data[key] = current
                        changed = True
                        break

            if changed:
                self._save()
            return changed
