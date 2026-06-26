"""Persistent cache for rate limiter quota usage."""

from __future__ import annotations

import contextlib
import json
import sys
import threading
from collections.abc import Generator, Sequence
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
        self._thread_lock = threading.Lock()
        self._data: dict[str, Any] = {}
        if self.enabled:
            self._ensure_cache_dir()
            with self._interprocess_lock():
                pass

    def _cache_path(self) -> Path:
        return Path(self.cache_file)

    def _lock_path(self) -> Path:
        return self._cache_path().with_suffix(".lock")

    def _ensure_cache_dir(self) -> None:
        self._cache_path().parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def filter_valid_timestamps(
        timestamps: Sequence[float],
        now: float,
        period: int,
        *,
        buffer_seconds: float = 0.05,
    ) -> list[float]:
        """Keep only timestamps that belong to the active quota window."""
        cutoff = now - period
        upper_bound = now + buffer_seconds
        return sorted(
            float(value) for value in timestamps if isinstance(value, int | float) and cutoff < value <= upper_bound
        )

    @contextlib.contextmanager
    def _interprocess_lock(self) -> Generator[None]:
        if not self.enabled:
            yield
            return

        self._ensure_cache_dir()
        with self._lock_path().open("a+", encoding="utf-8") as lock_handle:
            if sys.platform == "win32":
                import msvcrt

                lock_handle.seek(0)
                msvcrt.locking(lock_handle.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    with self._thread_lock:
                        if self._cache_path().exists():
                            self._load()
                        yield
                finally:
                    lock_handle.seek(0)
                    msvcrt.locking(lock_handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                try:
                    with self._thread_lock:
                        if self._cache_path().exists():
                            self._load()
                        yield
                finally:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _load(self) -> None:
        try:
            cache_path = self._cache_path()
            if cache_path.exists():
                with cache_path.open(encoding="utf-8") as handle:
                    self._data = json.load(handle)
        except (json.JSONDecodeError, OSError):
            self._data = {}

    @staticmethod
    def _period_from_key(key: str) -> int | None:
        suffix = key.rsplit("_", 1)[-1]
        return int(suffix) if suffix.isdigit() else None

    def _save_unlocked(self) -> None:
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

    def _save(self) -> None:
        with self._interprocess_lock():
            self._save_unlocked()

    def get(self, key: str) -> Any:
        if not self.enabled:
            return []
        with self._thread_lock:
            value = self._data.get(key, [])
            return list(value) if isinstance(value, list) else value

    def get_valid_timestamps(
        self,
        key: str,
        now: float,
        period: int,
        *,
        buffer_seconds: float = 0.05,
    ) -> list[float]:
        if not self.enabled:
            return []
        with self._interprocess_lock():
            raw = self._data.get(key, [])
            if not isinstance(raw, list):
                return []
            return self.filter_valid_timestamps(raw, now, period, buffer_seconds=buffer_seconds)

    def set(self, key: str, value: Any) -> None:
        if not self.enabled:
            return
        with self._interprocess_lock():
            self._data[key] = value
            self._save_unlocked()

    def _clean_list(
        self,
        key: str,
        *,
        period: int | None = None,
        cleanup_older_than: float | None = None,
    ) -> list[float]:
        """Return cleaned timestamps for live mutations under the interprocess lock."""
        current = list(self._data.get(key, []))
        effective_period = period if period is not None else self._period_from_key(key)

        if cleanup_older_than is not None:
            cutoff = cleanup_older_than
        elif effective_period is not None:
            import time

            cutoff = time.time() - effective_period
        else:
            return [float(value) for value in current if isinstance(value, int | float)]

        return sorted(float(value) for value in current if isinstance(value, int | float) and value > cutoff)

    def try_append_if_under_limit(
        self,
        key: str,
        value: float,
        max_length: int,
        cleanup_older_than: float | None = None,
    ) -> bool:
        if not self.enabled:
            return True
        with self._interprocess_lock():
            current = self._clean_list(key, cleanup_older_than=cleanup_older_than)
            if len(current) >= max_length:
                return False
            current.append(value)
            self._data[key] = current
            self._save_unlocked()
            return True

    def try_record_all_periods(
        self,
        periods_config: Sequence[tuple[str, int, int]],
        value: float,
    ) -> bool:
        """Atomically record one timestamp for every tracked period."""
        if not self.enabled:
            return True

        with self._interprocess_lock():
            staged: dict[str, list[float]] = {}
            for key, max_length, period in periods_config:
                current = self._clean_list(key, period=period)
                if len(current) >= max_length:
                    return False
                staged[key] = current

            for key, _, _ in periods_config:
                staged[key].append(value)
                self._data[key] = staged[key]

            self._save_unlocked()
            return True

    def remove_last_if_matches(self, key: str, value: float) -> bool:
        if not self.enabled:
            return False
        with self._interprocess_lock():
            current = list(self._data.get(key, []))
            for index in range(len(current) - 1, -1, -1):
                if current[index] == value:
                    del current[index]
                    self._data[key] = current
                    self._save_unlocked()
                    return True
            return False

    def remove_from_all_periods(self, keys: Sequence[str], value: float) -> bool:
        """Atomically remove a timestamp from all tracked periods."""
        if not self.enabled:
            return False

        changed = False
        with self._interprocess_lock():
            for key in keys:
                current = list(self._data.get(key, []))
                for index in range(len(current) - 1, -1, -1):
                    if current[index] == value:
                        del current[index]
                        self._data[key] = current
                        changed = True
                        break

            if changed:
                self._save_unlocked()
            return changed
