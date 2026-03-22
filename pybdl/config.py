import enum
import json
import os
from dataclasses import dataclass, field
from typing import Any

# API Constants
BDL_API_BASE_URL = "https://bdl.stat.gov.pl/api/v1"

# Sentinel value to distinguish "not provided" from "explicitly None"
_NOT_PROVIDED = object()


class Language(enum.Enum):
    PL = "pl"
    EN = "en"


class Format(enum.Enum):
    JSON = "json"
    JSONAPI = "jsonapi"
    XML = "xml"


DEFAULT_LANGUAGE = Language.EN
DEFAULT_FORMAT = Format.JSON
DEFAULT_CACHE_EXPIRY = 3600  # 1 hour in seconds
DEFAULT_PAGE_SIZE = 100
DEFAULT_REQUEST_RETRIES = 3
DEFAULT_RETRY_BACKOFF_FACTOR = 0.5
DEFAULT_MAX_RETRY_DELAY = 30.0
DEFAULT_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)

# Define constant quota periods (in seconds)
QUOTA_PERIODS = {"1s": 1, "15m": 15 * 60, "12h": 12 * 3600, "7d": 7 * 24 * 3600}
DEFAULT_QUOTAS = {  # (anon_limit, registered_limit)
    QUOTA_PERIODS["1s"]: (5, 10),
    QUOTA_PERIODS["15m"]: (100, 500),
    QUOTA_PERIODS["12h"]: (1000, 5000),
    QUOTA_PERIODS["7d"]: (10000, 50000),
}


@dataclass(init=False)
class BDLConfig:
    """
    Configuration for the BDL API client.

    This dataclass manages all configuration options for the BDL API client, supporting
    direct parameter passing, environment variable overrides, and sensible defaults.

    Attributes:
        api_key: API key for authentication (optional, None for anonymous access).
        language: Language code for API responses (default: "en").
        format: Response format (default: "json").
        use_cache: Whether to use request caching (default: True).
        cache_expire_after: Cache expiration time in seconds (default: 3600).
        proxy_url: Optional URL of the proxy server.
        proxy_username: Optional username for proxy authentication.
        proxy_password: Optional password for proxy authentication.
        custom_quotas: Optional custom quota dictionary (period: int).
        quota_cache_enabled: Enable persistent quota cache (default: True).
        quota_cache_file: Path to quota cache file (default: project .cache/pybdl).
        use_global_cache: Store quota cache in OS-specific location (default: False).
        page_size: Default page size for paginated requests (default: 100).
        request_retries: Number of retry attempts for transient HTTP errors (default: 3).
        retry_backoff_factor: Base backoff factor in seconds for retries (default: 0.5).
        max_retry_delay: Maximum time to wait between retries in seconds (default: 30).
        retry_status_codes: HTTP status codes that should be retried.
    """

    api_key: str | None
    language: Language
    format: Format
    use_cache: bool
    cache_expire_after: int
    proxy_url: str | None
    proxy_username: str | None
    proxy_password: str | None
    custom_quotas: dict[int, int] | None
    quota_cache_enabled: bool
    quota_cache_file: str | None
    use_global_cache: bool
    page_size: int
    request_retries: int
    retry_backoff_factor: float
    max_retry_delay: float
    retry_status_codes: tuple[int, ...]
    _provided_fields: set[str] = field(default_factory=set, init=False, repr=False)

    def __init__(
        self,
        api_key: str | None | object = _NOT_PROVIDED,
        language: Language | str | object = _NOT_PROVIDED,
        format: Format | str | object = _NOT_PROVIDED,
        use_cache: bool | object = _NOT_PROVIDED,
        cache_expire_after: int | object = _NOT_PROVIDED,
        proxy_url: str | None | object = _NOT_PROVIDED,
        proxy_username: str | None | object = _NOT_PROVIDED,
        proxy_password: str | None | object = _NOT_PROVIDED,
        custom_quotas: dict[int, int] | None | object = _NOT_PROVIDED,
        quota_cache_enabled: bool | object = _NOT_PROVIDED,
        quota_cache_file: str | None | object = _NOT_PROVIDED,
        use_global_cache: bool | object = _NOT_PROVIDED,
        page_size: int | object = _NOT_PROVIDED,
        request_retries: int | object = _NOT_PROVIDED,
        retry_backoff_factor: float | object = _NOT_PROVIDED,
        max_retry_delay: float | object = _NOT_PROVIDED,
        retry_status_codes: tuple[int, ...] | list[int] | object = _NOT_PROVIDED,
    ) -> None:
        self._provided_fields = {
            field_name
            for field_name, value in {
                "api_key": api_key,
                "language": language,
                "format": format,
                "use_cache": use_cache,
                "cache_expire_after": cache_expire_after,
                "proxy_url": proxy_url,
                "proxy_username": proxy_username,
                "proxy_password": proxy_password,
                "custom_quotas": custom_quotas,
                "quota_cache_enabled": quota_cache_enabled,
                "quota_cache_file": quota_cache_file,
                "use_global_cache": use_global_cache,
                "page_size": page_size,
                "request_retries": request_retries,
                "retry_backoff_factor": retry_backoff_factor,
                "max_retry_delay": max_retry_delay,
                "retry_status_codes": retry_status_codes,
            }.items()
            if value is not _NOT_PROVIDED
        }

        self.api_key = self._resolve_optional_str("api_key", api_key, "BDL_API_KEY")
        self.language = self._parse_language(
            self._resolve_value("language", language, "BDL_LANGUAGE", DEFAULT_LANGUAGE.value),
            source="BDL_LANGUAGE" if "language" not in self._provided_fields and os.getenv("BDL_LANGUAGE") else "language",
        )
        self.format = self._parse_format(
            self._resolve_value("format", format, "BDL_FORMAT", DEFAULT_FORMAT.value),
            source="BDL_FORMAT" if "format" not in self._provided_fields and os.getenv("BDL_FORMAT") else "format",
        )
        self.use_cache = self._resolve_bool("use_cache", use_cache, "BDL_USE_CACHE", True)
        self.cache_expire_after = self._resolve_int(
            "cache_expire_after",
            cache_expire_after,
            "BDL_CACHE_EXPIRY",
            DEFAULT_CACHE_EXPIRY,
        )
        self.proxy_url = self._resolve_optional_str("proxy_url", proxy_url, "BDL_PROXY_URL")
        self.proxy_username = self._resolve_optional_str("proxy_username", proxy_username, "BDL_PROXY_USERNAME")
        self.proxy_password = self._resolve_optional_str("proxy_password", proxy_password, "BDL_PROXY_PASSWORD")
        self.quota_cache_enabled = self._resolve_bool(
            "quota_cache_enabled",
            quota_cache_enabled,
            "BDL_QUOTA_CACHE_ENABLED",
            True,
        )
        self.quota_cache_file = self._resolve_optional_str("quota_cache_file", quota_cache_file, "BDL_QUOTA_CACHE")
        self.use_global_cache = self._resolve_bool(
            "use_global_cache",
            use_global_cache,
            "BDL_USE_GLOBAL_CACHE",
            False,
        )
        self.page_size = self._resolve_int("page_size", page_size, "BDL_PAGE_SIZE", DEFAULT_PAGE_SIZE)
        self.request_retries = self._resolve_int(
            "request_retries",
            request_retries,
            "BDL_REQUEST_RETRIES",
            DEFAULT_REQUEST_RETRIES,
        )
        self.retry_backoff_factor = self._resolve_float(
            "retry_backoff_factor",
            retry_backoff_factor,
            "BDL_RETRY_BACKOFF_FACTOR",
            DEFAULT_RETRY_BACKOFF_FACTOR,
        )
        self.max_retry_delay = self._resolve_float(
            "max_retry_delay",
            max_retry_delay,
            "BDL_MAX_RETRY_DELAY",
            DEFAULT_MAX_RETRY_DELAY,
        )
        self.retry_status_codes = self._resolve_retry_status_codes(retry_status_codes)
        self.custom_quotas = self._resolve_custom_quotas(custom_quotas)

        if self.page_size <= 0:
            raise ValueError("page_size must be a positive integer")
        if self.cache_expire_after < 0:
            raise ValueError("cache_expire_after must be greater than or equal to 0")
        if self.request_retries < 0:
            raise ValueError("request_retries must be greater than or equal to 0")
        if self.retry_backoff_factor < 0:
            raise ValueError("retry_backoff_factor must be greater than or equal to 0")
        if self.max_retry_delay <= 0:
            raise ValueError("max_retry_delay must be a positive number")

    def _resolve_value(self, field_name: str, value: object, env_name: str, default: Any) -> Any:
        if field_name in self._provided_fields:
            return value
        env_value = os.getenv(env_name)
        return default if env_value is None else env_value

    def _resolve_optional_str(self, field_name: str, value: object, env_name: str) -> str | None:
        if field_name in self._provided_fields:
            if value is _NOT_PROVIDED:
                return None
            if value is None or isinstance(value, str):
                return value
            raise ValueError(f"{field_name} must be a string or None")
        return os.getenv(env_name)

    def _resolve_bool(self, field_name: str, value: object, env_name: str, default: bool) -> bool:
        resolved = self._resolve_value(field_name, value, env_name, default)
        if isinstance(resolved, bool):
            return resolved
        if isinstance(resolved, str):
            return resolved.lower() in ("true", "1", "yes")
        raise ValueError(f"{field_name} must be a boolean")

    def _resolve_int(self, field_name: str, value: object, env_name: str, default: int) -> int:
        resolved = self._resolve_value(field_name, value, env_name, default)
        if isinstance(resolved, bool):
            raise ValueError(f"{field_name} must be an integer")
        try:
            return int(resolved)
        except (TypeError, ValueError) as e:
            env_label = env_name if field_name not in self._provided_fields and os.getenv(env_name) is not None else field_name
            raise ValueError(f"{env_label} must be an integer") from e

    def _resolve_float(self, field_name: str, value: object, env_name: str, default: float) -> float:
        resolved = self._resolve_value(field_name, value, env_name, default)
        if isinstance(resolved, bool):
            raise ValueError(f"{field_name} must be a number")
        try:
            return float(resolved)
        except (TypeError, ValueError) as e:
            env_label = env_name if field_name not in self._provided_fields and os.getenv(env_name) is not None else field_name
            raise ValueError(f"{env_label} must be a number") from e

    def _parse_language(self, value: object, *, source: str) -> Language:
        if isinstance(value, Language):
            return value
        if isinstance(value, str):
            try:
                return Language(value.lower())
            except ValueError as e:
                label = "language" if source == "language" else source
                raise ValueError(f"{label} must be one of: {[lang.value for lang in Language]}") from e
        raise ValueError("language must be one of: ['pl', 'en']")

    def _parse_format(self, value: object, *, source: str) -> Format:
        if isinstance(value, Format):
            return value
        if isinstance(value, str):
            try:
                return Format(value.lower())
            except ValueError as e:
                label = "format" if source == "format" else source
                raise ValueError(f"{label} must be one of: {[fmt.value for fmt in Format]}") from e
        raise ValueError("format must be one of: ['json', 'jsonapi', 'xml']")

    def _resolve_custom_quotas(self, custom_quotas: object) -> dict[int, int] | None:
        resolved = custom_quotas
        if "custom_quotas" not in self._provided_fields:
            env_quotas = os.getenv("BDL_QUOTAS")
            if env_quotas:
                try:
                    resolved = json.loads(env_quotas)
                except json.JSONDecodeError as e:
                    raise ValueError("BDL_QUOTAS must be a valid JSON string representing a dictionary") from e
            else:
                resolved = None

        if resolved is _NOT_PROVIDED or resolved is None:
            return None
        if not isinstance(resolved, dict):
            raise ValueError("custom_quotas must be a dictionary of {period_seconds: int}")

        normalized: dict[int, int] = {}
        for key, value in resolved.items():
            try:
                period = int(key)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"custom_quotas keys must be one of {list(QUOTA_PERIODS.values())} and values positive int"
                ) from e
            if not (period in QUOTA_PERIODS.values() and isinstance(value, int) and value > 0):
                raise ValueError(
                    f"custom_quotas keys must be one of {list(QUOTA_PERIODS.values())} and values positive int"
                )
            normalized[period] = value
        return normalized

    def _resolve_retry_status_codes(self, retry_status_codes: object) -> tuple[int, ...]:
        resolved = retry_status_codes
        if "retry_status_codes" not in self._provided_fields:
            env_codes = os.getenv("BDL_RETRY_STATUS_CODES")
            if env_codes:
                resolved = [part.strip() for part in env_codes.split(",") if part.strip()]
            else:
                resolved = DEFAULT_RETRY_STATUS_CODES

        if resolved is _NOT_PROVIDED:
            return DEFAULT_RETRY_STATUS_CODES
        if isinstance(resolved, tuple):
            codes = resolved
        elif isinstance(resolved, list):
            try:
                codes = tuple(int(code) for code in resolved)
            except (TypeError, ValueError) as e:
                raise ValueError("retry_status_codes must contain integers") from e
        else:
            raise ValueError("retry_status_codes must be a tuple or list of integers")

        if not codes:
            raise ValueError("retry_status_codes must contain at least one status code")
        return codes
