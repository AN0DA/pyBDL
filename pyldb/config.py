import enum
import os
from dataclasses import dataclass, field

# API Constants
LDB_API_BASE_URL = "https://bdl.stat.gov.pl/api/v1"

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

# Define constant quota periods (in seconds)
QUOTA_PERIODS = {"1s": 1, "15m": 15 * 60, "12h": 12 * 3600, "7d": 7 * 24 * 3600}
DEFAULT_QUOTAS = {  # (anon_limit, registered_limit)
    QUOTA_PERIODS["1s"]: (5, 10),
    QUOTA_PERIODS["15m"]: (100, 500),
    QUOTA_PERIODS["12h"]: (1000, 5000),
    QUOTA_PERIODS["7d"]: (10000, 50000),
}


@dataclass
class LDBConfig:
    """
    Configuration for the LDB API client.

    This dataclass manages all configuration options for the LDB API client, supporting
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
        quota_cache_file: Path to quota cache file (default: project .cache/pyldb).
        use_global_cache: Store quota cache in OS-specific location (default: False).
        page_size: Default page size for paginated requests (default: 100).
    """

    api_key: str | None = field(default=_NOT_PROVIDED)  # type: ignore[assignment]
    language: Language = field(default=DEFAULT_LANGUAGE)
    format: Format = field(default=DEFAULT_FORMAT)
    use_cache: bool = field(default=True)
    cache_expire_after: int = field(default=DEFAULT_CACHE_EXPIRY)
    proxy_url: str | None = field(default=None)
    proxy_username: str | None = field(default=None)
    proxy_password: str | None = field(default=None)
    custom_quotas: dict | None = field(default=None)
    quota_cache_enabled: bool = field(default=True)
    quota_cache_file: str | None = field(default=None)
    use_global_cache: bool = field(default=False)
    page_size: int = field(default=DEFAULT_PAGE_SIZE)

    def __post_init__(self) -> None:
        """
        Initialize configuration values from environment variables if not set directly.

        Raises:
            ValueError: If configuration values are invalid (e.g., invalid language code).
        """
        # Get API key from environment if not provided directly
        # If api_key is _NOT_PROVIDED, check environment variable
        # If api_key is explicitly None, use None (anonymous access)
        if self.api_key is _NOT_PROVIDED:
            self.api_key = os.getenv("LDB_API_KEY")
        # If explicitly None, keep it as None (anonymous access, stronger than env)

        # Get language from environment if not provided directly
        # Convert provided language string to Language enum if necessary
        if isinstance(self.language, str):
            try:
                self.language = Language(self.language.lower())
            except ValueError as e:
                raise ValueError(f"language must be one of: {[lang.value for lang in Language]}") from e

        env_language = os.getenv("LDB_LANGUAGE")
        if env_language:
            try:
                self.language = Language(env_language.lower())
            except ValueError as e:
                raise ValueError(f"LDB_LANGUAGE must be one of: {[lang.value for lang in Language]}") from e

        # Get format from environment if not provided directly
        # Convert provided format string to Format enum if necessary
        if isinstance(self.format, str):
            try:
                self.format = Format(self.format.lower())
            except ValueError as e:
                raise ValueError(f"format must be one of: {[fmt.value for fmt in Format]}") from e

        env_format = os.getenv("LDB_FORMAT")
        if env_format:
            try:
                self.format = Format(env_format.lower())
            except ValueError as e:
                raise ValueError(f"LDB_FORMAT must be one of: {[fmt.value for fmt in Format]}") from e

        # Get cache settings from environment if not provided directly
        env_use_cache = os.getenv("LDB_USE_CACHE")
        if env_use_cache is not None:
            self.use_cache = env_use_cache.lower() in ("true", "1", "yes")

        env_cache_expiry = os.getenv("LDB_CACHE_EXPIRY")
        if env_cache_expiry is not None:
            try:
                self.cache_expire_after = int(env_cache_expiry)
            except ValueError as e:
                raise ValueError("LDB_CACHE_EXPIRY must be an integer") from e

        # Get proxy settings from environment if not provided directly
        if self.proxy_url is None:
            self.proxy_url = os.getenv("LDB_PROXY_URL")

        if self.proxy_username is None:
            self.proxy_username = os.getenv("LDB_PROXY_USERNAME")

        if self.proxy_password is None:
            self.proxy_password = os.getenv("LDB_PROXY_PASSWORD")

        # Quota cache settings from env
        env_quota_cache_enabled = os.getenv("LDB_QUOTA_CACHE_ENABLED")
        if env_quota_cache_enabled is not None:
            self.quota_cache_enabled = env_quota_cache_enabled.lower() in ("true", "1", "yes")
        env_quota_cache_file = os.getenv("LDB_QUOTA_CACHE")
        if env_quota_cache_file:
            self.quota_cache_file = env_quota_cache_file
        env_use_global_cache = os.getenv("LDB_USE_GLOBAL_CACHE")
        if env_use_global_cache is not None:
            self.use_global_cache = env_use_global_cache.lower() in ("true", "1", "yes")

        # Get page_size from environment if not provided directly
        env_page_size = os.getenv("LDB_PAGE_SIZE")
        if env_page_size is not None:
            try:
                self.page_size = int(env_page_size)
            except ValueError as e:
                raise ValueError("LDB_PAGE_SIZE must be an integer") from e

        # Custom quotas from env (JSON string)
        env_quotas = os.getenv("LDB_QUOTAS")
        if env_quotas:
            try:
                import json

                loaded_quotas = json.loads(env_quotas)
                # Convert string keys to int if possible
                if isinstance(loaded_quotas, dict):
                    self.custom_quotas = {int(k): v for k, v in loaded_quotas.items()}
                else:
                    self.custom_quotas = loaded_quotas
            except Exception as e:
                raise ValueError("LDB_QUOTAS must be a valid JSON string representing a dictionary") from e
        # Validate and merge custom_quotas
        # If custom_quotas is provided, use those values (single ints)
        # Otherwise, keep DEFAULT_QUOTAS format (tuples) for rate limiter to choose based on registration
        if self.custom_quotas is not None:
            if not isinstance(self.custom_quotas, dict):
                raise ValueError("custom_quotas must be a dictionary of {period_seconds: int}")
            for k, v in self.custom_quotas.items():
                if not (isinstance(k, int) and k in QUOTA_PERIODS.values() and isinstance(v, int) and v > 0):
                    raise ValueError(
                        f"custom_quotas keys must be one of {list(QUOTA_PERIODS.values())} and values positive int"
                    )
        else:
            # No custom quotas, keep None so API client can use DEFAULT_QUOTAS with tuple format
            self.custom_quotas = None
