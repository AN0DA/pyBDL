import pytest
from pytest import MonkeyPatch

from pybdl.config import DEFAULT_CACHE_EXPIRY, DEFAULT_LANGUAGE, BDLConfig, Format, Language


@pytest.mark.unit
def test_config_direct_init() -> None:
    config = BDLConfig(api_key="abc123", language=Language.EN, use_cache=False, cache_expire_after=123)
    assert config.api_key == "abc123"
    assert config.language == Language.EN
    assert config.use_cache is False
    assert config.cache_backend is None
    assert config.cache_expire_after == 123


@pytest.mark.unit
def test_config_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("BDL_API_KEY", raising=False)
    monkeypatch.setenv("BDL_API_KEY", "envkey")
    monkeypatch.setenv("BDL_LANGUAGE", "pl")
    monkeypatch.setenv("BDL_USE_CACHE", "true")
    monkeypatch.setenv("BDL_CACHE_EXPIRY", "888")
    # Not providing api_key uses env var
    config = BDLConfig()
    assert config.api_key == "envkey"
    assert config.language == Language.PL
    assert config.use_cache is True
    assert config.cache_backend == "file"
    assert config.cache_expire_after == 888


@pytest.mark.unit
def test_explicit_values_override_environment(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_LANGUAGE", "pl")
    monkeypatch.setenv("BDL_USE_CACHE", "false")
    monkeypatch.setenv("BDL_CACHE_EXPIRY", "999")
    monkeypatch.setenv("BDL_PAGE_SIZE", "333")

    config = BDLConfig(
        api_key="abc123",
        language=Language.EN,
        use_cache=True,
        cache_expire_after=123,
        page_size=25,
    )

    assert config.language == Language.EN
    assert config.use_cache is True
    assert config.cache_backend == "file"
    assert config.cache_expire_after == 123
    assert config.page_size == 25


@pytest.mark.unit
def test_retry_config_from_environment(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_REQUEST_RETRIES", "5")
    monkeypatch.setenv("BDL_RETRY_BACKOFF_FACTOR", "0.25")
    monkeypatch.setenv("BDL_MAX_RETRY_DELAY", "12")
    monkeypatch.setenv("BDL_RETRY_STATUS_CODES", "429,500,503")

    config = BDLConfig(api_key="abc123")

    assert config.request_retries == 5
    assert config.retry_backoff_factor == 0.25
    assert config.max_retry_delay == 12.0
    assert config.retry_status_codes == (429, 500, 503)


@pytest.mark.unit
def test_http_429_retry_defaults() -> None:
    config = BDLConfig(api_key="abc123")
    assert config.http_429_max_retries == 12
    assert config.http_429_max_delay == 900.0


@pytest.mark.unit
def test_raise_on_rate_limit_default() -> None:
    config = BDLConfig(api_key="abc123")
    assert config.raise_on_rate_limit is False


@pytest.mark.unit
def test_raise_on_rate_limit_from_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_RATE_LIMIT_RAISE", "true")
    config = BDLConfig(api_key="abc123")
    assert config.raise_on_rate_limit is True


@pytest.mark.unit
def test_raise_on_rate_limit_explicit_overrides_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_RATE_LIMIT_RAISE", "true")
    config = BDLConfig(api_key="abc123", raise_on_rate_limit=False)
    assert config.raise_on_rate_limit is False


@pytest.mark.unit
def test_config_env_false_cache(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_API_KEY", "key2")
    monkeypatch.setenv("BDL_USE_CACHE", "false")
    config = BDLConfig()  # Use env var
    assert config.use_cache is False
    assert config.cache_backend is None


@pytest.mark.unit
def test_config_env_invalid_expiry(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_API_KEY", "key3")
    monkeypatch.setenv("BDL_CACHE_EXPIRY", "badint")
    with pytest.raises(ValueError):
        BDLConfig()  # Use env var


@pytest.mark.unit
def test_config_anonymous_access(monkeypatch: MonkeyPatch) -> None:
    """Test that config allows None api_key for anonymous access."""
    monkeypatch.delenv("BDL_API_KEY", raising=False)
    config = BDLConfig(api_key=None)
    assert config.api_key is None


@pytest.mark.unit
def test_config_anonymous_access_explicit_none_overrides_env(monkeypatch: MonkeyPatch) -> None:
    """Test that explicitly passing api_key=None overrides environment variable."""
    monkeypatch.setenv("BDL_API_KEY", "envkey")
    config = BDLConfig(api_key=None)
    # Explicit None should be stronger than env var
    assert config.api_key is None


@pytest.mark.unit
def test_config_anonymous_access_no_explicit_none_uses_env(monkeypatch: MonkeyPatch) -> None:
    """Test that not providing api_key uses environment variable."""
    monkeypatch.setenv("BDL_API_KEY", "envkey")
    config = BDLConfig()  # Not providing api_key
    assert config.api_key == "envkey"


@pytest.mark.unit
def test_config_defaults(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("BDL_API_KEY", raising=False)
    # Provide api_key directly, others use defaults
    config = BDLConfig(api_key="directkey")
    assert config.language == DEFAULT_LANGUAGE
    assert config.use_cache is True
    assert config.cache_backend == "file"
    assert config.cache_expire_after == DEFAULT_CACHE_EXPIRY
    assert config.proxy_url is None
    assert config.proxy_username is None
    assert config.proxy_password is None


@pytest.mark.unit
def test_config_cache_backend_direct_init() -> None:
    config = BDLConfig(api_key="abc123", cache_backend="memory")
    assert config.use_cache is True
    assert config.cache_backend == "memory"


@pytest.mark.unit
def test_config_cache_backend_none_disables_cache() -> None:
    config = BDLConfig(api_key="abc123", cache_backend=None)
    assert config.use_cache is False
    assert config.cache_backend is None


@pytest.mark.unit
def test_config_cache_backend_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_API_KEY", "envkey")
    monkeypatch.setenv("BDL_CACHE_BACKEND", "memory")
    config = BDLConfig()
    assert config.use_cache is True
    assert config.cache_backend == "memory"


@pytest.mark.unit
def test_config_proxy_direct_init() -> None:
    config = BDLConfig(
        api_key="abc123", proxy_url="http://proxy.example.com:8080", proxy_username="user", proxy_password="pass"
    )
    assert config.proxy_url == "http://proxy.example.com:8080"
    assert config.proxy_username == "user"
    assert config.proxy_password == "pass"


@pytest.mark.unit
def test_config_proxy_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_API_KEY", "envkey")
    monkeypatch.setenv("BDL_PROXY_URL", "http://proxy.example.com:8080")
    monkeypatch.setenv("BDL_PROXY_USERNAME", "envuser")
    monkeypatch.setenv("BDL_PROXY_PASSWORD", "envpass")

    config = BDLConfig()  # Use env var
    assert config.proxy_url == "http://proxy.example.com:8080"
    assert config.proxy_username == "envuser"
    assert config.proxy_password == "envpass"


@pytest.mark.unit
def test_config_quota_cache_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_API_KEY", "key4")
    monkeypatch.setenv("BDL_QUOTA_CACHE_ENABLED", "false")
    monkeypatch.setenv("BDL_QUOTA_CACHE", "/tmp/quota.json")
    monkeypatch.setenv("BDL_USE_GLOBAL_CACHE", "true")
    config = BDLConfig()  # Use env var
    assert config.quota_cache_enabled is False
    assert config.quota_cache_file == "/tmp/quota.json"
    assert config.use_global_cache is True


@pytest.mark.unit
def test_config_custom_quotas_env(monkeypatch: MonkeyPatch) -> None:
    import json

    monkeypatch.setenv("BDL_API_KEY", "key5")
    quotas = {1: 42, 900: 99, 43200: 123, 604800: 456}
    monkeypatch.setenv("BDL_QUOTAS", json.dumps(quotas))
    config = BDLConfig()  # Use env var
    for k, v in quotas.items():
        assert config.custom_quotas is not None
        assert config.custom_quotas[k] == v


@pytest.mark.unit
def test_config_custom_quotas_invalid_json(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_API_KEY", "key6")
    monkeypatch.setenv("BDL_QUOTAS", "not-a-json")
    with pytest.raises(ValueError):
        BDLConfig()  # Use env var


@pytest.mark.unit
def test_config_custom_quotas_not_dict(monkeypatch: MonkeyPatch) -> None:
    import json

    monkeypatch.setenv("BDL_API_KEY", "key7")
    monkeypatch.setenv("BDL_QUOTAS", json.dumps([1, 2, 3]))
    with pytest.raises(ValueError):
        BDLConfig()  # Use env var


@pytest.mark.unit
def test_config_custom_quotas_invalid_keys(monkeypatch: MonkeyPatch) -> None:
    import json

    monkeypatch.setenv("BDL_API_KEY", "key8")
    # key is not a valid period, value is negative
    monkeypatch.setenv("BDL_QUOTAS", json.dumps({123: -1}))
    with pytest.raises(ValueError):
        BDLConfig()  # Use env var


@pytest.mark.unit
def test_config_custom_quotas_none_uses_defaults(monkeypatch: MonkeyPatch) -> None:
    """Test that when custom_quotas is None, config keeps it as None for API client to use DEFAULT_QUOTAS."""
    monkeypatch.delenv("BDL_QUOTAS", raising=False)
    config = BDLConfig(api_key="dummy")
    assert config.custom_quotas is None


@pytest.mark.unit
def test_config_anonymous_access_custom_quotas(monkeypatch: MonkeyPatch) -> None:
    """Test anonymous access with custom quotas."""
    import json

    monkeypatch.delenv("BDL_API_KEY", raising=False)
    quotas = {1: 10, 900: 200}
    monkeypatch.setenv("BDL_QUOTAS", json.dumps(quotas))
    config = BDLConfig(api_key=None)
    assert config.api_key is None
    assert config.custom_quotas is not None
    assert config.custom_quotas[1] == 10
    assert config.custom_quotas[900] == 200


@pytest.mark.unit
@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"page_size": 0}, "page_size must be a positive integer"),
        ({"page_size": -1}, "page_size must be a positive integer"),
        ({"cache_expire_after": -1}, "cache_expire_after must be greater than or equal to 0"),
        ({"request_retries": -1}, "request_retries must be greater than or equal to 0"),
        ({"retry_backoff_factor": -0.1}, "retry_backoff_factor must be greater than or equal to 0"),
        ({"max_retry_delay": 0}, "max_retry_delay must be a positive number"),
        ({"http_429_max_retries": -1}, "http_429_max_retries must be greater than or equal to 0"),
        ({"http_429_max_delay": 0}, "http_429_max_delay must be a positive number"),
    ],
)
def test_config_validation_guards(kwargs: dict, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        BDLConfig(api_key="abc123", **kwargs)


@pytest.mark.unit
def test_config_format_enum_and_string() -> None:
    cfg = BDLConfig(api_key="x", format=Format.JSONAPI)
    assert cfg.format == Format.JSONAPI
    cfg2 = BDLConfig(api_key="x", format="xml")
    assert cfg2.format == Format.XML


@pytest.mark.unit
def test_config_format_invalid_string() -> None:
    with pytest.raises(ValueError, match="format must be one of"):
        BDLConfig(api_key="x", format="yaml")


@pytest.mark.unit
def test_config_format_invalid_type() -> None:
    with pytest.raises(ValueError, match="format must be one of"):
        BDLConfig(api_key="x", format=123)


@pytest.mark.unit
def test_config_page_size_bool_rejected() -> None:
    with pytest.raises(ValueError, match="page_size must be an integer"):
        BDLConfig(api_key="x", page_size=True)


@pytest.mark.unit
def test_config_max_retry_delay_bool_rejected() -> None:
    with pytest.raises(ValueError, match="max_retry_delay must be a number"):
        BDLConfig(api_key="x", max_retry_delay=True)


@pytest.mark.unit
def test_config_cache_backend_non_string_rejected() -> None:
    with pytest.raises(ValueError, match="cache_backend must be one of"):
        BDLConfig(api_key="x", cache_backend=99)


@pytest.mark.unit
def test_config_retry_status_codes_empty_list() -> None:
    with pytest.raises(ValueError, match="retry_status_codes must contain at least one"):
        BDLConfig(api_key="x", retry_status_codes=[])


@pytest.mark.unit
def test_config_retry_status_codes_empty_tuple() -> None:
    with pytest.raises(ValueError, match="retry_status_codes must contain at least one"):
        BDLConfig(api_key="x", retry_status_codes=())


@pytest.mark.unit
def test_config_retry_status_codes_invalid_container_type() -> None:
    with pytest.raises(ValueError, match="retry_status_codes must be a tuple or list"):
        BDLConfig(api_key="x", retry_status_codes={500})


@pytest.mark.unit
def test_config_proxy_username_non_string_explicit() -> None:
    with pytest.raises(ValueError, match="proxy_username must be a string or None"):
        BDLConfig(api_key="x", proxy_username=123)


@pytest.mark.unit
def test_config_raise_on_rate_limit_non_bool_non_string() -> None:
    with pytest.raises(ValueError, match="raise_on_rate_limit must be a boolean"):
        BDLConfig(api_key="x", raise_on_rate_limit=[])


@pytest.mark.unit
def test_config_language_invalid_non_string() -> None:
    with pytest.raises(ValueError, match="language must be one of"):
        BDLConfig(api_key="x", language=123)


@pytest.mark.unit
def test_config_custom_quotas_non_int_key() -> None:
    with pytest.raises(ValueError, match="custom_quotas keys must be one of"):
        BDLConfig(api_key="x", custom_quotas={"not-a-period": 1})
