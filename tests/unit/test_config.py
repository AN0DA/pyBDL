import pytest
from pytest import MonkeyPatch

from pybdl.config import DEFAULT_CACHE_EXPIRY, DEFAULT_LANGUAGE, BDLConfig, Language


@pytest.mark.unit
def test_config_direct_init() -> None:
    config = BDLConfig(api_key="abc123", language=Language.EN, use_cache=False, cache_expire_after=123)
    assert config.api_key == "abc123"
    assert config.language == Language.EN
    assert config.use_cache is False
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
    assert config.cache_expire_after == 888


@pytest.mark.unit
def test_config_env_false_cache(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_API_KEY", "key2")
    monkeypatch.setenv("BDL_USE_CACHE", "false")
    config = BDLConfig()  # Use env var
    assert config.use_cache is False


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
    assert config.cache_expire_after == DEFAULT_CACHE_EXPIRY
    assert config.proxy_url is None
    assert config.proxy_username is None
    assert config.proxy_password is None


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
