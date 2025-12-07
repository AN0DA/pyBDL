from types import SimpleNamespace

import pytest
from pytest import MonkeyPatch, raises

from pybdl.client import BDL
from pybdl.config import BDLConfig, Language


@pytest.mark.unit
def test_bdl_initializes_all_apis(monkeypatch: MonkeyPatch) -> None:
    # Use a dummy config and patch API classes to record instantiations
    class DummyAPI:
        def __init__(self, config: BDLConfig) -> None:
            self.config = config

    monkeypatch.setattr("pybdl.api.AggregatesAPI", DummyAPI)
    monkeypatch.setattr("pybdl.api.AttributesAPI", DummyAPI)
    monkeypatch.setattr("pybdl.api.DataAPI", DummyAPI)
    monkeypatch.setattr("pybdl.api.LevelsAPI", DummyAPI)
    monkeypatch.setattr("pybdl.api.MeasuresAPI", DummyAPI)
    monkeypatch.setattr("pybdl.api.SubjectsAPI", DummyAPI)
    monkeypatch.setattr("pybdl.api.UnitsAPI", DummyAPI)
    monkeypatch.setattr("pybdl.api.VariablesAPI", DummyAPI)
    monkeypatch.setattr("pybdl.api.VersionAPI", DummyAPI)
    monkeypatch.setattr("pybdl.api.YearsAPI", DummyAPI)

    config = BDLConfig(api_key="dummy")
    bdl = BDL(config=config)

    api = bdl.api
    assert isinstance(api, SimpleNamespace)
    # Check all endpoints exist and are DummyAPI instances
    assert isinstance(api.aggregates, DummyAPI)
    assert isinstance(api.attributes, DummyAPI)
    assert isinstance(api.data, DummyAPI)
    assert isinstance(api.levels, DummyAPI)
    assert isinstance(api.measures, DummyAPI)
    assert isinstance(api.subjects, DummyAPI)
    assert isinstance(api.units, DummyAPI)
    assert isinstance(api.variables, DummyAPI)
    assert isinstance(api.version, DummyAPI)
    assert isinstance(api.years, DummyAPI)
    # All configs passed through
    for attr in vars(api):
        assert getattr(api, attr).config is config


@pytest.mark.unit
def test_bdl_config_default(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_API_KEY", "envkey")
    monkeypatch.setenv("BDL_LANGUAGE", "en")
    monkeypatch.setenv("BDL_USE_CACHE", "0")
    monkeypatch.setenv("BDL_CACHE_EXPIRY", "42")
    bdl = BDL()
    assert bdl.config.api_key == "envkey"
    assert bdl.config.language == Language.EN
    assert bdl.config.use_cache is False
    assert bdl.config.cache_expire_after == 42


@pytest.mark.unit
def test_bdl_accepts_dict_config(monkeypatch: MonkeyPatch) -> None:
    config_dict = {"api_key": "dummy", "language": "en"}
    bdl = BDL(config=config_dict)
    assert bdl.config.api_key == "dummy"
    assert bdl.config.language.name == "EN"


@pytest.mark.unit
def test_bdl_config_type_error() -> None:
    with raises(TypeError):
        BDL(config=123)  # type: ignore[arg-type]


@pytest.mark.unit
def test_config_invalid_language_string() -> None:
    with raises(ValueError) as e:
        BDLConfig(api_key="dummy", language="xx")  # type: ignore[arg-type]
    assert "language must be one of" in str(e.value)


@pytest.mark.unit
def test_config_invalid_language_env(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("BDL_API_KEY", "dummy")
    monkeypatch.setenv("BDL_LANGUAGE", "xx")
    with raises(ValueError) as e:
        BDLConfig(api_key="dummy", language="xx")  # type: ignore[arg-type]
    assert "language must be one of" in str(e.value)


@pytest.mark.unit
def test_bdl_anonymous_access(monkeypatch: MonkeyPatch) -> None:
    """Test that BDL client can be initialized with None api_key for anonymous access."""
    monkeypatch.delenv("BDL_API_KEY", raising=False)

    # Test with None config
    bdl = BDL(config=None)
    assert bdl.config.api_key is None

    # Test with explicit None api_key
    config = BDLConfig(api_key=None)
    bdl2 = BDL(config=config)
    assert bdl2.config.api_key is None

    # Test with dict config with None api_key
    bdl3 = BDL(config={"api_key": None})
    assert bdl3.config.api_key is None


@pytest.mark.unit
def test_bdl_config_dict_with_none_api_key(monkeypatch: MonkeyPatch) -> None:
    """Test that dict config with None api_key works."""
    monkeypatch.delenv("BDL_API_KEY", raising=False)
    config_dict = {"api_key": None, "language": "en"}
    bdl = BDL(config=config_dict)
    assert bdl.config.api_key is None
    assert bdl.config.language.name == "EN"
