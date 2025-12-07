import os
import tempfile

import pytest

from pybdl.utils.cache import get_cache_file_path, get_default_cache_path


@pytest.mark.unit
def test_get_default_cache_path_custom() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = get_default_cache_path(custom_path=tmpdir)
        assert os.path.exists(path)
        assert path == tmpdir


@pytest.mark.unit
def test_get_default_cache_path_global(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate platformdirs available
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setattr("pybdl.utils.cache.user_cache_dir", lambda app, author: tmpdir)
        path = get_default_cache_path(use_global_cache=True)
        assert os.path.exists(path)
        assert path == tmpdir


@pytest.mark.unit
def test_get_default_cache_path_global_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate platformdirs not available
    monkeypatch.setattr("pybdl.utils.cache.user_cache_dir", None)
    fallback = os.path.expanduser("~/.cache/pybdl")
    path = get_default_cache_path(use_global_cache=True)
    assert os.path.exists(path)
    assert path == fallback


@pytest.mark.unit
def test_get_default_cache_path_project() -> None:
    path = get_default_cache_path()
    assert os.path.exists(path)
    assert path.endswith(os.path.join(".cache", "pybdl"))


@pytest.mark.unit
def test_get_cache_file_path_custom() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = get_cache_file_path("foo.json", custom_path=tmpdir)
        assert file_path.endswith("foo.json")
        assert os.path.dirname(file_path) == tmpdir


@pytest.mark.unit
def test_get_cache_file_path_global(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setattr("pybdl.utils.cache.user_cache_dir", lambda app, author: tmpdir)
        file_path = get_cache_file_path("bar.json", use_global_cache=True)
        assert file_path.endswith("bar.json")
        assert os.path.dirname(file_path) == tmpdir


@pytest.mark.unit
def test_get_cache_file_path_project() -> None:
    file_path = get_cache_file_path("baz.json")
    assert file_path.endswith("baz.json")
    assert os.path.exists(os.path.dirname(file_path))
