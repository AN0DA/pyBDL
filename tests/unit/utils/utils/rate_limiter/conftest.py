"""Common fixtures for rate limiter tests."""

from collections.abc import Generator

import pytest


@pytest.fixture(autouse=True)
def _enable_real_rate_limiting(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Enable real rate limiting for rate limiter tests by undoing global patching."""
    # Undo the global patching from tests/api/conftest.py for this test module
    monkeypatch.undo()
    yield
