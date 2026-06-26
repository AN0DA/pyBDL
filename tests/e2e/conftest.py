"""Shared fixtures and helpers for end-to-end tests."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from pybdl.client import BDL
from pybdl.config import BDLConfig

STUDY_VARIABLE_ID = "3643"
STUDY_UNIT_LEVEL = 2
STUDY_N_YEARS = 2
E2E_PAGE_SIZE = 10


def extract_recent_years(variable_meta: pd.DataFrame, n: int) -> list[int]:
    """Select the ``n`` most recent years available in variable metadata."""
    if "years" not in variable_meta.columns:
        raise ValueError("Variable metadata does not contain a 'years' column.")
    years_value = variable_meta.loc[variable_meta.index[0], "years"]
    if isinstance(years_value, str):
        cleaned = years_value.replace("[", "").replace("]", "")
        years = [int(part) for part in cleaned.split(",") if part.strip()]
    else:
        years = [int(year) for year in years_value]
    return sorted(years)[-n:]


@pytest.fixture(scope="session")
def e2e_cache_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Isolated cache directory shared within one E2E session."""
    return tmp_path_factory.mktemp("e2e_bdl_cache")


def _require_bdl_api_key() -> str:
    api_key = os.getenv("BDL_API_KEY")
    if not api_key:
        pytest.skip("BDL_API_KEY not set")
    assert api_key is not None
    return api_key


def _build_e2e_config(api_key: str, cache_dir: Path, *, use_http_cache: bool = True) -> BDLConfig:
    return BDLConfig(
        api_key=api_key,
        use_cache=use_http_cache,
        cache_backend="file" if use_http_cache else None,
        quota_cache_enabled=True,
        quota_cache_file=str(cache_dir / "quota_cache.json"),
        page_size=E2E_PAGE_SIZE,
    )


@pytest.fixture
def bdl_client(e2e_cache_dir: Path) -> BDL:
    """Create a BDL client for e2e tests with HTTP and quota caches enabled."""
    return BDL(_build_e2e_config(_require_bdl_api_key(), e2e_cache_dir))


@pytest.fixture
def bdl_client_fresh_quota(e2e_cache_dir: Path) -> BDL:
    """Client with HTTP cache but a fresh quota cache file for quota-sensitive checks."""
    fresh_dir = e2e_cache_dir / "fresh_quota"
    fresh_dir.mkdir(exist_ok=True)
    return BDL(_build_e2e_config(_require_bdl_api_key(), fresh_dir))
