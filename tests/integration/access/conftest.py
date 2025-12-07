"""Fixtures for integration tests."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def samples_dir() -> Path:
    """Return the path to sample data directory."""
    return Path(__file__).parent / "samples" / "raw"


@pytest.fixture
def load_sample_data(samples_dir: Path) -> Callable[[str], dict[str, Any]]:
    """Load sample data from JSON files."""

    def _load(filename: str) -> dict[str, Any]:
        """Load a sample JSON file."""
        filepath = samples_dir / filename
        with open(filepath) as f:
            return json.load(f)

    return _load


@pytest.fixture
def mock_api_client() -> MagicMock:
    """Create a mock API client."""
    return MagicMock()


@pytest.fixture
def mock_async_api_client() -> MagicMock:
    """Create a mock async API client."""
    client = MagicMock()
    # Set up common async methods as AsyncMock
    async_methods = [
        "alist_levels",
        "aget_level",
        "aget_levels_metadata",
        "alist_subjects",
        "aget_subject",
        "asearch_subjects",
        "aget_subjects_metadata",
        "alist_aggregates",
        "aget_aggregate",
        "aget_aggregates_metadata",
        "alist_attributes",
        "aget_attribute",
        "aget_attributes_metadata",
        "alist_measures",
        "aget_measure",
        "aget_measures_metadata",
        "alist_units",
        "aget_unit",
        "asearch_units",
        "alist_localities",
        "aget_locality",
        "asearch_localities",
        "aget_units_metadata",
        "alist_variables",
        "aget_variable",
        "asearch_variables",
        "aget_variables_metadata",
        "alist_years",
        "aget_year",
        "aget_years_metadata",
        "aget_data_by_variable",
        "aget_data_by_unit",
        "aget_data_by_variable_locality",
        "aget_data_by_unit_locality",
        "aget_data_metadata",
    ]
    for method_name in async_methods:
        setattr(client, method_name, AsyncMock())
    return client
