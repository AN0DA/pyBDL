"""End-to-end tests for client workflows."""

import os

import pandas as pd
import pytest

from pyldb.client import LDB
from pyldb.config import LDBConfig


@pytest.mark.e2e
@pytest.mark.skipif(not os.getenv("LDB_API_KEY"), reason="LDB_API_KEY not set")
class TestClientWorkflows:
    """End-to-end tests for client initialization and usage."""

    @pytest.fixture
    def ldb_client(self) -> LDB:
        """Create an LDB client for e2e tests."""
        api_key = os.getenv("LDB_API_KEY")
        if not api_key:
            pytest.skip("LDB_API_KEY not set")
        config = LDBConfig(api_key=api_key, use_cache=False)
        return LDB(config=config)

    def test_client_initialization(self, ldb_client: LDB) -> None:
        """Test that client initializes all access and API layers."""
        assert ldb_client.aggregates is not None
        assert ldb_client.attributes is not None
        assert ldb_client.data is not None
        assert ldb_client.levels is not None
        assert ldb_client.measures is not None
        assert ldb_client.subjects is not None
        assert ldb_client.units is not None
        assert ldb_client.variables is not None
        assert ldb_client.years is not None

    def test_api_layer_access(self, ldb_client: LDB) -> None:
        """Test that API layer is accessible."""
        assert ldb_client.api.aggregates is not None
        assert ldb_client.api.levels is not None
        assert ldb_client.api.subjects is not None

    def test_access_vs_api_layer(self, ldb_client: LDB) -> None:
        """Test that access layer returns DataFrames while API layer returns dicts."""
        # Access layer returns DataFrame
        df = ldb_client.levels.list_levels()
        assert isinstance(df, pd.DataFrame)
        # API layer returns list/dict
        api_result = ldb_client.api.levels.list_levels()
        assert isinstance(api_result, list)
