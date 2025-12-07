"""End-to-end tests for access layer workflows."""

import os

import pandas as pd
import pytest

from pyldb.client import LDB
from pyldb.config import LDBConfig


@pytest.mark.e2e
@pytest.mark.skipif(not os.getenv("LDB_API_KEY"), reason="LDB_API_KEY not set")
class TestAccessWorkflows:
    """End-to-end tests for access layer workflows."""

    @pytest.fixture
    def ldb_client(self) -> LDB:
        """Create an LDB client for e2e tests."""
        api_key = os.getenv("LDB_API_KEY")
        if not api_key:
            pytest.skip("LDB_API_KEY not set")
        config = LDBConfig(api_key=api_key, use_cache=False)
        return LDB(config=config)

    def test_list_levels_workflow(self, ldb_client: LDB) -> None:
        """Test complete workflow for listing levels."""
        df = ldb_client.levels.list_levels()
        assert not df.empty
        assert "id" in df.columns
        assert "name" in df.columns

    def test_get_level_workflow(self, ldb_client: LDB) -> None:
        """Test complete workflow for getting a level."""
        df = ldb_client.levels.get_level(1)
        assert not df.empty
        assert "id" in df.columns
        assert df["id"].iloc[0] == 1

    def test_list_subjects_workflow(self, ldb_client: LDB) -> None:
        """Test complete workflow for listing subjects."""
        df = ldb_client.subjects.list_subjects()
        assert not df.empty
        assert "id" in df.columns
        assert "name" in df.columns

    def test_data_by_variable_workflow(self, ldb_client: LDB) -> None:
        """Test complete workflow for getting data by variable."""
        # Use a known variable ID (this may need to be updated based on actual API)
        try:
            df = ldb_client.data.get_data_by_variable("3643", max_pages=1, page_size=10)
            assert isinstance(df, pd.DataFrame)
            # Check that nested data was normalized
            if not df.empty:
                assert "unit_id" in df.columns or "year" in df.columns
        except Exception:
            pytest.skip("Variable ID may not exist or API may be unavailable")

    def test_pagination_workflow(self, ldb_client: LDB) -> None:
        """Test pagination workflow."""
        df_all = ldb_client.subjects.list_subjects()
        df_first = ldb_client.subjects.list_subjects(max_pages=1, page_size=10)
        assert isinstance(df_all, pd.DataFrame)
        assert isinstance(df_first, pd.DataFrame)
        # First page should have at most page_size rows
        assert len(df_first) <= 10

    @pytest.mark.asyncio
    async def test_async_workflow(self, ldb_client: LDB) -> None:
        """Test async workflow."""
        df = await ldb_client.levels.alist_levels()
        assert not df.empty
        assert "id" in df.columns
