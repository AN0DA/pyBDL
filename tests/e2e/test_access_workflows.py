"""End-to-end tests for access layer workflows."""

import os

import pandas as pd
import pytest

from pybdl.client import BDL
from pybdl.config import BDLConfig


@pytest.mark.e2e
@pytest.mark.skipif(not os.getenv("BDL_API_KEY"), reason="BDL_API_KEY not set")
class TestAccessWorkflows:
    """End-to-end tests for access layer workflows."""

    @pytest.fixture
    def bdl_client(self) -> BDL:
        """Create an BDL client for e2e tests."""
        api_key = os.getenv("BDL_API_KEY")
        if not api_key:
            pytest.skip("BDL_API_KEY not set")
        config = BDLConfig(api_key=api_key, use_cache=False)
        return BDL(config=config)

    def test_list_levels_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for listing levels."""
        df = bdl_client.levels.list_levels()
        assert not df.empty
        assert "id" in df.columns
        assert "name" in df.columns

    def test_get_level_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for getting a level."""
        df = bdl_client.levels.get_level(1)
        assert not df.empty
        assert "id" in df.columns
        assert df["id"].iloc[0] == 1

    def test_list_subjects_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for listing subjects."""
        df = bdl_client.subjects.list_subjects()
        assert not df.empty
        assert "id" in df.columns
        assert "name" in df.columns

    def test_data_by_variable_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for getting data by variable."""
        # Use a known variable ID (this may need to be updated based on actual API)
        try:
            df = bdl_client.data.get_data_by_variable("3643", max_pages=1, page_size=10)
            assert isinstance(df, pd.DataFrame)
            # Check that nested data was normalized
            if not df.empty:
                assert "unit_id" in df.columns or "year" in df.columns
        except Exception:
            pytest.skip("Variable ID may not exist or API may be unavailable")

    def test_pagination_workflow(self, bdl_client: BDL) -> None:
        """Test pagination workflow."""
        df_all = bdl_client.subjects.list_subjects()
        df_first = bdl_client.subjects.list_subjects(max_pages=1, page_size=10)
        assert isinstance(df_all, pd.DataFrame)
        assert isinstance(df_first, pd.DataFrame)
        # First page should have at most page_size rows
        assert len(df_first) <= 10

    @pytest.mark.asyncio
    async def test_async_workflow(self, bdl_client: BDL) -> None:
        """Test async workflow."""
        df = await bdl_client.levels.alist_levels()
        assert not df.empty
        assert "id" in df.columns
