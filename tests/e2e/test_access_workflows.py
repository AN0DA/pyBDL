"""End-to-end tests for access layer workflows."""

import os

import httpx
import pandas as pd
import pytest

from pybdl.client import BDL
from tests.e2e.conftest import E2E_PAGE_SIZE, STUDY_UNIT_LEVEL, STUDY_VARIABLE_ID


@pytest.mark.e2e
@pytest.mark.skipif(not os.getenv("BDL_API_KEY"), reason="BDL_API_KEY not set")
class TestAccessWorkflows:
    """End-to-end tests for access layer workflows."""

    def test_list_levels_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for listing levels."""
        df = bdl_client.levels.list_levels(max_pages=1, page_size=E2E_PAGE_SIZE)
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
        df = bdl_client.subjects.list_subjects(max_pages=1, page_size=E2E_PAGE_SIZE)
        assert not df.empty
        assert "id" in df.columns
        assert "name" in df.columns

    def test_data_by_variable_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for getting data by variable."""
        try:
            df = bdl_client.data.get_data_by_variable(
                STUDY_VARIABLE_ID,
                max_pages=1,
                page_size=E2E_PAGE_SIZE,
            )
        except (httpx.HTTPError, ValueError) as exc:
            pytest.skip(f"Variable ID may not exist or API may be unavailable: {exc}")

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "unit_id" in df.columns or "year" in df.columns

    def test_pagination_workflow(self, bdl_client: BDL) -> None:
        """Test pagination workflow without fetching the full subjects catalog."""
        df_first = bdl_client.subjects.list_subjects(max_pages=1, page_size=E2E_PAGE_SIZE)
        df_two_pages = bdl_client.subjects.list_subjects(max_pages=2, page_size=E2E_PAGE_SIZE)
        assert isinstance(df_first, pd.DataFrame)
        assert isinstance(df_two_pages, pd.DataFrame)
        assert len(df_first) <= E2E_PAGE_SIZE
        assert len(df_two_pages) >= len(df_first)

    def test_data_by_unit_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for getting data by unit (UC-03)."""
        units = bdl_client.units.list_units(level=STUDY_UNIT_LEVEL, max_pages=1, page_size=1)
        assert not units.empty
        unit_id = str(units["id"].iloc[0])

        df = bdl_client.data.get_data_by_unit(
            unit_id=unit_id,
            variable_ids=[STUDY_VARIABLE_ID],
        )

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "year" in df.columns or "val" in df.columns

    @pytest.mark.asyncio
    async def test_async_workflow(self, bdl_client: BDL) -> None:
        """Test async workflow."""
        df = await bdl_client.levels.alist_levels(max_pages=1, page_size=E2E_PAGE_SIZE)
        assert not df.empty
        assert "id" in df.columns

    @pytest.mark.asyncio
    async def test_async_variable_workflow(self, bdl_client: BDL) -> None:
        """Test async workflow for variable metadata."""
        df = await bdl_client.variables.aget_variable(STUDY_VARIABLE_ID)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "id" in df.columns
