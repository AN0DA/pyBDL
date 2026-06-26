"""End-to-end tests for metadata exploration workflows."""

import os

import pandas as pd
import pytest

from pybdl.client import BDL
from tests.e2e.conftest import E2E_PAGE_SIZE, STUDY_VARIABLE_ID


@pytest.mark.e2e
@pytest.mark.skipif(not os.getenv("BDL_API_KEY"), reason="BDL_API_KEY not set")
class TestMetadataWorkflows:
    """End-to-end tests for metadata exploration (UC-01)."""

    def test_get_variable_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for getting variable metadata."""
        df = bdl_client.variables.get_variable(STUDY_VARIABLE_ID)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "id" in df.columns
        assert "years" in df.columns
        assert str(df["id"].iloc[0]) == STUDY_VARIABLE_ID

    def test_search_variables_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for searching variables."""
        df = bdl_client.variables.search_variables("population", max_pages=1, page_size=5)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "id" in df.columns
        assert "n1" in df.columns

    def test_list_units_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for listing voivodeship-level units."""
        df = bdl_client.units.list_units(level=2, max_pages=1, page_size=E2E_PAGE_SIZE)
        assert isinstance(df, pd.DataFrame)
        assert len(df) >= 5
        assert "id" in df.columns
        assert "name" in df.columns
        assert "level" in df.columns
        assert (df["level"] == 2).all()

    def test_list_years_workflow(self, bdl_client: BDL) -> None:
        """Test complete workflow for listing years."""
        df = bdl_client.years.list_years(max_pages=1)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "id" in df.columns
