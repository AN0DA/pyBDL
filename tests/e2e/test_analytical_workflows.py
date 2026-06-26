"""End-to-end tests for analytical workflows from the thesis case study."""

import os

import pandas as pd
import pytest

from pybdl.client import BDL
from tests.e2e.conftest import (
    E2E_PAGE_SIZE,
    STUDY_N_YEARS,
    STUDY_UNIT_LEVEL,
    STUDY_VARIABLE_ID,
    extract_recent_years,
)


@pytest.mark.e2e
@pytest.mark.skipif(not os.getenv("BDL_API_KEY"), reason="BDL_API_KEY not set")
class TestAnalyticalWorkflows:
    """End-to-end tests for the main analytical flow (UC-02, VAL-MAIN, STUD-01)."""

    def test_case_study_workflow(self, bdl_client: BDL) -> None:
        """Mirror the chapter 6 case study: metadata → filtered fetch → enriched DataFrame."""
        variable_meta = bdl_client.variables.get_variable(STUDY_VARIABLE_ID)
        years = extract_recent_years(variable_meta, STUDY_N_YEARS)
        data = bdl_client.data.get_data_by_variable(
            variable_id=STUDY_VARIABLE_ID,
            years=years,
            unit_level=STUDY_UNIT_LEVEL,
            enrich=["attributes"],
            max_pages=1,
            page_size=E2E_PAGE_SIZE,
        )

        assert isinstance(data, pd.DataFrame)
        assert "attr_id" in data.columns
        data = data[data["attr_id"] == 1]
        assert not data.empty
        assert data["year"].isin(years).all()
        assert "unit_name" in data.columns or "unit_id" in data.columns
        assert "attr_description" in data.columns or "attr_name" in data.columns
        if "unit_level" in data.columns:
            assert (data["unit_level"] == STUDY_UNIT_LEVEL).all()
        value_col = "val" if "val" in data.columns else "value"
        assert value_col in data.columns
        assert data[value_col].notna().all()

    def test_data_by_variable_with_filters_workflow(self, bdl_client: BDL) -> None:
        """Test fetching data with year and unit_level filters."""
        variable_meta = bdl_client.variables.get_variable(STUDY_VARIABLE_ID)
        years = extract_recent_years(variable_meta, 2)

        df = bdl_client.data.get_data_by_variable(
            STUDY_VARIABLE_ID,
            years=years,
            unit_level=STUDY_UNIT_LEVEL,
            max_pages=1,
            page_size=E2E_PAGE_SIZE,
        )

        assert isinstance(df, pd.DataFrame)
        if not df.empty and "year" in df.columns:
            assert df["year"].isin(years).all()
