"""End-to-end tests for enrichment decorator with live API (if configured)."""

import os

import httpx
import pandas as pd
import pytest

from pybdl.client import BDL
from tests.e2e.conftest import E2E_PAGE_SIZE, STUDY_VARIABLE_ID


@pytest.mark.e2e
@pytest.mark.skipif(not os.getenv("BDL_API_KEY"), reason="BDL_API_KEY not set")
class TestEnrichmentE2E:
    """Verify enrichment works against live API responses."""

    def test_variables_enrichment_live(self, bdl_client: BDL) -> None:
        """List variables and enrich with level and measure metadata."""
        df = bdl_client.variables.list_variables(
            max_pages=1,
            page_size=E2E_PAGE_SIZE,
            enrich_levels=True,
            enrich_measures=True,
        )
        assert isinstance(df, pd.DataFrame)
        assert "level_name" in df.columns
        assert "measure_unit_description" in df.columns

    def test_data_enrichment_attributes_live(self, bdl_client: BDL) -> None:
        """Fetch data and enrich attribute metadata only (small dictionary)."""
        try:
            df = bdl_client.data.get_data_by_variable(
                STUDY_VARIABLE_ID,
                max_pages=1,
                page_size=E2E_PAGE_SIZE,
                enrich_attributes=True,
            )
        except (httpx.HTTPError, ValueError) as exc:
            pytest.skip(f"Variable ID may not exist or API unavailable: {exc}")

        assert isinstance(df, pd.DataFrame)
        assert "attr_description" in df.columns

    def test_data_enrichment_list_syntax_live(self, bdl_client: BDL) -> None:
        """Fetch data using enrich list syntax with a lightweight dictionary."""
        try:
            df = bdl_client.data.get_data_by_variable(
                STUDY_VARIABLE_ID,
                max_pages=1,
                page_size=E2E_PAGE_SIZE,
                enrich=["attributes"],
            )
        except (httpx.HTTPError, ValueError) as exc:
            pytest.skip(f"Variable ID may not exist or API unavailable: {exc}")

        assert isinstance(df, pd.DataFrame)
        assert "attr_description" in df.columns
