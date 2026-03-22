"""End-to-end tests for enrichment decorator with live API (if configured)."""

import os

import pandas as pd
import pytest

from pybdl.client import BDL
from pybdl.config import BDLConfig


@pytest.mark.e2e
@pytest.mark.skipif(not os.getenv("BDL_API_KEY"), reason="BDL_API_KEY not set")
class TestEnrichmentE2E:
    """Verify enrichment works against live API responses."""

    @pytest.fixture
    def bdl_client(self) -> BDL:
        """Create BDL client with API key."""
        api_key = os.getenv("BDL_API_KEY")
        if not api_key:
            pytest.skip("BDL_API_KEY not set")
        config = BDLConfig(api_key=api_key, use_cache=False)
        return BDL(config=config)

    def test_variables_enrichment_live(self, bdl_client: BDL) -> None:
        """List variables and enrich with level and measure metadata."""
        df = bdl_client.variables.list_variables(max_pages=1, page_size=10, enrich_levels=True, enrich_measures=True)
        assert isinstance(df, pd.DataFrame)
        assert "level_name" in df.columns
        assert "measure_unit_description" in df.columns

    def test_data_enrichment_live(self, bdl_client: BDL) -> None:
        """Fetch data by variable and enrich unit/attribute metadata."""
        try:
            df = bdl_client.data.get_data_by_variable(
                "3643",
                max_pages=1,
                page_size=10,
                enrich_units=True,
                enrich_attributes=True,
            )
        except Exception:
            pytest.skip("Variable ID may not exist or API unavailable")

        assert isinstance(df, pd.DataFrame)
        assert "unit_level" in df.columns
        assert "unit_parent_id" in df.columns
        assert "attr_description" in df.columns
