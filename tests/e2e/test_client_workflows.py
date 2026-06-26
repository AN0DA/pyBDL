"""End-to-end tests for client workflows."""

import os

import pandas as pd
import pytest

from pybdl.client import BDL
from tests.e2e.conftest import E2E_PAGE_SIZE


@pytest.mark.e2e
@pytest.mark.skipif(not os.getenv("BDL_API_KEY"), reason="BDL_API_KEY not set")
class TestClientWorkflows:
    """End-to-end tests for client initialization and usage."""

    def test_client_initialization(self, bdl_client: BDL) -> None:
        """Test that client initializes all access and API layers."""
        assert bdl_client.aggregates is not None
        assert bdl_client.attributes is not None
        assert bdl_client.data is not None
        assert bdl_client.levels is not None
        assert bdl_client.measures is not None
        assert bdl_client.subjects is not None
        assert bdl_client.units is not None
        assert bdl_client.variables is not None
        assert bdl_client.years is not None

    def test_api_layer_access(self, bdl_client: BDL) -> None:
        """Test that API layer is accessible."""
        assert bdl_client.api.aggregates is not None
        assert bdl_client.api.levels is not None
        assert bdl_client.api.subjects is not None

    def test_access_vs_api_layer(self, bdl_client: BDL) -> None:
        """Test that access layer returns DataFrames while API layer returns dicts."""
        df = bdl_client.levels.list_levels(max_pages=1, page_size=E2E_PAGE_SIZE)
        assert isinstance(df, pd.DataFrame)
        api_result = bdl_client.api.levels.list_levels(max_pages=1, page_size=E2E_PAGE_SIZE)
        assert isinstance(api_result, list)

    def test_context_manager_workflow(self, bdl_client: BDL) -> None:
        """Test that the client context manager closes resources after use."""
        with bdl_client as bdl:
            df = bdl.levels.list_levels(max_pages=1, page_size=E2E_PAGE_SIZE)
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
