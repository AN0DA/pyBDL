"""Integration tests for VariablesAccess using sample data."""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pyldb.access.variables import VariablesAccess


@pytest.mark.integration
class TestVariablesAccessIntegration:
    """Integration tests for VariablesAccess with sample data."""

    def test_list_variables(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test list_variables with sample data."""
        samples = load_sample_data("samples_raw_variables.json")
        mock_api_client.list_variables.return_value = samples["list_variables"]
        access = VariablesAccess(mock_api_client)
        result = access.list_variables()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_variables"])
        assert "id" in result.columns
        assert result["id"].iloc[0] == 6

    def test_get_variable(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_variable with sample data."""
        samples = load_sample_data("samples_raw_variables.json")
        if "get_variable" in samples:
            mock_api_client.get_variable.return_value = samples["get_variable"]
        else:
            mock_api_client.get_variable.return_value = samples["list_variables"][0]
        access = VariablesAccess(mock_api_client)
        result = access.get_variable("6")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_search_variables(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test search_variables with sample data."""
        samples = load_sample_data("samples_raw_variables.json")
        mock_api_client.search_variables.return_value = samples["list_variables"][:2]
        access = VariablesAccess(mock_api_client)
        result = access.search_variables("ogółem")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


    @pytest.mark.asyncio
    async def test_alist_variables(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async list_variables with sample data."""
        samples = load_sample_data("samples_raw_variables.json")
        mock_async_api_client.alist_variables.return_value = samples["list_variables"]
        access = VariablesAccess(mock_async_api_client)
        result = await access.alist_variables()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_variables"])

    @pytest.mark.asyncio
    async def test_aget_variable(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_variable with sample data."""
        samples = load_sample_data("samples_raw_variables.json")
        mock_async_api_client.aget_variable.return_value = samples["list_variables"][0]
        access = VariablesAccess(mock_async_api_client)
        result = await access.aget_variable("6")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_asearch_variables(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async search_variables with sample data."""
        samples = load_sample_data("samples_raw_variables.json")
        mock_async_api_client.asearch_variables.return_value = samples["list_variables"][:2]
        access = VariablesAccess(mock_async_api_client)
        result = await access.asearch_variables("ogółem")
        assert isinstance(result, pd.DataFrame)

