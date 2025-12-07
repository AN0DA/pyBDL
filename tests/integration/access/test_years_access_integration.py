"""Integration tests for YearsAccess using sample data."""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pyldb.access.years import YearsAccess


@pytest.mark.integration
class TestYearsAccessIntegration:
    """Integration tests for YearsAccess with sample data."""

    def test_list_years(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test list_years with sample data."""
        samples = load_sample_data("samples_raw_years.json")
        mock_api_client.list_years.return_value = samples["list_years"]
        access = YearsAccess(mock_api_client)
        result = access.list_years()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_years"])
        assert "id" in result.columns
        assert result["id"].iloc[0] == 1995

    def test_get_year(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_year with sample data."""
        samples = load_sample_data("samples_raw_years.json")
        mock_api_client.get_year.return_value = samples["get_year"]
        access = YearsAccess(mock_api_client)
        result = access.get_year(2024)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["id"].iloc[0] == 2024

    @pytest.mark.asyncio
    async def test_alist_years(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async list_years with sample data."""
        samples = load_sample_data("samples_raw_years.json")
        mock_async_api_client.alist_years.return_value = samples["list_years"]
        access = YearsAccess(mock_async_api_client)
        result = await access.alist_years()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_years"])

    @pytest.mark.asyncio
    async def test_aget_year(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_year with sample data."""
        samples = load_sample_data("samples_raw_years.json")
        mock_async_api_client.aget_year.return_value = samples["get_year"]
        access = YearsAccess(mock_async_api_client)
        result = await access.aget_year(2024)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
