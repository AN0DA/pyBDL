"""Integration tests for LevelsAccess using sample data."""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pyldb.access.levels import LevelsAccess


@pytest.mark.integration
class TestLevelsAccessIntegration:
    """Integration tests for LevelsAccess with sample data."""

    def test_list_levels(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test list_levels with sample data."""
        samples = load_sample_data("samples_raw_levels.json")
        mock_api_client.list_levels.return_value = samples["list_levels"]
        access = LevelsAccess(mock_api_client)
        result = access.list_levels()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_levels"])
        assert "id" in result.columns
        assert "name" in result.columns
        assert result["id"].iloc[0] == 0
        assert result["name"].iloc[0] == "Poziom Polski"

    def test_get_level(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_level with sample data."""
        samples = load_sample_data("samples_raw_levels.json")
        mock_api_client.get_level.return_value = samples["get_level"]
        access = LevelsAccess(mock_api_client)
        result = access.get_level(0)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["id"].iloc[0] == 0
        assert result["name"].iloc[0] == "Poziom Polski"

    @pytest.mark.asyncio
    async def test_alist_levels(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async list_levels with sample data."""
        samples = load_sample_data("samples_raw_levels.json")
        mock_async_api_client.alist_levels.return_value = samples["list_levels"]
        access = LevelsAccess(mock_async_api_client)
        result = await access.alist_levels()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_levels"])

    @pytest.mark.asyncio
    async def test_aget_level(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_level with sample data."""
        samples = load_sample_data("samples_raw_levels.json")
        mock_async_api_client.aget_level.return_value = samples["get_level"]
        access = LevelsAccess(mock_async_api_client)
        result = await access.aget_level(0)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
