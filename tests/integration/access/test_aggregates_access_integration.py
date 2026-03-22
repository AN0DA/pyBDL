"""Integration tests for AggregatesAccess using sample data."""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pybdl.access.aggregates import AggregatesAccess


@pytest.mark.integration
class TestAggregatesAccessIntegration:
    """Integration tests for AggregatesAccess with sample data."""

    def test_list_aggregates(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test list_aggregates with sample data."""
        samples = load_sample_data("samples_raw_aggregates.json")
        mock_api_client.list_aggregates.return_value = samples["list_aggregates"]
        access = AggregatesAccess(mock_api_client)
        result = access.list_aggregates()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_aggregates"])
        assert "id" in result.columns
        assert "name" in result.columns
        assert result["id"].iloc[0] == 1
        assert result["name"].iloc[0] == "ogółem"

    def test_get_aggregate(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_aggregate with sample data."""
        samples = load_sample_data("samples_raw_aggregates.json")
        mock_api_client.get_aggregate.return_value = samples["get_aggregate"]
        access = AggregatesAccess(mock_api_client)
        result = access.get_aggregate("1")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["id"].iloc[0] == 1
        assert result["name"].iloc[0] == "ogółem"

    def test_list_aggregates_with_enrichment(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """List aggregates and enrich level name."""
        aggregates_sample = load_sample_data("samples_raw_aggregates.json")
        levels_sample = load_sample_data("samples_raw_levels.json")
        mock_api_client.list_aggregates.return_value = aggregates_sample["list_aggregates"]
        mock_api_client.list_levels.return_value = levels_sample["list_levels"]

        access = AggregatesAccess(mock_api_client)
        result = access.list_aggregates(enrich_levels=True)

        assert "level_name" in result.columns
        assert result["level_name"].notna().any()

    @pytest.mark.asyncio
    async def test_alist_aggregates(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async list_aggregates with sample data."""
        samples = load_sample_data("samples_raw_aggregates.json")
        mock_async_api_client.alist_aggregates.return_value = samples["list_aggregates"]
        access = AggregatesAccess(mock_async_api_client)
        result = await access.alist_aggregates()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_aggregates"])

    @pytest.mark.asyncio
    async def test_aget_aggregate(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_aggregate with sample data."""
        samples = load_sample_data("samples_raw_aggregates.json")
        mock_async_api_client.aget_aggregate.return_value = samples["get_aggregate"]
        access = AggregatesAccess(mock_async_api_client)
        result = await access.aget_aggregate("1")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
