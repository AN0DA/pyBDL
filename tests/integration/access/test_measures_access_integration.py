"""Integration tests for MeasuresAccess using sample data."""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pybdl.access.measures import MeasuresAccess


@pytest.mark.integration
class TestMeasuresAccessIntegration:
    """Integration tests for MeasuresAccess with sample data."""

    def test_list_measures(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test list_measures with sample data."""
        samples = load_sample_data("samples_raw_measures.json")
        mock_api_client.list_measures.return_value = samples["list_measures"]
        access = MeasuresAccess(mock_api_client)
        result = access.list_measures()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_measures"])
        assert "id" in result.columns
        assert "name" in result.columns
        assert result["id"].iloc[0] == 1

    def test_get_measure(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_measure with sample data."""
        samples = load_sample_data("samples_raw_measures.json")
        if "get_measure" in samples:
            mock_api_client.get_measure.return_value = samples["get_measure"]
        else:
            mock_api_client.get_measure.return_value = samples["list_measures"][0]
        access = MeasuresAccess(mock_api_client)
        result = access.get_measure(1)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_alist_measures(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async list_measures with sample data."""
        samples = load_sample_data("samples_raw_measures.json")
        mock_async_api_client.alist_measures.return_value = samples["list_measures"]
        access = MeasuresAccess(mock_async_api_client)
        result = await access.alist_measures()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_measures"])

    @pytest.mark.asyncio
    async def test_aget_measure(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_measure with sample data."""
        samples = load_sample_data("samples_raw_measures.json")
        mock_async_api_client.aget_measure.return_value = samples["list_measures"][0]
        access = MeasuresAccess(mock_async_api_client)
        result = await access.aget_measure(1)
        assert isinstance(result, pd.DataFrame)
