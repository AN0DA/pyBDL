"""Unit tests for AggregatesAccess class."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pybdl.access.aggregates import AggregatesAccess


@pytest.mark.unit
class TestAggregatesAccess:
    """Test AggregatesAccess class."""

    @pytest.fixture
    def mock_api_client(self) -> MagicMock:
        """Create a mock API client."""
        return MagicMock()

    @pytest.fixture
    def aggregates_access(self, mock_api_client: MagicMock) -> AggregatesAccess:
        """Create an AggregatesAccess instance."""
        return AggregatesAccess(mock_api_client)

    def test_list_aggregates(self, aggregates_access: AggregatesAccess, mock_api_client: MagicMock) -> None:
        """Test list_aggregates method."""
        mock_api_client.list_aggregates.return_value = [{"id": "1", "name": "Aggregate1"}]
        result = aggregates_access.list_aggregates()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        mock_api_client.list_aggregates.assert_called_once()

    def test_get_aggregate(self, aggregates_access: AggregatesAccess, mock_api_client: MagicMock) -> None:
        """Test get_aggregate method."""
        mock_api_client.get_aggregate.return_value = {"id": "1", "name": "Aggregate1"}
        result = aggregates_access.get_aggregate("1")
        assert isinstance(result, pd.DataFrame)
        mock_api_client.get_aggregate.assert_called_once_with("1")

    def test_list_aggregates_error(self, aggregates_access: AggregatesAccess, mock_api_client: MagicMock) -> None:
        """Test error propagation."""
        mock_api_client.list_aggregates.side_effect = ValueError("API Error")
        with pytest.raises(ValueError, match="API Error"):
            aggregates_access.list_aggregates()

    @pytest.mark.asyncio
    async def test_alist_aggregates(self, aggregates_access: AggregatesAccess, mock_api_client: MagicMock) -> None:
        """Test async list_aggregates method."""
        mock_api_client.alist_aggregates = AsyncMock(return_value=[{"id": "1", "name": "Aggregate1"}])
        result = await aggregates_access.alist_aggregates()
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_aggregate(self, aggregates_access: AggregatesAccess, mock_api_client: MagicMock) -> None:
        """Test async get_aggregate method."""
        mock_api_client.aget_aggregate = AsyncMock(return_value={"id": "1", "name": "Aggregate1"})
        result = await aggregates_access.aget_aggregate("1")
        assert isinstance(result, pd.DataFrame)
