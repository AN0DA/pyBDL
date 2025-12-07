"""Unit tests for MeasuresAccess class."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pyldb.access.measures import MeasuresAccess


@pytest.mark.unit
class TestMeasuresAccess:
    """Test MeasuresAccess class."""

    @pytest.fixture
    def mock_api_client(self) -> MagicMock:
        """Create a mock API client."""
        return MagicMock()

    @pytest.fixture
    def measures_access(self, mock_api_client: MagicMock) -> MeasuresAccess:
        """Create a MeasuresAccess instance."""
        return MeasuresAccess(mock_api_client)

    def test_list_measures(self, measures_access: MeasuresAccess, mock_api_client: MagicMock) -> None:
        """Test list_measures method."""
        mock_api_client.list_measures.return_value = [{"id": 1, "name": "Measure1"}]
        result = measures_access.list_measures()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        mock_api_client.list_measures.assert_called_once()

    def test_get_measure(self, measures_access: MeasuresAccess, mock_api_client: MagicMock) -> None:
        """Test get_measure method."""
        mock_api_client.get_measure.return_value = {"id": 1, "name": "Measure1"}
        result = measures_access.get_measure(1)
        assert isinstance(result, pd.DataFrame)
        mock_api_client.get_measure.assert_called_once_with(1)


    def test_list_measures_error(self, measures_access: MeasuresAccess, mock_api_client: MagicMock) -> None:
        """Test error propagation."""
        mock_api_client.list_measures.side_effect = ValueError("API Error")
        with pytest.raises(ValueError, match="API Error"):
            measures_access.list_measures()

    @pytest.mark.asyncio
    async def test_alist_measures(self, measures_access: MeasuresAccess, mock_api_client: MagicMock) -> None:
        """Test async list_measures method."""
        mock_api_client.alist_measures = AsyncMock(return_value=[{"id": 1, "name": "Measure1"}])
        result = await measures_access.alist_measures()
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_measure(self, measures_access: MeasuresAccess, mock_api_client: MagicMock) -> None:
        """Test async get_measure method."""
        mock_api_client.aget_measure = AsyncMock(return_value={"id": 1, "name": "Measure1"})
        result = await measures_access.aget_measure(1)
        assert isinstance(result, pd.DataFrame)

