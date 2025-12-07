"""Unit tests for DataAccess class."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pybdl.access.data import DataAccess


@pytest.mark.unit
class TestDataAccess:
    """Test DataAccess class."""

    @pytest.fixture
    def mock_api_client(self) -> MagicMock:
        """Create a mock API client."""
        return MagicMock()

    @pytest.fixture
    def data_access(self, mock_api_client: MagicMock) -> DataAccess:
        """Create a DataAccess instance."""
        return DataAccess(mock_api_client)

    def test_get_data_by_variable(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test get_data_by_variable method."""
        mock_data = [{"id": "000000000000", "name": "POLSKA", "values": [{"year": "2020", "val": 100, "attrId": 1}]}]
        mock_api_client.get_data_by_variable.return_value = mock_data
        result = data_access.get_data_by_variable("3643")
        assert isinstance(result, pd.DataFrame)
        assert "unit_id" in result.columns
        assert "unit_name" in result.columns
        assert "year" in result.columns
        assert "val" in result.columns
        assert "attr_id" in result.columns

    def test_get_data_by_variable_with_metadata(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test get_data_by_variable with return_metadata=True."""
        mock_data = [{"id": "000000000000", "name": "POLSKA", "values": [{"year": "2020", "val": 100, "attrId": 1}]}]
        mock_api_client.get_data_by_variable.return_value = (mock_data, {"total": 1})
        result = data_access.get_data_by_variable("3643", return_metadata=True)
        assert isinstance(result, tuple)
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], dict)

    def test_get_data_by_variable_empty_values(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test get_data_by_variable with empty values."""
        mock_data = [{"id": "000000000000", "name": "POLSKA", "values": []}]
        mock_api_client.get_data_by_variable.return_value = mock_data
        result = data_access.get_data_by_variable("3643")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1  # Should include parent row even with empty values

    def test_get_data_by_unit(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test get_data_by_unit method."""
        mock_api_client.get_data_by_unit.return_value = [{"id": "1", "value": 100}]
        result = data_access.get_data_by_unit("999", variable_ids=["3643"])
        assert isinstance(result, pd.DataFrame)
        mock_api_client.get_data_by_unit.assert_called_once()

    def test_get_data_by_unit_with_metadata(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test get_data_by_unit with return_metadata=True."""
        mock_api_client.get_data_by_unit.return_value = ([{"id": "1", "value": 100}], {"total": 1})
        result = data_access.get_data_by_unit("999", variable_ids=["3643"], return_metadata=True)
        assert isinstance(result, tuple)
        assert isinstance(result[0], pd.DataFrame)

    def test_get_data_by_variable_locality(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test get_data_by_variable_locality method."""
        mock_api_client.get_data_by_variable_locality.return_value = [{"id": "1", "value": 100}]
        result = data_access.get_data_by_variable_locality("7", "2")
        assert isinstance(result, pd.DataFrame)
        mock_api_client.get_data_by_variable_locality.assert_called_once()

    def test_get_data_by_unit_locality(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test get_data_by_unit_locality method."""
        mock_api_client.get_data_by_unit_locality.return_value = [{"id": "1", "value": 100}]
        result = data_access.get_data_by_unit_locality("44", variable_id=[3643])
        assert isinstance(result, pd.DataFrame)
        mock_api_client.get_data_by_unit_locality.assert_called_once()

    def test_get_data_by_variable_year_conversion(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test that year is converted to integer."""
        mock_data = [{"id": "000000000000", "name": "POLSKA", "values": [{"year": "2020", "val": 100, "attrId": 1}]}]
        mock_api_client.get_data_by_variable.return_value = mock_data
        result = data_access.get_data_by_variable("3643")
        assert isinstance(result, pd.DataFrame)
        assert result["year"].dtype == "Int64"
        assert result["year"].iloc[0] == 2020

    def test_get_data_by_variable_error(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test error propagation."""
        mock_api_client.get_data_by_variable.side_effect = ValueError("API Error")
        with pytest.raises(ValueError, match="API Error"):
            data_access.get_data_by_variable("3643")

    @pytest.mark.asyncio
    async def test_aget_data_by_variable(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test async get_data_by_variable method."""
        mock_data = [{"id": "000000000000", "name": "POLSKA", "values": [{"year": "2020", "val": 100, "attrId": 1}]}]
        mock_api_client.aget_data_by_variable = AsyncMock(return_value=mock_data)
        result = await data_access.aget_data_by_variable("3643")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_data_by_unit(self, data_access: DataAccess, mock_api_client: MagicMock) -> None:
        """Test async get_data_by_unit method."""
        mock_api_client.aget_data_by_unit = AsyncMock(return_value=[{"id": "1", "value": 100}])
        result = await data_access.aget_data_by_unit("999", variable_ids=["3643"])
        assert isinstance(result, pd.DataFrame)
