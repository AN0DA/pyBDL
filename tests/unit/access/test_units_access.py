"""Unit tests for UnitsAccess class."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pybdl.access.units import UnitsAccess


@pytest.mark.unit
class TestUnitsAccess:
    """Test UnitsAccess class."""

    @pytest.fixture
    def mock_api_client(self) -> MagicMock:
        """Create a mock API client."""
        return MagicMock()

    @pytest.fixture
    def units_access(self, mock_api_client: MagicMock) -> UnitsAccess:
        """Create a UnitsAccess instance."""
        return UnitsAccess(mock_api_client)

    def test_list_units(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test list_units method."""
        mock_api_client.list_units.return_value = [{"id": "1", "name": "Unit1"}]
        result = units_access.list_units()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        mock_api_client.list_units.assert_called_once()

    def test_get_unit(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test get_unit method."""
        mock_api_client.get_unit.return_value = {"id": "1", "name": "Unit1"}
        result = units_access.get_unit("1")
        assert isinstance(result, pd.DataFrame)
        mock_api_client.get_unit.assert_called_once_with("1")

    def test_search_units(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test search_units method."""
        mock_api_client.search_units.return_value = [{"id": "1", "name": "Unit1"}]
        result = units_access.search_units("test")
        assert isinstance(result, pd.DataFrame)
        mock_api_client.search_units.assert_called_once()

    def test_list_localities(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test list_localities method."""
        mock_api_client.list_localities.return_value = [{"id": "1", "name": "Locality1"}]
        result = units_access.list_localities()
        assert isinstance(result, pd.DataFrame)
        mock_api_client.list_localities.assert_called_once()

    def test_get_locality(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test get_locality method."""
        mock_api_client.get_locality.return_value = {"id": "1", "name": "Locality1"}
        result = units_access.get_locality("1")
        assert isinstance(result, pd.DataFrame)
        mock_api_client.get_locality.assert_called_once_with("1")

    def test_search_localities(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test search_localities method."""
        mock_api_client.search_localities.return_value = [{"id": "1", "name": "Locality1"}]
        result = units_access.search_localities("test")
        assert isinstance(result, pd.DataFrame)
        mock_api_client.search_localities.assert_called_once()

    def test_list_units_error(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test error propagation."""
        mock_api_client.list_units.side_effect = ValueError("API Error")
        with pytest.raises(ValueError, match="API Error"):
            units_access.list_units()

    @pytest.mark.asyncio
    async def test_alist_units(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test async list_units method."""
        mock_api_client.alist_units = AsyncMock(return_value=[{"id": "1", "name": "Unit1"}])
        result = await units_access.alist_units()
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_unit(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test async get_unit method."""
        mock_api_client.aget_unit = AsyncMock(return_value={"id": "1", "name": "Unit1"})
        result = await units_access.aget_unit("1")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_asearch_units(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test async search_units method."""
        mock_api_client.asearch_units = AsyncMock(return_value=[{"id": "1", "name": "Unit1"}])
        result = await units_access.asearch_units("test")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_alist_localities(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test async list_localities method."""
        mock_api_client.alist_localities = AsyncMock(return_value=[{"id": "1", "name": "Locality1"}])
        result = await units_access.alist_localities()
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_locality(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test async get_locality method."""
        mock_api_client.aget_locality = AsyncMock(return_value={"id": "1", "name": "Locality1"})
        result = await units_access.aget_locality("1")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_asearch_localities(self, units_access: UnitsAccess, mock_api_client: MagicMock) -> None:
        """Test async search_localities method."""
        mock_api_client.asearch_localities = AsyncMock(return_value=[{"id": "1", "name": "Locality1"}])
        result = await units_access.asearch_localities("test")
        assert isinstance(result, pd.DataFrame)
