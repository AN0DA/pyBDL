"""Unit tests for YearsAccess class."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pyldb.access.years import YearsAccess


@pytest.mark.unit
class TestYearsAccess:
    """Test YearsAccess class."""

    @pytest.fixture
    def mock_api_client(self) -> MagicMock:
        """Create a mock API client."""
        return MagicMock()

    @pytest.fixture
    def years_access(self, mock_api_client: MagicMock) -> YearsAccess:
        """Create a YearsAccess instance."""
        return YearsAccess(mock_api_client)

    def test_list_years(self, years_access: YearsAccess, mock_api_client: MagicMock) -> None:
        """Test list_years method."""
        mock_api_client.list_years.return_value = [{"id": 2020, "name": "2020"}]
        result = years_access.list_years()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        mock_api_client.list_years.assert_called_once()

    def test_get_year(self, years_access: YearsAccess, mock_api_client: MagicMock) -> None:
        """Test get_year method."""
        mock_api_client.get_year.return_value = {"id": 2020, "name": "2020"}
        result = years_access.get_year(2020)
        assert isinstance(result, pd.DataFrame)
        mock_api_client.get_year.assert_called_once_with(2020)

    def test_list_years_error(self, years_access: YearsAccess, mock_api_client: MagicMock) -> None:
        """Test error propagation."""
        mock_api_client.list_years.side_effect = ValueError("API Error")
        with pytest.raises(ValueError, match="API Error"):
            years_access.list_years()

    @pytest.mark.asyncio
    async def test_alist_years(self, years_access: YearsAccess, mock_api_client: MagicMock) -> None:
        """Test async list_years method."""
        mock_api_client.alist_years = AsyncMock(return_value=[{"id": 2020, "name": "2020"}])
        result = await years_access.alist_years()
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_year(self, years_access: YearsAccess, mock_api_client: MagicMock) -> None:
        """Test async get_year method."""
        mock_api_client.aget_year = AsyncMock(return_value={"id": 2020, "name": "2020"})
        result = await years_access.aget_year(2020)
        assert isinstance(result, pd.DataFrame)
