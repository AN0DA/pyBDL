"""Unit tests for LevelsAccess class."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pyldb.access.levels import LevelsAccess


@pytest.mark.unit
class TestLevelsAccess:
    """Test LevelsAccess class."""

    @pytest.fixture
    def mock_api_client(self) -> MagicMock:
        """Create a mock API client."""
        return MagicMock()

    @pytest.fixture
    def levels_access(self, mock_api_client: MagicMock) -> LevelsAccess:
        """Create a LevelsAccess instance."""
        return LevelsAccess(mock_api_client)

    def test_list_levels(self, levels_access: LevelsAccess, mock_api_client: MagicMock) -> None:
        """Test list_levels method."""
        mock_api_client.list_levels.return_value = [{"id": 1, "name": "Level1"}]
        result = levels_access.list_levels()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        mock_api_client.list_levels.assert_called_once()

    def test_list_levels_with_sort(self, levels_access: LevelsAccess, mock_api_client: MagicMock) -> None:
        """Test list_levels with sort parameter."""
        mock_api_client.list_levels.return_value = [{"id": 1, "name": "Level1"}]
        levels_access.list_levels(sort="Name")
        mock_api_client.list_levels.assert_called_once()
        # Check that sort was passed in the call
        call_args = mock_api_client.list_levels.call_args
        assert call_args.kwargs.get("sort") == "Name"

    def test_get_level(self, levels_access: LevelsAccess, mock_api_client: MagicMock) -> None:
        """Test get_level method."""
        mock_api_client.get_level.return_value = {"id": 1, "name": "Level1"}
        result = levels_access.get_level(1)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        mock_api_client.get_level.assert_called_once_with(1)

    def test_list_levels_empty(self, levels_access: LevelsAccess, mock_api_client: MagicMock) -> None:
        """Test list_levels with empty response."""
        mock_api_client.list_levels.return_value = []
        result = levels_access.list_levels()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_list_levels_error(self, levels_access: LevelsAccess, mock_api_client: MagicMock) -> None:
        """Test error propagation."""
        mock_api_client.list_levels.side_effect = ValueError("API Error")
        with pytest.raises(ValueError, match="API Error"):
            levels_access.list_levels()

    @pytest.mark.asyncio
    async def test_alist_levels(self, levels_access: LevelsAccess, mock_api_client: MagicMock) -> None:
        """Test async list_levels method."""
        mock_api_client.alist_levels = AsyncMock(return_value=[{"id": 1, "name": "Level1"}])
        result = await levels_access.alist_levels()
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_level(self, levels_access: LevelsAccess, mock_api_client: MagicMock) -> None:
        """Test async get_level method."""
        mock_api_client.aget_level = AsyncMock(return_value={"id": 1, "name": "Level1"})
        result = await levels_access.aget_level(1)
        assert isinstance(result, pd.DataFrame)
