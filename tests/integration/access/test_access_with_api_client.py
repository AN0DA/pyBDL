"""Integration tests for access layer with API client."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pybdl.access.levels import LevelsAccess


@pytest.mark.integration
class TestAccessWithAPIClient:
    """Test access layer integration with API clients."""

    def test_levels_access_calls_api_client(self) -> None:
        """Test that LevelsAccess properly calls API client."""
        mock_api = MagicMock()
        mock_api.list_levels.return_value = [{"id": 1, "name": "Level1"}, {"id": 2, "name": "Level2"}]
        access = LevelsAccess(mock_api)
        result = access.list_levels()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        mock_api.list_levels.assert_called_once()

    def test_error_propagation_from_api_to_access(self) -> None:
        """Test that API errors are properly propagated."""
        mock_api = MagicMock()
        mock_api.list_levels.side_effect = ValueError("API Error")
        access = LevelsAccess(mock_api)
        with pytest.raises(ValueError, match="API Error"):
            access.list_levels()

    def test_dataframe_transformation(self) -> None:
        """Test that API responses are properly transformed to DataFrames."""
        mock_api = MagicMock()
        mock_api.list_levels.return_value = [{"id": 1, "name": "Level1", "camelCase": "value"}]
        access = LevelsAccess(mock_api)
        result = access.list_levels()
        assert isinstance(result, pd.DataFrame)
        assert "camel_case" in result.columns
        # When no NaN values, pandas may use int64 instead of Int64
        assert result["id"].dtype in ("Int64", "int64")

    @pytest.mark.asyncio
    async def test_async_methods_call_async_api(self) -> None:
        """Test that async access methods call async API methods."""
        mock_api = MagicMock()
        mock_api.alist_levels = AsyncMock(return_value=[{"id": 1, "name": "Level1"}])
        access = LevelsAccess(mock_api)
        result = await access.alist_levels()
        assert isinstance(result, pd.DataFrame)
        mock_api.alist_levels.assert_called_once()
