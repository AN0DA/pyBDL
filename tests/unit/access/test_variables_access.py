"""Unit tests for VariablesAccess class."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pyldb.access.variables import VariablesAccess


@pytest.mark.unit
class TestVariablesAccess:
    """Test VariablesAccess class."""

    @pytest.fixture
    def mock_api_client(self) -> MagicMock:
        """Create a mock API client."""
        return MagicMock()

    @pytest.fixture
    def variables_access(self, mock_api_client: MagicMock) -> VariablesAccess:
        """Create a VariablesAccess instance."""
        return VariablesAccess(mock_api_client)

    def test_list_variables(self, variables_access: VariablesAccess, mock_api_client: MagicMock) -> None:
        """Test list_variables method."""
        mock_api_client.list_variables.return_value = [{"id": "1", "name": "Variable1"}]
        result = variables_access.list_variables()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        mock_api_client.list_variables.assert_called_once()

    def test_get_variable(self, variables_access: VariablesAccess, mock_api_client: MagicMock) -> None:
        """Test get_variable method."""
        mock_api_client.get_variable.return_value = {"id": "1", "name": "Variable1"}
        result = variables_access.get_variable("1")
        assert isinstance(result, pd.DataFrame)
        mock_api_client.get_variable.assert_called_once_with("1")

    def test_search_variables(self, variables_access: VariablesAccess, mock_api_client: MagicMock) -> None:
        """Test search_variables method."""
        mock_api_client.search_variables.return_value = [{"id": "1", "name": "Variable1"}]
        result = variables_access.search_variables("test")
        assert isinstance(result, pd.DataFrame)
        mock_api_client.search_variables.assert_called_once()

    def test_list_variables_error(self, variables_access: VariablesAccess, mock_api_client: MagicMock) -> None:
        """Test error propagation."""
        mock_api_client.list_variables.side_effect = ValueError("API Error")
        with pytest.raises(ValueError, match="API Error"):
            variables_access.list_variables()

    @pytest.mark.asyncio
    async def test_alist_variables(self, variables_access: VariablesAccess, mock_api_client: MagicMock) -> None:
        """Test async list_variables method."""
        mock_api_client.alist_variables = AsyncMock(return_value=[{"id": "1", "name": "Variable1"}])
        result = await variables_access.alist_variables()
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_variable(self, variables_access: VariablesAccess, mock_api_client: MagicMock) -> None:
        """Test async get_variable method."""
        mock_api_client.aget_variable = AsyncMock(return_value={"id": "1", "name": "Variable1"})
        result = await variables_access.aget_variable("1")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_asearch_variables(self, variables_access: VariablesAccess, mock_api_client: MagicMock) -> None:
        """Test async search_variables method."""
        mock_api_client.asearch_variables = AsyncMock(return_value=[{"id": "1", "name": "Variable1"}])
        result = await variables_access.asearch_variables("test")
        assert isinstance(result, pd.DataFrame)
