"""Unit tests for AttributesAccess class."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pyldb.access.attributes import AttributesAccess


@pytest.mark.unit
class TestAttributesAccess:
    """Test AttributesAccess class."""

    @pytest.fixture
    def mock_api_client(self) -> MagicMock:
        """Create a mock API client."""
        return MagicMock()

    @pytest.fixture
    def attributes_access(self, mock_api_client: MagicMock) -> AttributesAccess:
        """Create an AttributesAccess instance."""
        return AttributesAccess(mock_api_client)

    def test_list_attributes(self, attributes_access: AttributesAccess, mock_api_client: MagicMock) -> None:
        """Test list_attributes method."""
        mock_api_client.list_attributes.return_value = [{"id": "1", "name": "Attribute1"}]
        result = attributes_access.list_attributes()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        mock_api_client.list_attributes.assert_called_once()

    def test_get_attribute(self, attributes_access: AttributesAccess, mock_api_client: MagicMock) -> None:
        """Test get_attribute method."""
        mock_api_client.get_attribute.return_value = {"id": "1", "name": "Attribute1"}
        result = attributes_access.get_attribute("1")
        assert isinstance(result, pd.DataFrame)
        mock_api_client.get_attribute.assert_called_once_with("1")


    def test_list_attributes_error(self, attributes_access: AttributesAccess, mock_api_client: MagicMock) -> None:
        """Test error propagation."""
        mock_api_client.list_attributes.side_effect = ValueError("API Error")
        with pytest.raises(ValueError, match="API Error"):
            attributes_access.list_attributes()

    @pytest.mark.asyncio
    async def test_alist_attributes(self, attributes_access: AttributesAccess, mock_api_client: MagicMock) -> None:
        """Test async list_attributes method."""
        mock_api_client.alist_attributes = AsyncMock(return_value=[{"id": "1", "name": "Attribute1"}])
        result = await attributes_access.alist_attributes()
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_attribute(self, attributes_access: AttributesAccess, mock_api_client: MagicMock) -> None:
        """Test async get_attribute method."""
        mock_api_client.aget_attribute = AsyncMock(return_value={"id": "1", "name": "Attribute1"})
        result = await attributes_access.aget_attribute("1")
        assert isinstance(result, pd.DataFrame)

