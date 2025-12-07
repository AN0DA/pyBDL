"""Integration tests for AttributesAccess using sample data."""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pyldb.access.attributes import AttributesAccess


@pytest.mark.integration
class TestAttributesAccessIntegration:
    """Integration tests for AttributesAccess with sample data."""

    def test_list_attributes(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test list_attributes with sample data."""
        samples = load_sample_data("samples_raw_attributes.json")
        mock_api_client.list_attributes.return_value = samples["list_attributes"]
        access = AttributesAccess(mock_api_client)
        result = access.list_attributes()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_attributes"])
        assert "id" in result.columns
        assert "name" in result.columns
        assert result["id"].iloc[0] == 0

    def test_get_attribute(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_attribute with sample data."""
        samples = load_sample_data("samples_raw_attributes.json")
        if "get_attribute" in samples:
            mock_api_client.get_attribute.return_value = samples["get_attribute"]
            access = AttributesAccess(mock_api_client)
            result = access.get_attribute("0")
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
        else:
            # Use first item from list if get_attribute not available
            mock_api_client.get_attribute.return_value = samples["list_attributes"][0]
            access = AttributesAccess(mock_api_client)
            result = access.get_attribute("0")
            assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_alist_attributes(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async list_attributes with sample data."""
        samples = load_sample_data("samples_raw_attributes.json")
        mock_async_api_client.alist_attributes.return_value = samples["list_attributes"]
        access = AttributesAccess(mock_async_api_client)
        result = await access.alist_attributes()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_attributes"])

    @pytest.mark.asyncio
    async def test_aget_attribute(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_attribute with sample data."""
        samples = load_sample_data("samples_raw_attributes.json")
        mock_async_api_client.aget_attribute.return_value = samples["list_attributes"][0]
        access = AttributesAccess(mock_async_api_client)
        result = await access.aget_attribute("0")
        assert isinstance(result, pd.DataFrame)
