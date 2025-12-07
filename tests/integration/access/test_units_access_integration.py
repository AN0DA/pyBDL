"""Integration tests for UnitsAccess using sample data."""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pybdl.access.units import UnitsAccess


@pytest.mark.integration
class TestUnitsAccessIntegration:
    """Integration tests for UnitsAccess with sample data."""

    def test_list_units(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test list_units with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        mock_api_client.list_units.return_value = samples["list_units"]
        access = UnitsAccess(mock_api_client)
        result = access.list_units()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_units"])
        assert "id" in result.columns
        assert "name" in result.columns
        # ID may be converted to numeric, check string representation or actual value
        first_id = result["id"].iloc[0]
        assert str(first_id) == "000000000000" or first_id == 0
        assert result["name"].iloc[0] == "POLSKA"

    def test_get_unit(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_unit with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        if "get_unit" in samples:
            mock_api_client.get_unit.return_value = samples["get_unit"]
        else:
            mock_api_client.get_unit.return_value = samples["list_units"][0]
        access = UnitsAccess(mock_api_client)
        result = access.get_unit("000000000000")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_search_units(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test search_units with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        mock_api_client.search_units.return_value = samples["list_units"][:2]
        access = UnitsAccess(mock_api_client)
        result = access.search_units("POLSKA")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_list_localities(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test list_localities with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        if "list_localities" in samples:
            mock_api_client.list_localities.return_value = samples["list_localities"]
        else:
            mock_api_client.list_localities.return_value = samples["list_units"][:2]
        access = UnitsAccess(mock_api_client)
        result = access.list_localities()
        assert isinstance(result, pd.DataFrame)

    def test_get_locality(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_locality with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        if "get_locality" in samples:
            mock_api_client.get_locality.return_value = samples["get_locality"]
        else:
            mock_api_client.get_locality.return_value = samples["list_units"][0]
        access = UnitsAccess(mock_api_client)
        result = access.get_locality("000000000000")
        assert isinstance(result, pd.DataFrame)

    def test_search_localities(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test search_localities with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        if "search_localities" in samples:
            mock_api_client.search_localities.return_value = samples["search_localities"]
        else:
            mock_api_client.search_localities.return_value = samples["list_units"][:2]
        access = UnitsAccess(mock_api_client)
        result = access.search_localities("POLSKA")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_alist_units(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async list_units with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        mock_async_api_client.alist_units.return_value = samples["list_units"]
        access = UnitsAccess(mock_async_api_client)
        result = await access.alist_units()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_units"])

    @pytest.mark.asyncio
    async def test_aget_unit(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_unit with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        mock_async_api_client.aget_unit.return_value = samples["list_units"][0]
        access = UnitsAccess(mock_async_api_client)
        result = await access.aget_unit("000000000000")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_asearch_units(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async search_units with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        mock_async_api_client.asearch_units.return_value = samples["list_units"][:2]
        access = UnitsAccess(mock_async_api_client)
        result = await access.asearch_units("POLSKA")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_alist_localities(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async list_localities with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        mock_async_api_client.alist_localities.return_value = samples["list_units"][:2]
        access = UnitsAccess(mock_async_api_client)
        result = await access.alist_localities()
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_locality(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_locality with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        mock_async_api_client.aget_locality.return_value = samples["list_units"][0]
        access = UnitsAccess(mock_async_api_client)
        result = await access.aget_locality("000000000000")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_asearch_localities(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async search_localities with sample data."""
        samples = load_sample_data("samples_raw_units.json")
        mock_async_api_client.asearch_localities.return_value = samples["list_units"][:2]
        access = UnitsAccess(mock_async_api_client)
        result = await access.asearch_localities("POLSKA")
        assert isinstance(result, pd.DataFrame)
