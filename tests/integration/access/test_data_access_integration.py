"""Integration tests for DataAccess using sample data."""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pyldb.access.data import DataAccess


@pytest.mark.integration
class TestDataAccessIntegration:
    """Integration tests for DataAccess with sample data."""

    def test_get_data_by_variable(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_data_by_variable with sample data."""
        samples = load_sample_data("samples_raw_data.json")
        mock_api_client.get_data_by_variable.return_value = samples["get_data_by_variable"]
        access = DataAccess(mock_api_client)
        result = access.get_data_by_variable("3643")
        assert isinstance(result, pd.DataFrame)
        # Check that nested data was normalized
        assert "unit_id" in result.columns
        assert "unit_name" in result.columns
        assert "year" in result.columns
        assert "val" in result.columns
        assert "attr_id" in result.columns
        # Check that year was converted to integer
        assert result["year"].dtype in ("Int64", "int64")

    def test_get_data_by_variable_with_metadata(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_data_by_variable with return_metadata=True."""
        samples = load_sample_data("samples_raw_data.json")
        mock_api_client.get_data_by_variable.return_value = (
            samples["get_data_by_variable"],
            {"total": len(samples["get_data_by_variable"])},
        )
        access = DataAccess(mock_api_client)
        result = access.get_data_by_variable("3643", return_metadata=True)
        assert isinstance(result, tuple)
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], dict)
        assert result[1]["total"] == len(samples["get_data_by_variable"])

    def test_get_data_by_unit(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_data_by_unit with sample data."""
        samples = load_sample_data("samples_raw_data.json")
        if "get_data_by_unit" in samples:
            mock_api_client.get_data_by_unit.return_value = samples["get_data_by_unit"]
            access = DataAccess(mock_api_client)
            result = access.get_data_by_unit("999", variable_id="3643")
            assert isinstance(result, pd.DataFrame)

    def test_get_data_by_unit_with_metadata(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_data_by_unit with return_metadata=True."""
        samples = load_sample_data("samples_raw_data.json")
        if "get_data_by_unit" in samples:
            mock_api_client.get_data_by_unit.return_value = (
                samples["get_data_by_unit"],
                {"total": 1},
            )
            access = DataAccess(mock_api_client)
            result = access.get_data_by_unit("999", variable_id="3643", return_metadata=True)
            assert isinstance(result, tuple)
            assert isinstance(result[0], pd.DataFrame)

    def test_get_data_by_variable_locality(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_data_by_variable_locality with sample data."""
        samples = load_sample_data("samples_raw_data.json")
        if "get_data_by_variable_locality" in samples:
            mock_api_client.get_data_by_variable_locality.return_value = samples["get_data_by_variable_locality"]
            access = DataAccess(mock_api_client)
            result = access.get_data_by_variable_locality("7", "2")
            assert isinstance(result, pd.DataFrame)

    def test_get_data_by_unit_locality(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_data_by_unit_locality with sample data."""
        samples = load_sample_data("samples_raw_data.json")
        if "get_data_by_unit_locality" in samples:
            mock_api_client.get_data_by_unit_locality.return_value = samples["get_data_by_unit_locality"]
            access = DataAccess(mock_api_client)
            result = access.get_data_by_unit_locality(unit_id="44", variable_id=[3643])
            assert isinstance(result, pd.DataFrame)


    @pytest.mark.asyncio
    async def test_aget_data_by_variable(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_data_by_variable with sample data."""
        samples = load_sample_data("samples_raw_data.json")
        mock_async_api_client.aget_data_by_variable.return_value = samples["get_data_by_variable"]
        access = DataAccess(mock_async_api_client)
        result = await access.aget_data_by_variable("3643")
        assert isinstance(result, pd.DataFrame)
        assert "unit_id" in result.columns
        assert "year" in result.columns

    @pytest.mark.asyncio
    async def test_aget_data_by_unit(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_data_by_unit with sample data."""
        samples = load_sample_data("samples_raw_data.json")
        if "get_data_by_unit" in samples:
            mock_async_api_client.aget_data_by_unit.return_value = samples["get_data_by_unit"]
            access = DataAccess(mock_async_api_client)
            result = await access.aget_data_by_unit(unit_id="999", variable_id="3643")
            assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_data_by_variable_locality(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_data_by_variable_locality with sample data."""
        samples = load_sample_data("samples_raw_data.json")
        if "get_data_by_variable_locality" in samples:
            mock_async_api_client.aget_data_by_variable_locality.return_value = samples["get_data_by_variable_locality"]
            access = DataAccess(mock_async_api_client)
            result = await access.aget_data_by_variable_locality("7", "2")
            assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_aget_data_by_unit_locality(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_data_by_unit_locality with sample data."""
        samples = load_sample_data("samples_raw_data.json")
        if "get_data_by_unit_locality" in samples:
            mock_async_api_client.aget_data_by_unit_locality.return_value = samples["get_data_by_unit_locality"]
            access = DataAccess(mock_async_api_client)
            result = await access.aget_data_by_unit_locality(unit_id="44", variable_id=[3643])
            assert isinstance(result, pd.DataFrame)

