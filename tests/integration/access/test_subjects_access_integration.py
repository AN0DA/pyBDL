"""Integration tests for SubjectsAccess using sample data."""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pyldb.access.subjects import SubjectsAccess


@pytest.mark.integration
class TestSubjectsAccessIntegration:
    """Integration tests for SubjectsAccess with sample data."""

    def test_list_subjects(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test list_subjects with sample data."""
        samples = load_sample_data("samples_raw_subjects.json")
        mock_api_client.list_subjects.return_value = samples["list_subjects"]
        access = SubjectsAccess(mock_api_client)
        result = access.list_subjects()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_subjects"])
        assert "id" in result.columns
        assert "name" in result.columns
        assert result["id"].iloc[0] == "K15"
        assert result["name"].iloc[0] == "CENY"

    def test_get_subject(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test get_subject with sample data."""
        samples = load_sample_data("samples_raw_subjects.json")
        if "get_subject" in samples:
            mock_api_client.get_subject.return_value = samples["get_subject"]
            access = SubjectsAccess(mock_api_client)
            result = access.get_subject("K15")
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1

    def test_search_subjects(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test search_subjects with sample data."""
        samples = load_sample_data("samples_raw_subjects.json")
        mock_api_client.search_subjects.return_value = samples["list_subjects"][:2]
        access = SubjectsAccess(mock_api_client)
        result = access.search_subjects("CENY")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


    @pytest.mark.asyncio
    async def test_alist_subjects(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async list_subjects with sample data."""
        samples = load_sample_data("samples_raw_subjects.json")
        mock_async_api_client.alist_subjects.return_value = samples["list_subjects"]
        access = SubjectsAccess(mock_async_api_client)
        result = await access.alist_subjects()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(samples["list_subjects"])

    @pytest.mark.asyncio
    async def test_aget_subject(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async get_subject with sample data."""
        samples = load_sample_data("samples_raw_subjects.json")
        if "get_subject" in samples:
            mock_async_api_client.aget_subject.return_value = samples["get_subject"]
            access = SubjectsAccess(mock_async_api_client)
            result = await access.aget_subject("K15")
            assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_asearch_subjects(
        self,
        mock_async_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """Test async search_subjects with sample data."""
        samples = load_sample_data("samples_raw_subjects.json")
        mock_async_api_client.asearch_subjects.return_value = samples["list_subjects"][:2]
        access = SubjectsAccess(mock_async_api_client)
        result = await access.asearch_subjects("CENY")
        assert isinstance(result, pd.DataFrame)

