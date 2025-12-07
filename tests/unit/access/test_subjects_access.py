"""Unit tests for SubjectsAccess class."""

from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from pyldb.access.subjects import SubjectsAccess


@pytest.mark.unit
class TestSubjectsAccess:
    """Test SubjectsAccess class."""

    @pytest.fixture
    def mock_api_client(self) -> MagicMock:
        """Create a mock API client."""
        return MagicMock()

    @pytest.fixture
    def subjects_access(self, mock_api_client: MagicMock) -> SubjectsAccess:
        """Create a SubjectsAccess instance."""
        return SubjectsAccess(mock_api_client)

    def test_list_subjects(self, subjects_access: SubjectsAccess, mock_api_client: MagicMock) -> None:
        """Test list_subjects method."""
        mock_api_client.list_subjects.return_value = [{"id": "1", "name": "Subject1"}]
        result = subjects_access.list_subjects()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "name" in result.columns
        mock_api_client.list_subjects.assert_called_once()

    def test_list_subjects_with_params(self, subjects_access: SubjectsAccess, mock_api_client: MagicMock) -> None:
        """Test list_subjects with parameters."""
        mock_api_client.list_subjects.return_value = [{"id": "1", "name": "Subject1"}]
        subjects_access.list_subjects(parent_id="parent1", sort="name", page_size=50)
        mock_api_client.list_subjects.assert_called_once()
        # Check that the expected parameters were passed
        call_args = mock_api_client.list_subjects.call_args
        assert call_args.kwargs.get("parent_id") == "parent1"
        assert call_args.kwargs.get("sort") == "name"
        assert call_args.kwargs.get("page_size") == 50
        assert call_args.kwargs.get("max_pages") is None

    def test_get_subject(self, subjects_access: SubjectsAccess, mock_api_client: MagicMock) -> None:
        """Test get_subject method."""
        mock_api_client.get_subject.return_value = {"id": "1", "name": "Subject1"}
        result = subjects_access.get_subject("1")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        mock_api_client.get_subject.assert_called_once_with("1")

    def test_search_subjects(self, subjects_access: SubjectsAccess, mock_api_client: MagicMock) -> None:
        """Test search_subjects method."""
        mock_api_client.search_subjects.return_value = [{"id": "1", "name": "Subject1"}]
        result = subjects_access.search_subjects("test")
        assert isinstance(result, pd.DataFrame)
        mock_api_client.search_subjects.assert_called_once()

    def test_list_subjects_empty_response(self, subjects_access: SubjectsAccess, mock_api_client: MagicMock) -> None:
        """Test list_subjects with empty response."""
        mock_api_client.list_subjects.return_value = []
        result = subjects_access.list_subjects()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_list_subjects_error_propagation(self, subjects_access: SubjectsAccess, mock_api_client: MagicMock) -> None:
        """Test that API errors are propagated."""
        mock_api_client.list_subjects.side_effect = ValueError("API Error")
        with pytest.raises(ValueError, match="API Error"):
            subjects_access.list_subjects()

    @pytest.mark.asyncio
    async def test_alist_subjects(self, subjects_access: SubjectsAccess, mock_api_client: MagicMock) -> None:
        """Test async list_subjects method."""
        mock_api_client.alist_subjects = AsyncMock(return_value=[{"id": "1", "name": "Subject1"}])
        result = await subjects_access.alist_subjects()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_aget_subject(self, subjects_access: SubjectsAccess, mock_api_client: MagicMock) -> None:
        """Test async get_subject method."""
        mock_api_client.aget_subject = AsyncMock(return_value={"id": "1", "name": "Subject1"})
        result = await subjects_access.aget_subject("1")
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_asearch_subjects(self, subjects_access: SubjectsAccess, mock_api_client: MagicMock) -> None:
        """Test async search_subjects method."""
        mock_api_client.asearch_subjects = AsyncMock(return_value=[{"id": "1", "name": "Subject1"}])
        result = await subjects_access.asearch_subjects("test")
        assert isinstance(result, pd.DataFrame)
