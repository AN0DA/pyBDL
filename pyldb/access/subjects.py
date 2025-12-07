"""Access layer for subjects API endpoints."""

from typing import Any

import pandas as pd

from pyldb.access.base import BaseAccess


class SubjectsAccess(BaseAccess):
    """Access layer for subjects API, converting responses to DataFrames."""

    def list_subjects(
        self,
        parent_id: str | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        List all subjects as a DataFrame.

        Args:
            parent_id: Optional parent subject ID. If not specified, returns all top-level subjects.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with subjects data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "parent_id": parent_id,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = self.api_client.list_subjects(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    def get_subject(
        self,
        subject_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve metadata for a specific subject as a DataFrame.

        Args:
            subject_id: Subject identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with subject metadata.
        """
        data = self.api_client.get_subject(subject_id, **kwargs)
        return self._to_dataframe(data)

    def search_subjects(
        self,
        name: str,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Search for subjects by name as a DataFrame.

        Args:
            name: Subject name to search for.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with matching subjects.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = self.api_client.search_subjects(
            name=name,
            page_size=page_size,
            max_pages=max_pages,
            **kwargs,
        )
        return self._to_dataframe(data)

    async def alist_subjects(
        self,
        parent_id: str | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously list all subjects as a DataFrame.

        Args:
            parent_id: Optional parent subject ID. If not specified, returns all top-level subjects.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with subjects data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "parent_id": parent_id,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = await self.api_client.alist_subjects(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    async def aget_subject(
        self,
        subject_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously retrieve metadata for a specific subject as a DataFrame.

        Args:
            subject_id: Subject identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with subject metadata.
        """
        data = await self.api_client.aget_subject(subject_id, **kwargs)
        return self._to_dataframe(data)

    async def asearch_subjects(
        self,
        name: str,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously search for subjects by name as a DataFrame.

        Args:
            name: Subject name to search for.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with matching subjects.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = await self.api_client.asearch_subjects(
            name=name,
            page_size=page_size,
            max_pages=max_pages,
            **kwargs,
        )
        return self._to_dataframe(data)
