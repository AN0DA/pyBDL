"""Access layer for years API endpoints."""

from typing import Any

import pandas as pd

from pyldb.access.base import BaseAccess


class YearsAccess(BaseAccess):
    """Access layer for years API, converting responses to DataFrames."""

    def list_years(
        self,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        List all available years as a DataFrame.

        Args:
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with available years.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = self.api_client.list_years(page_size=page_size, max_pages=max_pages, **kwargs)
        return self._to_dataframe(data)

    def get_year(
        self,
        year_id: int,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve metadata for a specific year as a DataFrame.

        Args:
            year_id: Year identifier (integer, e.g. 2020).
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with year metadata.
        """
        data = self.api_client.get_year(year_id, **kwargs)
        return self._to_dataframe(data)

    async def alist_years(
        self,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously list all available years as a DataFrame.

        Args:
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with available years.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = await self.api_client.alist_years(page_size=page_size, max_pages=max_pages, **kwargs)
        return self._to_dataframe(data)

    async def aget_year(
        self,
        year_id: int,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously retrieve metadata for a specific year as a DataFrame.

        Args:
            year_id: Year identifier (integer, e.g. 2020).
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with year metadata.
        """
        data = await self.api_client.aget_year(year_id, **kwargs)
        return self._to_dataframe(data)
