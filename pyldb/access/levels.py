"""Access layer for levels API endpoints."""

from typing import Any

import pandas as pd

from pyldb.access.base import BaseAccess


class LevelsAccess(BaseAccess):
    """Access layer for levels API, converting responses to DataFrames."""

    def list_levels(
        self,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        List all administrative unit aggregation levels as a DataFrame.

        Args:
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with levels data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = self.api_client.list_levels(page_size=page_size, max_pages=max_pages, **kwargs)
        return self._to_dataframe(data)

    def get_level(
        self,
        level_id: int,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve metadata for a specific aggregation level as a DataFrame.

        Args:
            level_id: Aggregation level identifier (integer).
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with level metadata.
        """
        data = self.api_client.get_level(level_id, **kwargs)
        return self._to_dataframe(data)

    async def alist_levels(
        self,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously list all administrative unit aggregation levels as a DataFrame.

        Args:
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with levels data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = await self.api_client.alist_levels(page_size=page_size, max_pages=max_pages, **kwargs)
        return self._to_dataframe(data)

    async def aget_level(
        self,
        level_id: int,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously retrieve metadata for a specific aggregation level as a DataFrame.

        Args:
            level_id: Aggregation level identifier (integer).
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with level metadata.
        """
        data = await self.api_client.aget_level(level_id, **kwargs)
        return self._to_dataframe(data)
