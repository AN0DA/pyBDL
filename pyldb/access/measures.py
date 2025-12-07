"""Access layer for measures API endpoints."""

from typing import Any

import pandas as pd

from pyldb.access.base import BaseAccess


class MeasuresAccess(BaseAccess):
    """Access layer for measures API, converting responses to DataFrames."""

    def list_measures(
        self,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        List all measure units as a DataFrame.

        Args:
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with measures data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = self.api_client.list_measures(page_size=page_size, max_pages=max_pages, **kwargs)
        return self._to_dataframe(data)

    def get_measure(
        self,
        measure_id: int,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve metadata for a specific measure unit as a DataFrame.

        Args:
            measure_id: Measure unit identifier (integer).
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with measure unit metadata.
        """
        data = self.api_client.get_measure(measure_id, **kwargs)
        return self._to_dataframe(data)

    async def alist_measures(
        self,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously list all measure units as a DataFrame.

        Args:
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with measures data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = await self.api_client.alist_measures(page_size=page_size, max_pages=max_pages, **kwargs)
        return self._to_dataframe(data)

    async def aget_measure(
        self,
        measure_id: int,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously retrieve metadata for a specific measure unit as a DataFrame.

        Args:
            measure_id: Measure unit identifier (integer).
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with measure unit metadata.
        """
        data = await self.api_client.aget_measure(measure_id, **kwargs)
        return self._to_dataframe(data)
