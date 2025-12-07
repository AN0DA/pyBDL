"""Access layer for aggregates API endpoints."""

from typing import Any

import pandas as pd

from pyldb.access.base import BaseAccess


class AggregatesAccess(BaseAccess):
    """
    Access layer for aggregates API, converting responses to DataFrames.
    
    Example column renaming:
        _column_renames = {
            "list_aggregates": {
                "id": "aggregate_id",
                "name": "aggregate_name",
            },
            "get_aggregate": {
                "id": "aggregate_id",
            },
        }
    """

    def list_aggregates(
        self,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        List all aggregates as a DataFrame.

        Args:
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with aggregates data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = self.api_client.list_aggregates(page_size=page_size, max_pages=max_pages, **kwargs)
        return self._to_dataframe(data)

    def get_aggregate(
        self,
        aggregate_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve metadata details for a specific aggregate as a DataFrame.

        Args:
            aggregate_id: Aggregate identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with aggregate metadata.
        """
        data = self.api_client.get_aggregate(aggregate_id, **kwargs)
        return self._to_dataframe(data)


    async def alist_aggregates(
        self,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously list all aggregates as a DataFrame.

        Args:
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with aggregates data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = await self.api_client.alist_aggregates(page_size=page_size, max_pages=max_pages, **kwargs)
        return self._to_dataframe(data)

    async def aget_aggregate(
        self,
        aggregate_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously retrieve metadata details for a specific aggregate as a DataFrame.

        Args:
            aggregate_id: Aggregate identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with aggregate metadata.
        """
        data = await self.api_client.aget_aggregate(aggregate_id, **kwargs)
        return self._to_dataframe(data)


