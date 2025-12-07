"""Access layer for attributes API endpoints."""

from typing import Any

import pandas as pd

from pybdl.access.base import BaseAccess


class AttributesAccess(BaseAccess):
    """Access layer for attributes API, converting responses to DataFrames."""

    def list_attributes(
        self,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        List all attributes as a DataFrame.

        Args:
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with attributes data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = self.api_client.list_attributes(page_size=page_size, max_pages=max_pages, **kwargs)
        return self._to_dataframe(data)

    def get_attribute(
        self,
        attribute_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve metadata details for a specific attribute as a DataFrame.

        Args:
            attribute_id: Attribute identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with attribute metadata.
        """
        data = self.api_client.get_attribute(attribute_id, **kwargs)
        return self._to_dataframe(data)

    async def alist_attributes(
        self,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously list all attributes as a DataFrame.

        Args:
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with attributes data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        data = await self.api_client.alist_attributes(page_size=page_size, max_pages=max_pages, **kwargs)
        return self._to_dataframe(data)

    async def aget_attribute(
        self,
        attribute_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously retrieve metadata details for a specific attribute as a DataFrame.

        Args:
            attribute_id: Attribute identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with attribute metadata.
        """
        data = await self.api_client.aget_attribute(attribute_id, **kwargs)
        return self._to_dataframe(data)
