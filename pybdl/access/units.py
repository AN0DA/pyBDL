"""Access layer for units API endpoints."""

from typing import Any

import pandas as pd

from pybdl.access.base import BaseAccess


class UnitsAccess(BaseAccess):
    """Access layer for units API, converting responses to DataFrames."""

    def list_units(
        self,
        parent_id: str | None = None,
        level: int | list[int] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        List all administrative units as a DataFrame.

        Args:
            parent_id: Optional parent unit ID.
            level: Optional administrative level (integer or list of integers).
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., name, sort, lang, format, extra_query).

        Returns:
            DataFrame with units data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        # Convert single level to list for API compatibility
        level_list = [level] if isinstance(level, int) else level
        explicit_params = {
            "parent_id": parent_id,
            "level": level_list,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = self.api_client.list_units(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    def get_unit(
        self,
        unit_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve metadata details for a specific administrative unit as a DataFrame.

        Args:
            unit_id: Administrative unit identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with unit metadata.
        """
        data = self.api_client.get_unit(unit_id, **kwargs)
        return self._to_dataframe(data)

    def search_units(
        self,
        name: str | None = None,
        level: int | list[int] | None = None,
        years: list[int] | None = None,
        kind: str | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Search for administrative units by name and optional filters as a DataFrame.

        Args:
            name: Optional substring to search in unit name.
            level: Optional administrative level (integer or list of integers).
            years: Optional list of years to filter by.
            kind: Optional unit kind filter.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with matching units.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        # Convert single level to list for API compatibility
        level_list = [level] if isinstance(level, int) else level
        explicit_params = {
            "name": name,
            "level": level_list,
            "years": years,
            "kind": kind,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = self.api_client.search_units(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    def list_localities(
        self,
        parent_id: str | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        List all statistical localities as a DataFrame.

        Args:
            parent_id: Optional parent unit ID.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., name, level, sort, lang, format, extra_query).

        Returns:
            DataFrame with localities data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "parent_id": parent_id,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = self.api_client.list_localities(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    def get_locality(
        self,
        locality_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve metadata details for a specific statistical locality as a DataFrame.

        Args:
            locality_id: Locality identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with locality metadata.
        """
        data = self.api_client.get_locality(locality_id, **kwargs)
        return self._to_dataframe(data)

    def search_localities(
        self,
        name: str | None = None,
        years: list[int] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Search for statistical localities by name and optional filters as a DataFrame.

        Args:
            name: Optional substring to search in locality name.
            years: Optional list of years to filter by.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., level, parent_id, sort, lang, format, extra_query).

        Returns:
            DataFrame with matching localities.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "name": name,
            "years": years,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = self.api_client.search_localities(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    # Async versions
    async def alist_units(
        self,
        parent_id: str | None = None,
        level: int | list[int] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously list all administrative units as a DataFrame.

        Args:
            parent_id: Optional parent unit ID.
            level: Optional administrative level (integer or list of integers).
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., name, sort, lang, format, extra_query).

        Returns:
            DataFrame with units data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        # Convert single level to list for API compatibility
        level_list = [level] if isinstance(level, int) else level
        explicit_params = {
            "parent_id": parent_id,
            "level": level_list,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = await self.api_client.alist_units(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    async def aget_unit(
        self,
        unit_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously retrieve metadata details for a specific administrative unit as a DataFrame.

        Args:
            unit_id: Administrative unit identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with unit metadata.
        """
        data = await self.api_client.aget_unit(unit_id, **kwargs)
        return self._to_dataframe(data)

    async def asearch_units(
        self,
        name: str | None = None,
        level: int | list[int] | None = None,
        years: list[int] | None = None,
        kind: str | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously search for administrative units by name and optional filters as a DataFrame.

        Args:
            name: Optional substring to search in unit name.
            level: Optional administrative level (integer or list of integers).
            years: Optional list of years to filter by.
            kind: Optional unit kind filter.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with matching units.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        # Convert single level to list for API compatibility
        level_list = [level] if isinstance(level, int) else level
        explicit_params = {
            "name": name,
            "level": level_list,
            "years": years,
            "kind": kind,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = await self.api_client.asearch_units(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    async def alist_localities(
        self,
        parent_id: str | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously list all statistical localities as a DataFrame.

        Args:
            parent_id: Optional parent unit ID.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., name, level, sort, lang, format, extra_query).

        Returns:
            DataFrame with localities data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "parent_id": parent_id,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = await self.api_client.alist_localities(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    async def aget_locality(
        self,
        locality_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously retrieve metadata details for a specific statistical locality as a DataFrame.

        Args:
            locality_id: Locality identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with locality metadata.
        """
        data = await self.api_client.aget_locality(locality_id, **kwargs)
        return self._to_dataframe(data)

    async def asearch_localities(
        self,
        name: str | None = None,
        years: list[int] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously search for statistical localities by name and optional filters as a DataFrame.

        Args:
            name: Optional substring to search in locality name.
            years: Optional list of years to filter by.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., level, parent_id, sort, lang, format, extra_query).

        Returns:
            DataFrame with matching localities.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "name": name,
            "years": years,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = await self.api_client.asearch_localities(**resolved_params, **kwargs)
        return self._to_dataframe(data)
