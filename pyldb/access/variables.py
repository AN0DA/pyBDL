"""Access layer for variables API endpoints."""

from typing import Any

import pandas as pd

from pyldb.access.base import BaseAccess


class VariablesAccess(BaseAccess):
    """Access layer for variables API, converting responses to DataFrames."""

    def list_variables(
        self,
        subject_id: str | None = None,
        level: int | None = None,
        years: list[int] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        List all variables as a DataFrame.

        Args:
            subject_id: Optional subject ID to filter variables.
            level: Optional level to filter variables.
            years: Optional list of years to filter variables.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with variables data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "subject_id": subject_id,
            "level": level,
            "years": years,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = self.api_client.list_variables(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    def get_variable(
        self,
        variable_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve metadata details for a specific variable as a DataFrame.

        Args:
            variable_id: Variable identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with variable metadata.
        """
        data = self.api_client.get_variable(variable_id, **kwargs)
        return self._to_dataframe(data)

    def search_variables(
        self,
        name: str | None = None,
        subject_id: str | None = None,
        level: int | None = None,
        years: list[int] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Search for variables by name and optional filters as a DataFrame.

        Args:
            name: Optional substring to search in variable name.
            subject_id: Optional subject ID to filter variables.
            level: Optional level to filter variables.
            years: Optional list of years to filter variables.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with matching variables.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "name": name,
            "subject_id": subject_id,
            "level": level,
            "years": years,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = self.api_client.search_variables(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    async def alist_variables(
        self,
        subject_id: str | None = None,
        level: int | None = None,
        years: list[int] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously list all variables as a DataFrame.

        Args:
            subject_id: Optional subject ID to filter variables.
            level: Optional level to filter variables.
            years: Optional list of years to filter variables.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with variables data.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "subject_id": subject_id,
            "level": level,
            "years": years,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = await self.api_client.alist_variables(**resolved_params, **kwargs)
        return self._to_dataframe(data)

    async def aget_variable(
        self,
        variable_id: str,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously retrieve metadata details for a specific variable as a DataFrame.

        Args:
            variable_id: Variable identifier.
            **kwargs: Additional parameters passed to API layer (e.g., lang, format, extra_query).

        Returns:
            DataFrame with variable metadata.
        """
        data = await self.api_client.aget_variable(variable_id, **kwargs)
        return self._to_dataframe(data)

    async def asearch_variables(
        self,
        name: str | None = None,
        subject_id: str | None = None,
        level: int | None = None,
        years: list[int] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Asynchronously search for variables by name and optional filters as a DataFrame.

        Args:
            name: Optional substring to search in variable name.
            subject_id: Optional subject ID to filter variables.
            level: Optional level to filter variables.
            years: Optional list of years to filter variables.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            **kwargs: Additional parameters passed to API layer (e.g., sort, lang, format, extra_query).

        Returns:
            DataFrame with matching variables.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "name": name,
            "subject_id": subject_id,
            "level": level,
            "years": years,
            "page_size": page_size,
            "max_pages": max_pages,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        data = await self.api_client.asearch_variables(**resolved_params, **kwargs)
        return self._to_dataframe(data)

