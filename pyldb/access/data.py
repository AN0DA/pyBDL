"""Access layer for data API endpoints with nested data normalization."""

from typing import Any

import pandas as pd

from pyldb.access.base import BaseAccess


class DataAccess(BaseAccess):
    """Access layer for data API, converting responses to DataFrames with nested data normalization."""

    def get_data_by_variable(
        self,
        variable_id: str,
        years: list[int] | None = None,
        unit_parent_id: str | None = None,
        unit_level: int | None = None,
        aggregate_id: int | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        return_metadata: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        """
        Retrieve statistical data for a specific variable as a DataFrame.

        The nested 'values' array is normalized into separate rows, with each row containing:
        unit_id, unit_name, year, val, attr_id.

        Args:
            variable_id: Identifier of the variable.
            years: Optional list of years to filter by.
            unit_parent_id: Optional parent administrative unit ID.
            unit_level: Optional administrative unit aggregation level.
            aggregate_id: Optional aggregate ID.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer (e.g., format, lang, extra_query).

        Returns:
            DataFrame with normalized data, or tuple (DataFrame, metadata) if return_metadata is True.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "variable_id": variable_id,
            "years": years,
            "unit_parent_id": unit_parent_id,
            "unit_level": unit_level,
            "aggregate_id": aggregate_id,
            "page_size": page_size,
            "max_pages": max_pages,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = self.api_client.get_data_by_variable(**resolved_params, **kwargs)

        if return_metadata:
            data, metadata = result
        else:
            data = result
            metadata = {}

        # Normalize nested data structure
        normalized_data = self._normalize_nested_data(
            data,
            nested_key="values",
            parent_keys=["id", "name"],
        )

        # Rename parent keys for clarity and normalize nested keys
        for item in normalized_data:
            if "id" in item:
                item["unit_id"] = item.pop("id")
            if "name" in item:
                item["unit_name"] = item.pop("name")
            # Normalize camelCase keys in nested values (e.g., attrId -> attr_id)
            if "attrId" in item:
                item["attr_id"] = item.pop("attrId")

        df = self._to_dataframe(normalized_data)

        # Convert year to integer if present
        if "year" in df.columns:
            df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

        return (df, metadata) if return_metadata else df

    def get_data_by_unit(
        self,
        unit_id: str,
        variable_ids: list[str],
        years: list[int] | None = None,
        aggregate_id: int | None = None,
        return_metadata: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        """
        Retrieve statistical data for a specific administrative unit as a DataFrame.

        Args:
            unit_id: Identifier of the administrative unit.
            variable_ids: List of variable IDs (as strings) to get results.
            years: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer (e.g., format, lang, extra_query).

        Returns:
            DataFrame with data, or tuple (DataFrame, metadata) if return_metadata=True.
        """
        # Convert variable_ids (list[str]) to list[int] for API compatibility
        variable_id_list = [int(var_id) for var_id in variable_ids]
        explicit_params = {
            "unit_id": unit_id,
            "variable_id": variable_id_list,
            "years": years,
            "aggregate_id": aggregate_id,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = self.api_client.get_data_by_unit(**resolved_params, **kwargs)

        if return_metadata:
            data, metadata = result
        else:
            data = result
            metadata = {}

        df = self._to_dataframe(data)
        return (df, metadata) if return_metadata else df

    def get_data_by_variable_locality(
        self,
        variable_id: str,
        unit_parent_id: str,
        years: list[int] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        return_metadata: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        """
        Retrieve data for a variable within a specific locality as a DataFrame.

        Args:
            variable_id: Identifier of the variable.
            unit_parent_id: Parent unit ID (required).
            years: Optional list of years to filter by.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer (e.g., format, lang, extra_query).

        Returns:
            DataFrame with data, or tuple (DataFrame, metadata) if return_metadata=True.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "variable_id": variable_id,
            "unit_parent_id": unit_parent_id,
            "years": years,
            "page_size": page_size,
            "max_pages": max_pages,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = self.api_client.get_data_by_variable_locality(**resolved_params, **kwargs)

        if return_metadata:
            data, metadata = result
        else:
            data = result
            metadata = {}

        df = self._to_dataframe(data)
        return (df, metadata) if return_metadata else df

    def get_data_by_unit_locality(
        self,
        unit_id: str,
        variable_id: list[int] | int,
        years: list[int] | None = None,
        aggregate_id: int | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        return_metadata: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        """
        Retrieve data for a single statistical locality by unit as a DataFrame.

        Args:
            unit_id: Identifier of the statistical locality.
            variable_id: Variable ID or list of variable IDs to filter.
            years: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer (e.g., format, lang, extra_query).

        Returns:
            DataFrame with data, or tuple (DataFrame, metadata) if return_metadata=True.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        # Convert single variable_id to list for API compatibility
        variable_id_list = [variable_id] if isinstance(variable_id, int) else variable_id
        explicit_params = {
            "unit_id": unit_id,
            "variable_id": variable_id_list,
            "years": years,
            "aggregate_id": aggregate_id,
            "page_size": page_size,
            "max_pages": max_pages,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = self.api_client.get_data_by_unit_locality(**resolved_params, **kwargs)

        if return_metadata:
            data, metadata = result
        else:
            data = result
            metadata = {}

        df = self._to_dataframe(data)
        return (df, metadata) if return_metadata else df

    # Async versions
    async def aget_data_by_variable(
        self,
        variable_id: str,
        years: list[int] | None = None,
        unit_parent_id: str | None = None,
        unit_level: int | None = None,
        aggregate_id: int | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        return_metadata: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        """
        Asynchronously retrieve statistical data for a specific variable as a DataFrame.

        Args:
            variable_id: Identifier of the variable.
            years: Optional list of years to filter by.
            unit_parent_id: Optional parent administrative unit ID.
            unit_level: Optional administrative unit aggregation level.
            aggregate_id: Optional aggregate ID.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer (e.g., format, lang, extra_query).

        Returns:
            DataFrame with normalized data, or tuple (DataFrame, metadata) if return_metadata is True.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "variable_id": variable_id,
            "years": years,
            "unit_parent_id": unit_parent_id,
            "unit_level": unit_level,
            "aggregate_id": aggregate_id,
            "page_size": page_size,
            "max_pages": max_pages,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = await self.api_client.aget_data_by_variable(**resolved_params, **kwargs)

        if return_metadata:
            data, metadata = result
        else:
            data = result
            metadata = {}

        normalized_data = self._normalize_nested_data(
            data,
            nested_key="values",
            parent_keys=["id", "name"],
        )

        for item in normalized_data:
            if "id" in item:
                item["unit_id"] = item.pop("id")
            if "name" in item:
                item["unit_name"] = item.pop("name")
            # Normalize camelCase keys in nested values (e.g., attrId -> attr_id)
            if "attrId" in item:
                item["attr_id"] = item.pop("attrId")

        df = self._to_dataframe(normalized_data)

        if "year" in df.columns:
            df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

        return (df, metadata) if return_metadata else df

    async def aget_data_by_unit(
        self,
        unit_id: str,
        variable_ids: list[str],
        years: list[int] | None = None,
        aggregate_id: int | None = None,
        return_metadata: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        """
        Asynchronously retrieve statistical data for a specific administrative unit as a DataFrame.

        Args:
            unit_id: Identifier of the administrative unit.
            variable_ids: List of variable IDs (as strings) to get results.
            years: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer (e.g., format, lang, extra_query).

        Returns:
            DataFrame with data, or tuple (DataFrame, metadata) if return_metadata=True.
        """
        # Convert variable_ids (list[str]) to list[int] for API compatibility
        variable_id_list = [int(var_id) for var_id in variable_ids]
        explicit_params = {
            "unit_id": unit_id,
            "variable_id": variable_id_list,
            "years": years,
            "aggregate_id": aggregate_id,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = await self.api_client.aget_data_by_unit(**resolved_params, **kwargs)

        if return_metadata:
            data, metadata = result
        else:
            data = result
            metadata = {}

        df = self._to_dataframe(data)
        return (df, metadata) if return_metadata else df

    async def aget_data_by_variable_locality(
        self,
        variable_id: str,
        unit_parent_id: str,
        years: list[int] | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        return_metadata: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        """
        Asynchronously retrieve data for a variable within a specific locality as a DataFrame.

        Args:
            variable_id: Identifier of the variable.
            unit_parent_id: Parent unit ID (required).
            years: Optional list of years to filter by.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer (e.g., format, lang, extra_query).

        Returns:
            DataFrame with data, or tuple (DataFrame, metadata) if return_metadata=True.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        explicit_params = {
            "variable_id": variable_id,
            "unit_parent_id": unit_parent_id,
            "years": years,
            "page_size": page_size,
            "max_pages": max_pages,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = await self.api_client.aget_data_by_variable_locality(**resolved_params, **kwargs)

        if return_metadata:
            data, metadata = result
        else:
            data = result
            metadata = {}

        df = self._to_dataframe(data)
        return (df, metadata) if return_metadata else df

    async def aget_data_by_unit_locality(
        self,
        unit_id: str,
        variable_id: list[int] | int,
        years: list[int] | None = None,
        aggregate_id: int | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        return_metadata: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        """
        Asynchronously retrieve data for a single statistical locality by unit as a DataFrame.

        Args:
            unit_id: Identifier of the statistical locality.
            variable_id: Variable ID or list of variable IDs to filter.
            years: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer (e.g., format, lang, extra_query).

        Returns:
            DataFrame with data, or tuple (DataFrame, metadata) if return_metadata=True.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        # Convert single variable_id to list for API compatibility
        variable_id_list = [variable_id] if isinstance(variable_id, int) else variable_id
        explicit_params = {
            "unit_id": unit_id,
            "variable_id": variable_id_list,
            "years": years,
            "aggregate_id": aggregate_id,
            "page_size": page_size,
            "max_pages": max_pages,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = await self.api_client.aget_data_by_unit_locality(**resolved_params, **kwargs)

        if return_metadata:
            data, metadata = result
        else:
            data = result
            metadata = {}

        df = self._to_dataframe(data)
        return (df, metadata) if return_metadata else df
