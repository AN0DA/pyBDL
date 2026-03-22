"""Access layer for data API endpoints with nested data normalization."""

from collections.abc import Sequence
from typing import Any

import pandas as pd

from pybdl.access.base import BaseAccess
from pybdl.access.enrichment import AGGREGATES_SPEC, ATTRIBUTES_SPEC, UNITS_SPEC, with_enrichment


class DataAccess(BaseAccess):
    """Access layer for data API, converting responses to DataFrames with nested data normalization."""

    @staticmethod
    def _split_dataframe_result(
        result: pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]],
        *,
        return_metadata: bool,
    ) -> tuple[pd.DataFrame, dict[str, Any]] | pd.DataFrame:
        if return_metadata:
            return result
        if isinstance(result, tuple):
            return result[0]
        return result

    @staticmethod
    def _extract_metadata(
        result: Any,
        *,
        return_metadata: bool,
    ) -> tuple[Any, dict[str, Any]]:
        if return_metadata:
            data, metadata = result
            return data, metadata
        return result, {}

    @staticmethod
    def _normalize_variable_ids(
        variable_ids: Sequence[str | int] | str | int | None,
        kwargs: dict[str, Any],
    ) -> list[int]:
        legacy_value = kwargs.pop("variable_id", None)
        if variable_ids is not None and legacy_value is not None:
            raise TypeError("Use either 'variable_ids' or the legacy 'variable_id' parameter, not both.")
        resolved = variable_ids if variable_ids is not None else legacy_value
        if resolved is None:
            raise TypeError("'variable_ids' is required.")
        items = [resolved] if isinstance(resolved, (str, int)) else list(resolved)
        return [int(item) for item in items]

    def _normalize_variable_dataframe(self, data: list[dict[str, Any]]) -> pd.DataFrame:
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
            if "attrId" in item:
                item["attr_id"] = item.pop("attrId")

        df = self._to_dataframe(normalized_data)
        if "year" in df.columns:
            df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        return df

    def _to_dataframe_result(
        self,
        result: Any,
        *,
        return_metadata: bool,
        normalize_variable_values: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        data, metadata = self._extract_metadata(result, return_metadata=return_metadata)
        df = self._normalize_variable_dataframe(data) if normalize_variable_values else self._to_dataframe(data)
        return (df, metadata) if return_metadata else df

    @with_enrichment(UNITS_SPEC, ATTRIBUTES_SPEC, AGGREGATES_SPEC)
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
            **kwargs: Additional parameters passed to API layer, including `enrich=["units", ...]`
                or legacy `enrich_units=True`-style flags.

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
        return self._to_dataframe_result(result, return_metadata=return_metadata, normalize_variable_values=True)

    def get_data_by_variable_with_metadata(self, *args: Any, **kwargs: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Retrieve data by variable and always return `(dataframe, metadata)`."""
        kwargs["return_metadata"] = True
        return self.get_data_by_variable(*args, **kwargs)

    @with_enrichment(UNITS_SPEC, ATTRIBUTES_SPEC, AGGREGATES_SPEC)
    def get_data_by_unit(
        self,
        unit_id: str,
        variable_ids: Sequence[str | int] | str | int | None = None,
        years: list[int] | None = None,
        aggregate_id: int | None = None,
        return_metadata: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        """
        Retrieve statistical data for a specific administrative unit as a DataFrame.

        Args:
            unit_id: Identifier of the administrative unit.
            variable_ids: Variable ID or sequence of variable IDs.
            years: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer, including `enrich=["units", ...]`
                or legacy `enrich_units=True`-style flags.

        Returns:
            DataFrame with data, or tuple (DataFrame, metadata) if return_metadata=True.
        """
        variable_id_list = self._normalize_variable_ids(variable_ids, kwargs)
        explicit_params = {
            "unit_id": unit_id,
            "variable_ids": variable_id_list,
            "years": years,
            "aggregate_id": aggregate_id,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = self.api_client.get_data_by_unit(**resolved_params, **kwargs)
        return self._to_dataframe_result(result, return_metadata=return_metadata)

    def get_data_by_unit_with_metadata(self, *args: Any, **kwargs: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Retrieve data by unit and always return `(dataframe, metadata)`."""
        kwargs["return_metadata"] = True
        return self.get_data_by_unit(*args, **kwargs)

    @with_enrichment(UNITS_SPEC, ATTRIBUTES_SPEC, AGGREGATES_SPEC)
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
            **kwargs: Additional parameters passed to API layer, including `enrich=["units", ...]`
                or legacy `enrich_units=True`-style flags.

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
        return self._to_dataframe_result(result, return_metadata=return_metadata)

    def get_data_by_variable_locality_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Retrieve locality data by variable and always return `(dataframe, metadata)`."""
        kwargs["return_metadata"] = True
        return self.get_data_by_variable_locality(*args, **kwargs)

    @with_enrichment(UNITS_SPEC, ATTRIBUTES_SPEC, AGGREGATES_SPEC)
    def get_data_by_unit_locality(
        self,
        unit_id: str,
        variable_ids: Sequence[str | int] | str | int | None = None,
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
            variable_ids: Variable ID or sequence of variable IDs.
            years: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer, including `enrich=["units", ...]`
                or legacy `enrich_units=True`-style flags.

        Returns:
            DataFrame with data, or tuple (DataFrame, metadata) if return_metadata=True.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        variable_id_list = self._normalize_variable_ids(variable_ids, kwargs)
        explicit_params = {
            "unit_id": unit_id,
            "variable_ids": variable_id_list,
            "years": years,
            "aggregate_id": aggregate_id,
            "page_size": page_size,
            "max_pages": max_pages,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = self.api_client.get_data_by_unit_locality(**resolved_params, **kwargs)
        return self._to_dataframe_result(result, return_metadata=return_metadata)

    def get_data_by_unit_locality_with_metadata(self, *args: Any, **kwargs: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Retrieve locality data by unit and always return `(dataframe, metadata)`."""
        kwargs["return_metadata"] = True
        return self.get_data_by_unit_locality(*args, **kwargs)

    # Async versions
    @with_enrichment(UNITS_SPEC, ATTRIBUTES_SPEC, AGGREGATES_SPEC)
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
            **kwargs: Additional parameters passed to API layer, including `enrich=["units", ...]`
                or legacy `enrich_units=True`-style flags.

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
        return self._to_dataframe_result(result, return_metadata=return_metadata, normalize_variable_values=True)

    async def aget_data_by_variable_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Asynchronously retrieve data by variable and always return `(dataframe, metadata)`."""
        kwargs["return_metadata"] = True
        return await self.aget_data_by_variable(*args, **kwargs)

    @with_enrichment(UNITS_SPEC, ATTRIBUTES_SPEC, AGGREGATES_SPEC)
    async def aget_data_by_unit(
        self,
        unit_id: str,
        variable_ids: Sequence[str | int] | str | int | None = None,
        years: list[int] | None = None,
        aggregate_id: int | None = None,
        return_metadata: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
        """
        Asynchronously retrieve statistical data for a specific administrative unit as a DataFrame.

        Args:
            unit_id: Identifier of the administrative unit.
            variable_ids: Variable ID or sequence of variable IDs.
            years: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer, including `enrich=["units", ...]`
                or legacy `enrich_units=True`-style flags.

        Returns:
            DataFrame with data, or tuple (DataFrame, metadata) if return_metadata=True.
        """
        variable_id_list = self._normalize_variable_ids(variable_ids, kwargs)
        explicit_params = {
            "unit_id": unit_id,
            "variable_ids": variable_id_list,
            "years": years,
            "aggregate_id": aggregate_id,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = await self.api_client.aget_data_by_unit(**resolved_params, **kwargs)
        return self._to_dataframe_result(result, return_metadata=return_metadata)

    async def aget_data_by_unit_with_metadata(self, *args: Any, **kwargs: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Asynchronously retrieve data by unit and always return `(dataframe, metadata)`."""
        kwargs["return_metadata"] = True
        return await self.aget_data_by_unit(*args, **kwargs)

    @with_enrichment(UNITS_SPEC, ATTRIBUTES_SPEC, AGGREGATES_SPEC)
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
            **kwargs: Additional parameters passed to API layer, including `enrich=["units", ...]`
                or legacy `enrich_units=True`-style flags.

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
        return self._to_dataframe_result(result, return_metadata=return_metadata)

    async def aget_data_by_variable_locality_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Asynchronously retrieve locality data by variable and always return `(dataframe, metadata)`."""
        kwargs["return_metadata"] = True
        return await self.aget_data_by_variable_locality(*args, **kwargs)

    @with_enrichment(UNITS_SPEC, ATTRIBUTES_SPEC, AGGREGATES_SPEC)
    async def aget_data_by_unit_locality(
        self,
        unit_id: str,
        variable_ids: Sequence[str | int] | str | int | None = None,
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
            variable_ids: Variable ID or sequence of variable IDs.
            years: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            page_size: Number of results per page (defaults to config.page_size or 100).
            max_pages: Maximum number of pages to fetch (None for all pages).
            return_metadata: If True, return tuple (DataFrame, metadata).
            **kwargs: Additional parameters passed to API layer, including `enrich=["units", ...]`
                or legacy `enrich_units=True`-style flags.

        Returns:
            DataFrame with data, or tuple (DataFrame, metadata) if return_metadata=True.
        """
        if page_size is None:
            page_size = self._get_default_page_size()
        variable_id_list = self._normalize_variable_ids(variable_ids, kwargs)
        explicit_params = {
            "unit_id": unit_id,
            "variable_ids": variable_id_list,
            "years": years,
            "aggregate_id": aggregate_id,
            "page_size": page_size,
            "max_pages": max_pages,
            "return_metadata": return_metadata,
        }
        resolved_params = self._resolve_api_params(explicit_params, kwargs)
        result = await self.api_client.aget_data_by_unit_locality(**resolved_params, **kwargs)
        return self._to_dataframe_result(result, return_metadata=return_metadata)

    async def aget_data_by_unit_locality_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Asynchronously retrieve locality data by unit and always return `(dataframe, metadata)`."""
        kwargs["return_metadata"] = True
        return await self.aget_data_by_unit_locality(*args, **kwargs)
