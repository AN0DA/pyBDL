"""Base access class for converting API responses to DataFrames."""

import inspect
from typing import Any

import pandas as pd


class BaseAccess:
    """
    Base class for access layer implementations.

    Supports per-function column renaming through the `_column_renames` class attribute.
    Child classes can define column rename mappings that apply to both sync and async methods.

    Example:
        class MyAccess(BaseAccess):
            _column_renames = {
                "list_items": {
                    "id": "item_id",
                    "name": "item_name",
                },
                "get_item": {
                    "id": "item_id",
                },
            }
    """

    # Column rename configuration: maps function_name -> {old_column: new_column}
    # Both sync and async methods use the same mapping (async methods are normalized by removing 'a' prefix)
    _column_renames: dict[str, dict[str, str]] = {}

    def __init__(self, api_client: Any):
        """
        Initialize base access class.

        Args:
            api_client: API client instance (e.g., LevelsAPI, AttributesAPI).
        """
        self.api_client = api_client

    def _get_default_page_size(self) -> int:
        """
        Get default page size from config.

        Returns:
            Default page size from config, or 100 if not available.
        """
        if hasattr(self.api_client, "config") and hasattr(self.api_client.config, "page_size"):
            return self.api_client.config.page_size
        return 100

    def _resolve_api_params(
        self,
        explicit_params: dict[str, Any],
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Resolve API parameters, giving precedence to kwargs over explicit parameters.

        For any parameter present in both explicit_params and kwargs, kwargs takes precedence.
        This prevents duplicate parameter errors when calling API client methods.

        Args:
            explicit_params: Dictionary of explicitly passed parameters.
            kwargs: Keyword arguments dict (will be modified to remove resolved params).

        Returns:
            Dictionary of resolved parameters (kwargs values override explicit values).

        Example:
            explicit = {"max_pages": None, "page_size": 100}
            kwargs = {"max_pages": 1, "extra": "value"}
            resolved = self._resolve_api_params(explicit, kwargs)
            # Returns: {"max_pages": 1, "page_size": 100}
            # kwargs now: {"extra": "value"}  (max_pages removed)
        """
        resolved = explicit_params.copy()

        # For each key in kwargs that matches an explicit parameter, use kwargs value
        for key in list(kwargs.keys()):
            if key in resolved:
                # kwargs takes precedence - use its value and remove from kwargs
                resolved[key] = kwargs.pop(key)

        return resolved

    def _to_dataframe(
        self,
        data: list[dict[str, Any]] | dict[str, Any],
    ) -> pd.DataFrame:
        """
        Convert API response to DataFrame with proper column names and data types.

        Automatically detects the calling function name to apply column renames defined
        in `_column_renames`. Both sync and async methods are supported (async names are
        normalized by removing 'a' prefix).

        Args:
            data: List of dictionaries or single dictionary from API response.

        Returns:
            DataFrame with normalized column names and proper data types.
        """
        # Handle single dict by converting to list
        if isinstance(data, dict):
            data = [data]

        if not data:
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Normalize column names (camelCase to snake_case)
        df.columns = [self._camel_to_snake(col) for col in df.columns]

        # Infer and convert data types
        df = self._infer_dtypes(df)

        # Automatically detect calling function name for column renames
        function_name = self._get_calling_function_name()
        if function_name:
            df = self._apply_column_renames(df, function_name)

        return df

    def _get_calling_function_name(self) -> str | None:
        """
        Get the name of the function that called `_to_dataframe`.

        Inspects the call stack to find the calling function, skipping internal
        methods like `_to_dataframe` itself. The function name is automatically
        detected from the call stack.

        Returns:
            Function name if found, None otherwise.
        """
        stack = inspect.stack()
        # Skip frames:
        # 0: _get_calling_function_name (this method)
        # 1: _to_dataframe (the method that called this)
        # 2: The actual calling method (what we want)
        if len(stack) > 2:
            frame_info = stack[2]
            func_name = frame_info.function
            # Return the function name (could be sync like 'list_aggregates' or async like 'alist_aggregates')
            # Skip only if it's a module-level call or internal method
            if func_name != "<module>" and not func_name.startswith("__"):
                return func_name
        return None

    @staticmethod
    def _normalize_function_name(function_name: str) -> str:
        """
        Normalize function name for column rename lookup.

        Removes 'a' prefix from async function names so that sync and async
        methods can share the same column rename configuration.

        Args:
            function_name: Function name (e.g., 'list_aggregates' or 'alist_aggregates').

        Returns:
            Normalized function name (e.g., 'list_aggregates').

        Examples:
            >>> BaseAccess._normalize_function_name('alist_aggregates')
            'list_aggregates'
            >>> BaseAccess._normalize_function_name('list_aggregates')
            'list_aggregates'
        """
        if function_name.startswith("a") and len(function_name) > 1:
            # Check if it's likely an async function (a + verb pattern)
            # Common patterns: alist, aget, asearch, etc.
            return function_name[1:]
        return function_name

    def _apply_column_renames(
        self,
        df: pd.DataFrame,
        function_name: str,
    ) -> pd.DataFrame:
        """
        Apply column renames for a specific function.

        Looks up column rename mappings in `_column_renames` class attribute.
        Both sync and async methods use the same mapping (async names are normalized).

        Args:
            df: DataFrame to rename columns in.
            function_name: Function name (sync or async).

        Returns:
            DataFrame with renamed columns (if mappings exist).
        """
        # Normalize function name (remove 'a' prefix for async methods)
        normalized_name = self._normalize_function_name(function_name)

        # Get column rename mapping for this function
        rename_map = self._column_renames.get(normalized_name, {})

        if not rename_map:
            return df

        # Apply renames
        df = df.copy()
        df = df.rename(columns=rename_map)

        return df

    @staticmethod
    def _camel_to_snake(name: str) -> str:
        """
        Convert camelCase to snake_case.

        Args:
            name: Column name in camelCase.

        Returns:
            Column name in snake_case.
        """
        import re

        # Insert underscore before uppercase letters (except at the start)
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        # Insert underscore before uppercase letters that follow lowercase
        s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
        return s2.lower()

    @staticmethod
    def _infer_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        """
        Infer and convert data types for DataFrame columns.

        Args:
            df: DataFrame with potentially incorrect types.

        Returns:
            DataFrame with proper data types.
        """
        df = df.copy()

        for col in df.columns:
            # Skip if already has proper type
            if df[col].dtype == "object":
                # Try to convert to numeric
                try:
                    # Try integer first
                    pd.to_numeric(df[col], errors="raise")
                    # If successful, check if it's integer or float
                    if df[col].dropna().apply(lambda x: isinstance(x, (int, float)) and float(x).is_integer()).all():
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                    else:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                except (ValueError, TypeError):
                    # Try boolean
                    if df[col].dtype == "object":
                        bool_values = {"true": True, "false": False, "True": True, "False": False}
                        if df[col].dropna().isin(bool_values.keys()).all():
                            df[col] = df[col].map(bool_values)
                        # Otherwise keep as string/object

        return df

    def _normalize_nested_data(
        self,
        data: list[dict[str, Any]],
        nested_key: str = "values",
        parent_keys: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Normalize nested data structures by flattening nested arrays.

        Args:
            data: List of dictionaries, each potentially containing nested arrays.
            nested_key: Key name for the nested array (e.g., 'values').
            parent_keys: List of keys from parent to include in each flattened row.

        Returns:
            List of flattened dictionaries.
        """
        normalized = []
        parent_keys = parent_keys or []

        for item in data:
            # Extract parent fields
            parent_data = {k: v for k, v in item.items() if k != nested_key}

            # Get nested array
            nested_array = item.get(nested_key, [])

            if not nested_array:
                # If no nested data, include parent row as-is
                normalized.append(parent_data)
            else:
                # Flatten: create one row per nested item
                for nested_item in nested_array:
                    row = parent_data.copy()
                    row.update(nested_item)
                    normalized.append(row)

        return normalized
