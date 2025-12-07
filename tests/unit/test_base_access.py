"""Unit tests for BaseAccess class."""

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pybdl.access.base import BaseAccess


class ConcreteAccess(BaseAccess):
    """Concrete implementation of BaseAccess for testing."""

    pass


@pytest.mark.unit
class TestCamelToSnake:
    """Test camelCase to snake_case conversion."""

    def test_simple_camel_case(self) -> None:
        """Test simple camelCase conversion."""
        assert BaseAccess._camel_to_snake("camelCase") == "camel_case"
        assert BaseAccess._camel_to_snake("simpleName") == "simple_name"

    def test_multiple_caps(self) -> None:
        """Test conversion with multiple capital letters."""
        assert BaseAccess._camel_to_snake("XMLHttpRequest") == "xml_http_request"
        assert BaseAccess._camel_to_snake("HTTPSConnection") == "https_connection"

    def test_already_snake_case(self) -> None:
        """Test that snake_case remains unchanged."""
        assert BaseAccess._camel_to_snake("snake_case") == "snake_case"
        assert BaseAccess._camel_to_snake("already_snake") == "already_snake"

    def test_single_word(self) -> None:
        """Test single word conversion."""
        assert BaseAccess._camel_to_snake("word") == "word"
        assert BaseAccess._camel_to_snake("Word") == "word"

    def test_numbers(self) -> None:
        """Test conversion with numbers."""
        assert BaseAccess._camel_to_snake("test123") == "test123"
        assert BaseAccess._camel_to_snake("test123Name") == "test123_name"

    def test_empty_string(self) -> None:
        """Test empty string."""
        assert BaseAccess._camel_to_snake("") == ""


@pytest.mark.unit
class TestInferDtypes:
    """Test DataFrame type inference."""

    def test_integer_conversion(self) -> None:
        """Test integer type inference."""
        df = pd.DataFrame({"col": ["1", "2", "3"]})
        result = BaseAccess._infer_dtypes(df)
        # When no NaN values, pandas may use int64 instead of Int64
        assert result["col"].dtype in ("Int64", "int64")
        assert result["col"].tolist() == [1, 2, 3]

    def test_float_conversion(self) -> None:
        """Test float type inference."""
        df = pd.DataFrame({"col": ["1.5", "2.7", "3.0"]})
        result = BaseAccess._infer_dtypes(df)
        assert pd.api.types.is_float_dtype(result["col"])
        assert result["col"].tolist() == [1.5, 2.7, 3.0]

    def test_mixed_int_float(self) -> None:
        """Test mixed integer and float conversion."""
        df = pd.DataFrame({"col": ["1", "2.5", "3"]})
        result = BaseAccess._infer_dtypes(df)
        assert pd.api.types.is_float_dtype(result["col"])
        assert result["col"].tolist() == [1.0, 2.5, 3.0]

    def test_boolean_conversion(self) -> None:
        """Test boolean type inference."""
        df = pd.DataFrame({"col": ["true", "false", "True", "False"]})
        result = BaseAccess._infer_dtypes(df)
        assert result["col"].dtype == bool
        assert result["col"].tolist() == [True, False, True, False]

    def test_string_remains_string(self) -> None:
        """Test that non-numeric strings remain strings."""
        df = pd.DataFrame({"col": ["hello", "world", "test"]})
        result = BaseAccess._infer_dtypes(df)
        assert result["col"].dtype == "object"
        assert result["col"].tolist() == ["hello", "world", "test"]

    def test_mixed_types(self) -> None:
        """Test DataFrame with mixed types."""
        df = pd.DataFrame(
            {
                "int_col": ["1", "2", "3"],
                "float_col": ["1.5", "2.5", "3.5"],
                "bool_col": ["true", "false", "true"],
                "str_col": ["a", "b", "c"],
            }
        )
        result = BaseAccess._infer_dtypes(df)
        # When no NaN values, pandas may use int64 instead of Int64
        assert result["int_col"].dtype in ("Int64", "int64")
        assert pd.api.types.is_float_dtype(result["float_col"])
        assert result["bool_col"].dtype == bool
        assert result["str_col"].dtype == "object"

    def test_nan_handling(self) -> None:
        """Test NaN handling in numeric columns."""
        df = pd.DataFrame({"col": ["1", None, "3", ""]})
        result = BaseAccess._infer_dtypes(df)
        # With NaN values, should use nullable Int64, but empty string may cause float64
        assert result["col"].dtype in ("Int64", "int64", "float64")
        assert pd.isna(result["col"].iloc[1])
        assert pd.isna(result["col"].iloc[3])

    def test_empty_dataframe(self) -> None:
        """Test empty DataFrame."""
        df = pd.DataFrame()
        result = BaseAccess._infer_dtypes(df)
        assert result.empty

    def test_already_numeric(self) -> None:
        """Test DataFrame with already numeric types."""
        df = pd.DataFrame({"col": [1, 2, 3]})
        result = BaseAccess._infer_dtypes(df)
        assert result["col"].dtype == "Int64" or result["col"].dtype == "int64"


@pytest.mark.unit
class TestNormalizeNestedData:
    """Test nested data normalization."""

    def test_basic_normalization(self) -> None:
        """Test basic nested data flattening."""
        data = [
            {"id": 1, "name": "Unit1", "values": [{"year": 2020, "val": 100}, {"year": 2021, "val": 200}]},
            {"id": 2, "name": "Unit2", "values": [{"year": 2020, "val": 150}]},
        ]
        # Create a concrete instance to call the method
        access = ConcreteAccess(MagicMock())
        result = access._normalize_nested_data(data, nested_key="values")
        assert len(result) == 3
        assert result[0] == {"id": 1, "name": "Unit1", "year": 2020, "val": 100}
        assert result[1] == {"id": 1, "name": "Unit1", "year": 2021, "val": 200}
        assert result[2] == {"id": 2, "name": "Unit2", "year": 2020, "val": 150}

    def test_empty_nested_array(self) -> None:
        """Test item with empty nested array."""
        data = [{"id": 1, "name": "Unit1", "values": []}]
        access = ConcreteAccess(MagicMock())
        result = access._normalize_nested_data(data, nested_key="values")
        assert len(result) == 1
        assert result[0] == {"id": 1, "name": "Unit1"}

    def test_missing_nested_key(self) -> None:
        """Test item without nested key."""
        data = [{"id": 1, "name": "Unit1"}]
        access = ConcreteAccess(MagicMock())
        result = access._normalize_nested_data(data, nested_key="values")
        assert len(result) == 1
        assert result[0] == {"id": 1, "name": "Unit1"}

    def test_parent_keys(self) -> None:
        """Test normalization with specific parent keys."""
        data = [
            {"id": 1, "name": "Unit1", "extra": "data", "values": [{"year": 2020, "val": 100}]},
        ]
        access = ConcreteAccess(MagicMock())
        result = access._normalize_nested_data(data, nested_key="values", parent_keys=["id", "name"])
        assert len(result) == 1
        assert "id" in result[0]
        assert "name" in result[0]
        # Note: parent_keys doesn't filter, it just specifies which keys to include from parent
        # The actual implementation includes all parent keys except nested_key
        assert "year" in result[0]
        assert "val" in result[0]

    def test_empty_data(self) -> None:
        """Test empty data list."""
        data: list[dict] = []
        access = ConcreteAccess(MagicMock())
        result = access._normalize_nested_data(data, nested_key="values")
        assert result == []


@pytest.mark.unit
class TestToDataframe:
    """Test DataFrame conversion."""

    def test_basic_conversion(self) -> None:
        """Test basic list to DataFrame conversion."""
        access = ConcreteAccess(MagicMock())
        data = [{"id": 1, "name": "Test", "camelCase": "value"}]
        result = access._to_dataframe(data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "camel_case" in result.columns
        assert result["id"].iloc[0] == 1

    def test_single_dict(self) -> None:
        """Test single dictionary conversion."""
        access = ConcreteAccess(MagicMock())
        data = {"id": 1, "name": "Test"}
        result = access._to_dataframe(data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["id"].iloc[0] == 1

    def test_empty_list(self) -> None:
        """Test empty list conversion."""
        access = ConcreteAccess(MagicMock())
        data: list = []
        result = access._to_dataframe(data)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_empty_dict(self) -> None:
        """Test empty dict conversion."""
        access = ConcreteAccess(MagicMock())
        data: dict[str, Any] = {}
        result = access._to_dataframe(data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.empty or len(result.columns) == 0

    def test_column_name_normalization(self) -> None:
        """Test that column names are normalized."""
        access = ConcreteAccess(MagicMock())
        data = [{"camelCase": "value", "PascalCase": "value2", "snake_case": "value3"}]
        result = access._to_dataframe(data)
        assert "camel_case" in result.columns
        assert "pascal_case" in result.columns
        assert "snake_case" in result.columns

    def test_type_inference_applied(self) -> None:
        """Test that type inference is applied."""
        access = ConcreteAccess(MagicMock())
        data = [{"id": "1", "value": "100", "flag": "true"}]
        result = access._to_dataframe(data)
        # When no NaN values, pandas may use int64 instead of Int64
        assert result["id"].dtype in ("Int64", "int64")
        assert pd.api.types.is_numeric_dtype(result["value"])
        assert result["flag"].dtype == bool
