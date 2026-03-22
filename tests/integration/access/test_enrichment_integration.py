"""Integration tests for enrichment decorator across access layers."""

from collections.abc import Callable
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pybdl.access.data import DataAccess
from pybdl.access.variables import VariablesAccess


@pytest.mark.integration
class TestEnrichmentIntegration:
    """Integration coverage for enrichment merges using sample payloads."""

    def test_variables_enrichment(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """VariablesAccess enriches level, measure, and subject details."""
        samples = load_sample_data("samples_raw_variables.json")
        levels = load_sample_data("samples_raw_levels.json")
        measures = load_sample_data("samples_raw_measures.json")
        subjects = load_sample_data("samples_raw_subjects.json")

        mock_api_client.list_variables.return_value = samples["list_variables"]
        mock_api_client.list_levels.return_value = levels["list_levels"]
        mock_api_client.list_measures.return_value = measures["list_measures"]
        mock_api_client.list_subjects.return_value = subjects["list_subjects"]

        access = VariablesAccess(mock_api_client)
        df = access.list_variables(enrich_levels=True, enrich_measures=True, enrich_subjects=True)

        assert isinstance(df, pd.DataFrame)
        assert "level_name" in df.columns
        assert "measure_unit_description" in df.columns
        assert "subject_name" in df.columns
        assert df["level_name"].notna().any()
        assert df["measure_unit_description"].notna().any()
        assert df["subject_name"].notna().any()

    def test_data_enrichment(
        self,
        mock_api_client: MagicMock,
        load_sample_data: Callable[[str], dict[str, Any]],
    ) -> None:
        """DataAccess enriches units and attributes; metadata tuple is preserved."""
        data_samples = load_sample_data("samples_raw_data.json")
        units = load_sample_data("samples_raw_units.json")
        attributes = load_sample_data("samples_raw_attributes.json")

        mock_api_client.get_data_by_variable.return_value = data_samples["get_data_by_variable"]
        mock_api_client.list_units.return_value = units["list_units"]
        mock_api_client.list_attributes.return_value = attributes["list_attributes"]

        access = DataAccess(mock_api_client)
        df, metadata = access.get_data_by_variable(
            "3643",
            enrich_units=True,
            enrich_attributes=True,
            return_metadata=True,
        )

        assert isinstance(df, pd.DataFrame)
        assert isinstance(metadata, dict)
        assert "unit_level" in df.columns
        assert "unit_parent_id" in df.columns
        assert "attr_description" in df.columns
        assert df["attr_description"].notna().any()
