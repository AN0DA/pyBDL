from dataclasses import dataclass
from typing import Any

import pybdl.access as access
import pybdl.api as api
from pybdl.config import BDLConfig


@dataclass
class APINamespace:
    """Typed namespace for low-level API clients."""

    aggregates: Any
    attributes: Any
    data: Any
    levels: Any
    measures: Any
    subjects: Any
    units: Any
    variables: Any
    version: Any
    years: Any


class BDL:
    """
    Main interface for interacting with the Local Data Bank (BDL) API.

    This class provides a unified entry point to all BDL API endpoints, including aggregates,
    attributes, data, levels, measures, subjects, units, variables, version, and years.

    The access layer (default interface) returns pandas DataFrames with proper column labels
    and data types. The API layer (via .api) returns raw dictionaries for advanced use cases.
    """

    def __init__(self, config: BDLConfig | dict | None = None):
        """
        Initialize the BDL client and all API endpoint namespaces.

        Args:
            config: BDLConfig instance or dict. If not provided, configuration is loaded from
                environment variables and defaults.

        Raises:
            TypeError: If config is not a dict, BDLConfig, or None.

        Note:
            Configuration can be set through:
            1. Direct parameter passing to BDLConfig
            2. Environment variables (BDL_API_KEY, BDL_LANGUAGE, etc.)
            3. Default values
        """
        if config is None:
            config_obj = BDLConfig()
        elif isinstance(config, dict):
            config_obj = BDLConfig(**config)
        elif isinstance(config, BDLConfig):
            config_obj = config
        else:
            raise TypeError(f"config must be a dict, BDLConfig, or None, got {type(config)}")
        self.config = config_obj

        self.api = APINamespace(
            aggregates=api.AggregatesAPI(self.config),
            attributes=api.AttributesAPI(self.config),
            data=api.DataAPI(self.config),
            levels=api.LevelsAPI(self.config),
            measures=api.MeasuresAPI(self.config),
            subjects=api.SubjectsAPI(self.config),
            units=api.UnitsAPI(self.config),
            variables=api.VariablesAPI(self.config),
            version=api.VersionAPI(self.config),
            years=api.YearsAPI(self.config),
        )

        # Initialize access layer (default interface, returns DataFrames)
        self.aggregates = access.AggregatesAccess(self.api.aggregates)
        self.attributes = access.AttributesAccess(self.api.attributes)
        self.data = access.DataAccess(self.api.data)
        self.levels = access.LevelsAccess(self.api.levels)
        self.measures = access.MeasuresAccess(self.api.measures)
        self.subjects = access.SubjectsAccess(self.api.subjects)
        self.units = access.UnitsAccess(self.api.units)
        self.variables = access.VariablesAccess(self.api.variables)
        self.years = access.YearsAccess(self.api.years)

    def close(self) -> None:
        """Close all synchronous HTTP resources owned by the client."""
        for client in vars(self.api).values():
            close = getattr(client, "close", None)
            if callable(close):
                close()

    async def aclose(self) -> None:
        """Close all synchronous and asynchronous HTTP resources owned by the client."""
        for client in vars(self.api).values():
            aclose = getattr(client, "aclose", None)
            if callable(aclose):
                await aclose()
            else:
                close = getattr(client, "close", None)
                if callable(close):
                    close()

    def __enter__(self) -> "BDL":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self.close()
        return False

    async def __aenter__(self) -> "BDL":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        await self.aclose()
        return False
