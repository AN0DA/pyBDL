from types import SimpleNamespace

import pybdl.access as access
import pybdl.api as api
from pybdl.config import BDLConfig


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

        # Initialize API namespace (for raw API access)
        self.api = SimpleNamespace()
        self.api.aggregates = api.AggregatesAPI(self.config)
        self.api.attributes = api.AttributesAPI(self.config)
        self.api.data = api.DataAPI(self.config)
        self.api.levels = api.LevelsAPI(self.config)
        self.api.measures = api.MeasuresAPI(self.config)
        self.api.subjects = api.SubjectsAPI(self.config)
        self.api.units = api.UnitsAPI(self.config)
        self.api.variables = api.VariablesAPI(self.config)
        self.api.version = api.VersionAPI(self.config)
        self.api.years = api.YearsAPI(self.config)

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
