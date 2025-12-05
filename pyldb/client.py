from types import SimpleNamespace

import pyldb.api as api
from pyldb.config import LDBConfig


class LDB:
    """
    Main interface for interacting with the Local Data Bank (LDB) API.

    This class provides a unified entry point to all LDB API endpoints, including aggregates,
    attributes, data, levels, measures, subjects, units, variables, version, and years.
    """

    def __init__(self, config: LDBConfig | dict | None = None):
        """
        Initialize the LDB client and all API endpoint namespaces.

        Args:
            config: LDBConfig instance or dict. If not provided, configuration is loaded from
                environment variables and defaults.

        Raises:
            TypeError: If config is not a dict, LDBConfig, or None.

        Note:
            Configuration can be set through:
            1. Direct parameter passing to LDBConfig
            2. Environment variables (LDB_API_KEY, LDB_LANGUAGE, etc.)
            3. Default values
        """
        if config is None:
            config_obj = LDBConfig()
        elif isinstance(config, dict):
            config_obj = LDBConfig(**config)
        elif isinstance(config, LDBConfig):
            config_obj = config
        else:
            raise TypeError(f"config must be a dict, LDBConfig, or None, got {type(config)}")
        self.config = config_obj

        # Initialize API namespace first
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
