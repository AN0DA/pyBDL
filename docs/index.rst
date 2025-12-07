pyBDL documentation
===================

What is the Local Data Bank (BDL)?
----------------------------------

The Local Data Bank (BDL, Bank Danych Lokalnych) is Poland's official statistical data warehouse, maintained by Statistics Poland (GUS). It provides access to a vast range of statistical indicators and datasets covering:

- Demographics and population
- Economy and labor market
- Education, health, and social welfare
- Environment and infrastructure
- Regional and local statistics (down to municipality level)
- Historical time series and more

Data is available for various administrative units (country, voivodeship, county, municipality) and can be filtered by year, subject, and other attributes. The BDL is a primary source for open, official statistics in Poland.

For a full description of available data, endpoints, and API usage, see:

- Official BDL API documentation: https://api.stat.gov.pl/Home/BdlApi
- BDL web portal: https://bdl.stat.gov.pl/bdl/start

pyBDL is a modern, Pythonic client library for the Local Data Bank (BDL, Bank Danych Lokalnych) API,
enabling easy, robust access to Polish official statistics for data science, research,
and applications.

Features
--------

- Clean, modular API client for all BDL endpoints
- Pandas DataFrame integration for tabular data
- Full support for pagination, filtering, and internationalization
- Built-in API key, language, and cache configuration
- Open source, tested, and ready for data analysis and visualization

Quick Start
-----------

.. code-block:: python

    from pybdl import BDL, BDLConfig
    
    # Initialize client
    bdl = BDL(BDLConfig(api_key="your-api-key"))  # Reads config from environment or defaults
    
    # Use the access layer (returns pandas DataFrames)
    df = bdl.data.get_data_by_variable(variable_id="3643", years=[2021])
    print(df.head())
    
    # Data is ready for analysis
    print(df.dtypes)
    print(df.columns)

Configuration
-------------

Configure your API key and options via environment variables or directly:

.. code-block:: python

    from pybdl import BDLConfig
    config = BDLConfig(api_key="your-api-key", language="en", use_cache=True)
    bdl = BDL(config=config)

Or set environment variables::

    export BDL_API_KEY=your-api-key
    export BDL_LANGUAGE=en

Documentation
-------------

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   main_client
   access_layer
   examples
   api_clients
   config
   rate_limiting

.. toctree::
   :maxdepth: 2
   :caption: Reference

   appendix

Contributing & License
----------------------

pyBDL is open source under the MIT license. Contributions and issues are welcome!
For details, see the `GitHub repository <https://github.com/AN0DA/pybdl>`_.

