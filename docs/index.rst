pyLDB documentation
===================

What is the Local Data Bank (LDB)?
----------------------------------

The Local Data Bank (LDB, Bank Danych Lokalnych) is Poland's official statistical data warehouse, maintained by Statistics Poland (GUS). It provides access to a vast range of statistical indicators and datasets covering:

- Demographics and population
- Economy and labor market
- Education, health, and social welfare
- Environment and infrastructure
- Regional and local statistics (down to municipality level)
- Historical time series and more

Data is available for various administrative units (country, voivodeship, county, municipality) and can be filtered by year, subject, and other attributes. The LDB is a primary source for open, official statistics in Poland.

For a full description of available data, endpoints, and API usage, see:

- Official BDL API documentation: https://api.stat.gov.pl/Home/BdlApi
- LDB web portal: https://bdl.stat.gov.pl/bdl/start

pyLDB is a modern, Pythonic client library for the Local Data Bank (LDB, Bank Danych Lokalnych) API,
enabling easy, robust access to Polish official statistics for data science, research,
and applications.

Features
--------

- Clean, modular API client for all LDB endpoints
- Pandas DataFrame integration for tabular data
- Full support for pagination, filtering, and internationalization
- Built-in API key, language, and cache configuration
- Open source, tested, and ready for data analysis and visualization

Quick Start
-----------

.. code-block:: python

    from pyldb import LDB, LDBConfig
    
    # Initialize client
    ldb = LDB(LDBConfig(api_key="your-api-key"))  # Reads config from environment or defaults
    
    # Use the access layer (returns pandas DataFrames)
    df = ldb.data.get_data_by_variable(variable_id="3643", years=[2021])
    print(df.head())
    
    # Data is ready for analysis
    print(df.dtypes)
    print(df.columns)

Configuration
-------------

Configure your API key and options via environment variables or directly:

.. code-block:: python

    from pyldb import LDBConfig
    config = LDBConfig(api_key="your-api-key", language="en", use_cache=True)
    ldb = LDB(config=config)

Or set environment variables::

    export LDB_API_KEY=your-api-key
    export LDB_LANGUAGE=en

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

pyLDB is open source under the MIT license. Contributions and issues are welcome!
For details, see the `GitHub repository <https://github.com/AN0DA/pyldb>`_.

