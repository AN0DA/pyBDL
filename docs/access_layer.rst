Access Layer
=============

The access layer is the **primary user-facing interface** of pyLDB. It provides a clean, pandas DataFrame-based API that automatically handles data conversion and normalization.

Overview
--------

The access layer sits on top of the raw API clients and provides:

- **Automatic DataFrame conversion**: All responses are converted to pandas DataFrames
- **Column name normalization**: camelCase API fields are converted to snake_case
- **Data type inference**: Proper types (integers, floats, booleans) are automatically detected
- **Nested data normalization**: Complex nested structures are flattened into tabular format

The main client provides two interfaces:

- **Access layer** (default): Returns pandas DataFrames - use `ldb.levels`, `ldb.data`, etc.
- **API layer**: Returns raw dictionaries - use `ldb.api.levels`, `ldb.api.data`, etc.

For most users, the access layer is recommended as it provides a more Pythonic and data-analysis-friendly interface.

Quick Start
-----------

.. code-block:: python

    from pyldb import LDB, LDBConfig

    # Initialize client
    ldb = LDB(LDBConfig(api_key="your-api-key"))

    # Access layer returns DataFrames
    levels_df = ldb.levels.list_levels()
    print(levels_df.head())

    # Data is ready for analysis
    print(levels_df.dtypes)
    print(levels_df.columns)

Key Features
------------

DataFrame Conversion
~~~~~~~~~~~~~~~~~~~~

All access layer methods return pandas DataFrames, making data immediately ready for analysis:

.. code-block:: python

    # Get variables as DataFrame
    variables_df = ldb.variables.list_variables()
    
    # Use pandas operations directly
    filtered = variables_df[variables_df['name'].str.contains('population', case=False)]
    sorted_vars = variables_df.sort_values('name')

Column Name Normalization
~~~~~~~~~~~~~~~~~~~~~~~~~

API responses use camelCase (e.g., ``variableId``, ``unitName``), but the access layer converts these to snake_case (e.g., ``variable_id``, ``unit_name``) for Pythonic access:

.. code-block:: python

    df = ldb.variables.get_variable("3643")
    # Columns are: variable_id, name, description (not variableId, Name, Description)
    print(df.columns)

Data Type Inference
~~~~~~~~~~~~~~~~~~~

The access layer automatically infers and converts data types:

.. code-block:: python

    df = ldb.data.get_data_by_variable("3643", years=[2021])
    # year column is Int64, val column is float
    print(df.dtypes)

Nested Data Normalization
~~~~~~~~~~~~~~~~~~~~~~~~~

The data endpoints return nested structures. The access layer automatically flattens them:

.. code-block:: python

    # API returns: [{"id": "1", "name": "Warsaw", "values": [{"year": 2021, "val": 1000}, ...]}]
    # Access layer returns flat DataFrame:
    df = ldb.data.get_data_by_variable("3643", years=[2021])
    # Columns: unit_id, unit_name, year, val, attr_id
    print(df.head())


Available Endpoints
-------------------

The access layer provides endpoints for all LDB API resources:

.. list-table:: Available Access Endpoints
   :header-rows: 1

   * - Endpoint
     - Access Method
     - Description
   * - Aggregates
     - ``ldb.aggregates``
     - Aggregation level metadata
   * - Attributes
     - ``ldb.attributes``
     - Attribute metadata
   * - Data
     - ``ldb.data``
     - Statistical data access
   * - Levels
     - ``ldb.levels``
     - Administrative unit levels
   * - Measures
     - ``ldb.measures``
     - Measure unit metadata
   * - Subjects
     - ``ldb.subjects``
     - Subject hierarchy
   * - Units
     - ``ldb.units``
     - Administrative units
   * - Variables
     - ``ldb.variables``
     - Variable metadata
   * - Years
     - ``ldb.years``
     - Available years

Endpoint Details
----------------

Levels
~~~~~~

Administrative unit aggregation levels (country, voivodeship, county, municipality):

.. code-block:: python

    # List all levels
    levels_df = ldb.levels.list_levels()
    
    # Get specific level
    level_df = ldb.levels.get_level(1)  # Level 1 = country
    
    # Get metadata
    metadata_df = ldb.levels.get_levels_metadata()

Subjects
~~~~~~~~

Subject categories and hierarchy:

.. code-block:: python

    # List all top-level subjects
    subjects_df = ldb.subjects.list_subjects()
    
    # Get subjects under a parent
    child_subjects = ldb.subjects.list_subjects(parent_id="P0001")
    
    # Search subjects
    results = ldb.subjects.search_subjects(name="population")
    
    # Get specific subject
    subject_df = ldb.subjects.get_subject("P0001")

Variables
~~~~~~~~~

Statistical variables (indicators):

.. code-block:: python

    # List all variables
    variables_df = ldb.variables.list_variables()
    
    # Filter variables
    filtered = ldb.variables.list_variables(
        category_id="P0001",
        name="population"
    )
    
    # Search variables
    results = ldb.variables.search_variables(name="unemployment")
    
    # Get specific variable
    variable_df = ldb.variables.get_variable("3643")

Data
~~~~

Statistical data retrieval:

.. code-block:: python

    # Get data by variable (most common)
    df = ldb.data.get_data_by_variable(
        variable_id="3643",
        years=[2021],
        unit_level=2  # Voivodeship level
    )
    
    # Get data for multiple years
    df = ldb.data.get_data_by_variable(
        variable_id="3643",
        years=[2020, 2021, 2022],
        unit_level=2
    )
    
    # Get data with aggregate filter
    df = ldb.data.get_data_by_variable(
        variable_id="3643",
        years=[2021],
        aggregate_id=1
    )
    
    # Get data by administrative unit
    df = ldb.data.get_data_by_unit(
        unit_id="020000000000",
        variable_ids=["3643"],
        years=[2021]
    )
    
    # Get data for a locality
    df = ldb.data.get_data_by_variable_locality(
        variable_id="3643",
        unit_parent_id="1465011",
        years=[2021]
    )
    
    # Get data by unit locality
    df = ldb.data.get_data_by_unit_locality(
        unit_id="1465011",
        variable_id="3643",
        years=[2021]
    )

The data endpoints automatically normalize nested ``values`` arrays into flat rows.

Units
~~~~~

Administrative units (regions, cities, etc.):

.. code-block:: python

    # List units by level
    voivodeships = ldb.units.list_units(level=2)  # Level 2 = voivodeship
    
    # Search units
    warsaw = ldb.units.search_units(name="Warsaw")
    
    # Get specific unit
    unit_df = ldb.units.get_unit("020000000000")
    
    # List localities (statistical localities)
    localities = ldb.units.list_localities(level=6)  # Level 6 = municipality
    
    # Search localities
    warsaw_localities = ldb.units.search_localities(name="Warsaw", level=6)
    
    # Get specific locality
    locality_df = ldb.units.get_locality("1465011")

Attributes
~~~~~~~~~~

Data attributes (dimensions):

.. code-block:: python

    # List all attributes
    attributes_df = ldb.attributes.list_attributes()
    
    # Get specific attribute
    attr_df = ldb.attributes.get_attribute("1")

Measures
~~~~~~~~

Measure units:

.. code-block:: python

    # List all measures
    measures_df = ldb.measures.list_measures()
    
    # Get specific measure
    measure_df = ldb.measures.get_measure(1)

Aggregates
~~~~~~~~~~

Aggregation types:

.. code-block:: python

    # List all aggregates
    aggregates_df = ldb.aggregates.list_aggregates()
    
    # Get specific aggregate
    aggregate_df = ldb.aggregates.get_aggregate("1")

Years
~~~~~

Available years for data:

.. code-block:: python

    # List all available years
    years_df = ldb.years.list_years()
    
    # Get specific year metadata
    year_df = ldb.years.get_year(2021)


Pagination
----------

Most list methods support pagination:

.. code-block:: python

    # Fetch all pages (default, max_pages=None)
    all_data = ldb.variables.list_variables()
    
    # Fetch only first page
    first_page = ldb.variables.list_variables(max_pages=1, page_size=50)
    
    # Limit number of pages
    limited = ldb.variables.list_variables(max_pages=5, page_size=100)

Parameters:

- ``max_pages``: Maximum number of pages to fetch. ``None`` (default) fetches all pages, ``1`` fetches only the first page, ``N`` fetches up to N pages
- ``page_size``: Number of results per page (default: 100 from config or 100)

Async Usage
-----------

All access layer methods have async versions (prefixed with ``a``):

.. code-block:: python

    import asyncio
    from pyldb import LDB

    async def main():
        ldb = LDB()
        
        # Async methods return DataFrames
        levels_df = await ldb.levels.alist_levels()
        variables_df = await ldb.variables.alist_variables()
        
        # Can run multiple requests concurrently
        levels_task = ldb.levels.alist_levels()
        variables_task = ldb.variables.alist_variables()
        
        levels_df, variables_df = await asyncio.gather(levels_task, variables_task)
        
        return levels_df, variables_df

    asyncio.run(main())

Available async methods:

- ``alist_levels()``, ``alist_variables()``, ``alist_subjects()``, etc.
- ``aget_level()``, ``aget_variable()``, ``aget_subject()``, etc.
- ``aget_data_by_variable()``, ``aget_data_by_unit()``, etc.

Examples
--------

Basic Usage
~~~~~~~~~~~

.. code-block:: python

    from pyldb import LDB, LDBConfig

    ldb = LDB(LDBConfig(api_key="your-api-key"))

    # Get administrative levels
    levels = ldb.levels.list_levels()
    print(f"Found {len(levels)} administrative levels")

    # Get variables related to population
    population_vars = ldb.variables.search_variables(name="population")
    print(f"Found {len(population_vars)} population-related variables")

    # Get data for a specific variable
    data = ldb.data.get_data_by_variable(
        variable_id="3643",
        years=[2021],
        unit_level=2  # Voivodeship level
    )
    print(data.head())

Filtering and Analysis
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Get all variables
    variables = ldb.variables.list_variables()
    
    # Filter using pandas
    economic_vars = variables[variables['name'].str.contains('economic', case=False)]
    
    # Get data for multiple variables
    for var_id in economic_vars['id'].head(5):
        data = ldb.data.get_data_by_variable(var_id, years=[2021])
        print(f"Variable {var_id}: {len(data)} records")

Getting Data
~~~~~~~~~~~~

.. code-block:: python

    # Get data
    df = ldb.data.get_data_by_variable("3643", years=[2021])
    
    # DataFrame includes IDs and values
    print(df[['unit_name', 'attr_name', 'val']].head())
    
    # Group by attribute name
    by_attr = df.groupby('attr_name')['val'].mean()
    print(by_attr)

Working with Nested Data
~~~~~~~~~~~~~~~~~~~~~~~~~

The data endpoints automatically normalize nested structures:

.. code-block:: python

    # API returns nested structure, but access layer flattens it
    df = ldb.data.get_data_by_variable("3643", years=[2021])
    
    # Each row represents one data point
    # Columns: unit_id, unit_name, year, val, attr_id, attr_name
    print(df.head())
    
    # Easy to analyze
    avg_by_unit = df.groupby('unit_name')['val'].mean()
    print(avg_by_unit)
    
    # Get data for multiple years
    multi_year_df = ldb.data.get_data_by_variable("3643", years=[2020, 2021, 2022])
    # Analyze trends over time
    yearly_avg = multi_year_df.groupby('year')['val'].mean()
    print(yearly_avg)

See :doc:`examples` for more comprehensive real-world examples.

API Reference
-------------

.. automodule:: pyldb.access
    :members:
    :undoc-members:
    :show-inheritance:

.. seealso::
   - :doc:`main_client` for main client usage
   - :doc:`api_clients` for low-level API access
   - :doc:`examples` for real-world examples
   - :doc:`config` for configuration options

