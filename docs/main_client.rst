Main Client
===========

.. automodule:: pyldb.client
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:
    :noindex:

The :class:`pyldb.client.LDB` class is the main entry point for the library. It provides **two interfaces** for accessing LDB data:

1. **Access Layer** (default, recommended): Returns pandas DataFrames - use ``ldb.levels``, ``ldb.data``, etc.
2. **API Layer**: Returns raw dictionaries - use ``ldb.api.levels``, ``ldb.api.data``, etc.

For most users, the **access layer is recommended** as it provides DataFrames that are immediately ready for data analysis.

Access Layer (Default Interface)
---------------------------------

The access layer is the primary interface and returns pandas DataFrames:

.. code-block:: python

    from pyldb import LDB, LDBConfig
    
    ldb = LDB(LDBConfig(api_key="your-api-key"))
    
    # Access layer - returns DataFrames
    levels_df = ldb.levels.list_levels()
    variables_df = ldb.variables.list_variables()
    data_df = ldb.data.get_data_by_variable(variable_id="3643", years=[2021])
    
    # Data is ready for pandas operations
    print(levels_df.head())
    print(data_df.dtypes)
    print(data_df.columns)

Key features of the access layer:

- **Automatic DataFrame conversion**: All responses are pandas DataFrames
- **Column normalization**: camelCase → snake_case (e.g., ``variableId`` → ``variable_id``)
- **Data type inference**: Proper types (integers, floats, booleans)
- **Nested data flattening**: Complex structures are normalized into tabular format

See :doc:`access_layer` for comprehensive documentation on the access layer.

API Layer (Low-Level Interface)
---------------------------------

The API layer provides direct access to raw API responses as dictionaries:

.. code-block:: python

    from pyldb import LDB
    
    ldb = LDB()
    
    # API layer - returns raw dictionaries
    levels_data = ldb.api.levels.list_levels()
    data_dict = ldb.api.data.get_data_by_variable(variable_id="3643", years=[2021])
    
    # Raw API response structure
    print(type(levels_data))  # list
    print(type(data_dict))    # list or dict

Use the API layer when you need:

- Raw API response structure
- Custom response processing
- Direct access to API metadata
- Integration with non-pandas workflows

See :doc:`api_clients` for details about the API layer.

Examples
--------

Basic Usage with Access Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pyldb import LDB, LDBConfig
    
    # Initialize client
    ldb = LDB(LDBConfig(api_key="your-api-key"))
    
    # Get administrative levels
    levels = ldb.levels.list_levels()
    print(f"Found {len(levels)} administrative levels")
    
    # Get variables
    variables = ldb.variables.search_variables(name="population")
    print(f"Found {len(variables)} population variables")
    
    # Get data
    data = ldb.data.get_data_by_variable("3643", years=[2021], unit_level=2)
    print(f"Retrieved {len(data)} data points")
    print(data.head())

Using Both Interfaces
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pyldb import LDB
    
    ldb = LDB()
    
    # Access layer for DataFrame analysis
    df = ldb.data.get_data_by_variable("3643", years=[2021])
    avg_value = df['val'].mean()
    
    # API layer for raw metadata
    metadata = ldb.api.data.get_data_by_variable(
        "3643", years=[2021], return_metadata=True
    )
    if isinstance(metadata, tuple):
        data, meta = metadata
        print(f"Total pages: {meta.get('totalPages', 'unknown')}")

Async Usage
~~~~~~~~~~~

Both interfaces support async operations:

.. code-block:: python

    import asyncio
    from pyldb import LDB
    
    async def main():
        ldb = LDB()
        
        # Async access layer
        levels_df = await ldb.levels.alist_levels()
        variables_df = await ldb.variables.alist_variables()
        
        # Async API layer
        levels_data = await ldb.api.levels.alist_levels()
        
        return levels_df, variables_df, levels_data
    
    asyncio.run(main())

.. seealso::
   - :doc:`access_layer` for comprehensive access layer documentation
   - :doc:`api_clients` for API layer details
   - :doc:`examples` for real-world usage examples
   - :doc:`config` for configuration options
