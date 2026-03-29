# Access Layer

The access layer is the **primary user-facing interface** of pyBDL. It
provides a clean, pandas DataFrame-based API that automatically handles
data conversion and normalization.

## Overview

The access layer sits on top of the raw API clients and provides:

- **Automatic DataFrame conversion**: All responses are converted to
  pandas DataFrames
- **Column name normalization**: camelCase API fields are converted to
  snake_case
- **Data type inference**: Proper types (integers, floats, booleans) are
  automatically detected
- **Nested data normalization**: Complex nested structures are flattened
  into tabular format

The main client provides two interfaces:

- **Access layer** (default): Returns pandas DataFrames - use
  `bdl.levels`,
  `bdl.data`, etc.
- **API layer**: Returns raw dictionaries - use
  `bdl.api.levels`,
  `bdl.api.data`, etc.

For most users, the access layer is recommended as it provides a more
Pythonic and data-analysis-friendly interface.

## Quick Start

``` python
from pybdl import BDL, BDLConfig

# Initialize client
bdl = BDL(BDLConfig(api_key="your-api-key"))

# Access layer returns DataFrames
levels_df = bdl.levels.list_levels()
print(levels_df.head())

# Data is ready for analysis
print(levels_df.dtypes)
print(levels_df.columns)
```

## Key Features

### DataFrame Conversion

All access layer methods return pandas DataFrames, making data
immediately ready for analysis:

``` python
# Get variables as DataFrame
variables_df = bdl.variables.list_variables()

# Use pandas operations directly
filtered = variables_df[variables_df['name'].str.contains('population', case=False)]
sorted_vars = variables_df.sort_values('name')
```

### Column Name Normalization

API responses use camelCase (e.g., `variableId`, `unitName`), but the
access layer converts these to snake_case (e.g., `variable_id`,
`unit_name`) for Pythonic access:

``` python
df = bdl.variables.get_variable("3643")
# Columns are: variable_id, name, description (not variableId, Name, Description)
print(df.columns)
```

### Data Type Inference

The access layer automatically infers and converts data types:

``` python
df = bdl.data.get_data_by_variable("3643", years=[2021])
# year column is Int64, val column is float
print(df.dtypes)
```

### Nested Data Normalization

The data endpoints return nested structures. The access layer
automatically flattens them:

```python
# API returns: [{"id": "1", "name": "Warsaw",
#   "values": [{"year": 2021, "val": 1000}, ...]}]
# Access layer returns flat DataFrame:
df = bdl.data.get_data_by_variable("3643", years=[2021])
# Columns: unit_id, unit_name, year, val, attr_id
print(df.head())
```

## Available Endpoints

The access layer provides endpoints for all BDL API resources:

| Endpoint   | Access Method    | Description                |
|------------|------------------|----------------------------|
| Aggregates | `bdl.aggregates` | Aggregation level metadata |
| Attributes | `bdl.attributes` | Attribute metadata         |
| Data       | `bdl.data`       | Statistical data access    |
| Levels     | `bdl.levels`     | Administrative unit levels |
| Measures   | `bdl.measures`   | Measure unit metadata      |
| Subjects   | `bdl.subjects`   | Subject hierarchy          |
| Units      | `bdl.units`      | Administrative units       |
| Variables  | `bdl.variables`  | Variable metadata          |
| Years      | `bdl.years`      | Available years            |

Available Access Endpoints

## Endpoint Details

### Levels

Administrative unit aggregation levels (country, voivodeship, county,
municipality):

``` python
# List all levels
levels_df = bdl.levels.list_levels()

# Get specific level
level_df = bdl.levels.get_level(1)  # Level 1 = country

# Get metadata
metadata_df = bdl.levels.get_levels_metadata()
```

### Subjects

Subject categories and hierarchy:

``` python
# List all top-level subjects
subjects_df = bdl.subjects.list_subjects()

# Get subjects under a parent
child_subjects = bdl.subjects.list_subjects(parent_id="P0001")

# Search subjects
results = bdl.subjects.search_subjects(name="population")

# Get specific subject
subject_df = bdl.subjects.get_subject("P0001")
```

### Variables

Statistical variables (indicators):

``` python
# List all variables
variables_df = bdl.variables.list_variables()

# Filter variables
filtered = bdl.variables.list_variables(
    category_id="P0001",
    name="population"
)

# Search variables
results = bdl.variables.search_variables(name="unemployment")

# Get specific variable
variable_df = bdl.variables.get_variable("3643")
```

### Data

Statistical data retrieval:

``` python
# Get data by variable (most common)
df = bdl.data.get_data_by_variable(
    variable_id="3643",
    years=[2021],
    unit_level=2  # Voivodeship level
)

# Get data for multiple years
df = bdl.data.get_data_by_variable(
    variable_id="3643",
    years=[2020, 2021, 2022],
    unit_level=2
)

# Get data with aggregate filter
df = bdl.data.get_data_by_variable(
    variable_id="3643",
    years=[2021],
    aggregate_id=1
)

# Get data by administrative unit
df = bdl.data.get_data_by_unit(
    unit_id="020000000000",
    variable_ids=["3643"],
    years=[2021]
)

# Get data for a locality
df = bdl.data.get_data_by_variable_locality(
    variable_id="3643",
    unit_parent_id="1465011",
    years=[2021]
)

# Get data by unit locality
df = bdl.data.get_data_by_unit_locality(
    unit_id="1465011",
    variable_id="3643",
    years=[2021]
)
```

The data endpoints automatically normalize nested `values` arrays into
flat rows.

#### Accessing Pagination Metadata

Use `return_metadata=True` to receive a `(DataFrame, metadata)` tuple
alongside the data. The metadata dictionary contains information from
the first response page, including pagination details such as
`totalPages` and `totalRecords`.

``` python
# Returns (DataFrame, metadata_dict)
df, meta = bdl.data.get_data_by_variable(
    variable_id="3643",
    years=[2021],
    return_metadata=True,
)
print(meta.get("totalPages"))
print(meta.get("totalRecords"))
```

Convenience `*_with_metadata` wrappers always return a tuple:

``` python
df, meta = bdl.data.get_data_by_variable_with_metadata(variable_id="3643", years=[2021])
df, meta = bdl.data.get_data_by_unit_with_metadata(unit_id="020000000000", variable_ids=["3643"])
df, meta = bdl.data.get_data_by_variable_locality_with_metadata(
    variable_id="3643", unit_parent_id="1465011", years=[2021]
)
df, meta = bdl.data.get_data_by_unit_locality_with_metadata(
    unit_id="1465011", variable_ids=["3643"]
)
```

### Units

Administrative units (regions, cities, etc.):

``` python
# List units by level
voivodeships = bdl.units.list_units(level=2)  # Level 2 = voivodeship

# Search units
warsaw = bdl.units.search_units(name="Warsaw")

# Get specific unit
unit_df = bdl.units.get_unit("020000000000")

# List localities (statistical localities)
localities = bdl.units.list_localities(level=6)  # Level 6 = municipality

# Search localities
warsaw_localities = bdl.units.search_localities(name="Warsaw", level=6)

# Get specific locality
locality_df = bdl.units.get_locality("1465011")
```

### Attributes

Data attributes (dimensions):

``` python
# List all attributes
attributes_df = bdl.attributes.list_attributes()

# Get specific attribute
attr_df = bdl.attributes.get_attribute("1")
```

### Measures

Measure units:

``` python
# List all measures
measures_df = bdl.measures.list_measures()

# Get specific measure
measure_df = bdl.measures.get_measure(1)
```

### Aggregates

Aggregation types:

``` python
# List all aggregates
aggregates_df = bdl.aggregates.list_aggregates()

# Get specific aggregate
aggregate_df = bdl.aggregates.get_aggregate("1")
```

### Years

Available years for data:

``` python
# List all available years
years_df = bdl.years.list_years()

# Get specific year metadata
year_df = bdl.years.get_year(2021)
```

## Enrichment

Many access layer methods accept an `enrich` parameter (or individual
`enrich_*` flags) to automatically join human-readable reference data
into the returned DataFrame. Enrichment fetches the required lookup
table once per client session, caches it in memory, and left-joins the
resolved columns onto each result row.

### Usage

Pass a list of dimension names to `enrich`:

``` python
# Enrich variables with level names, measure descriptions, and subject names
variables = bdl.variables.list_variables(enrich=["levels", "measures", "subjects"])

# Enrich data with unit details, attribute labels, and aggregate descriptions
data = bdl.data.get_data_by_variable(
    variable_id="3643",
    years=[2021],
    enrich=["units", "attributes", "aggregates"],
)
```

Individual `enrich_*` boolean flags are also accepted (legacy style):

``` python
data = bdl.data.get_data_by_variable("3643", years=[2021], enrich_attributes=True)
```

### Supported Enrichment Dimensions

The available enrichment dimensions depend on the endpoint:

<!-- pyml disable line-length -->
| Endpoint | `enrich` values | Added columns |
|----|----|----|
| Variables (`bdl.variables.*`) | `"levels"`, `"measures"`, `"subjects"` | `level_name`; `measure_unit_description`; `subject_name` |
| Data (`bdl.data.*`) | `"units"`, `"attributes"`, `"aggregates"` | `unit_name_enriched`, `unit_level`, `unit_parent_id`, `unit_kind`; `attr_name`, `attr_symbol`, `attr_description`; `aggregate_name`, `aggregate_description`, `aggregate_level` |
| Units (`bdl.units.*`) | `"levels"` | `level_name` |
| Aggregates (`bdl.aggregates.*`) | `"levels"` | `level_name` |
<!-- pyml enable line-length -->

### Caching

Lookup tables are fetched once per access-layer instance and cached for
the lifetime of the client session. Subsequent calls using the same
enrichment dimension reuse the cached data without additional API
requests.

``` python
bdl = BDL()
# First call: fetches the levels lookup table from the API
v1 = bdl.variables.list_variables(enrich=["levels"])
# Second call: reuses the cached levels table — no extra request
v2 = bdl.variables.get_variable("3643", enrich=["levels"])
```

## Pagination

Most list methods support pagination:

``` python
# Fetch all pages (default, max_pages=None)
all_data = bdl.variables.list_variables()

# Fetch only first page
first_page = bdl.variables.list_variables(max_pages=1, page_size=50)

# Limit number of pages
limited = bdl.variables.list_variables(max_pages=5, page_size=100)
```

Parameters:

- `max_pages`: Maximum number of pages to fetch. `None` (default)
  fetches all pages, `1` fetches only the first page, `N` fetches up to
  N pages.
- `page_size`: Number of results per page (default: 100 from config or
  100).
- `show_progress`: Display a `tqdm` progress bar while fetching pages
  (default: `True`). Set to `False` to suppress output in scripts or
  automated pipelines.

``` python
# Suppress progress bar
data = bdl.data.get_data_by_variable("3643", years=[2021], show_progress=False)
```

## Async Usage

All access layer methods have async versions (prefixed with `a`):

``` python
import asyncio
from pybdl import BDL

async def main():
    bdl = BDL()

    # Async methods return DataFrames
    levels_df = await bdl.levels.alist_levels()
    variables_df = await bdl.variables.alist_variables()

    # Can run multiple requests concurrently
    levels_task = bdl.levels.alist_levels()
    variables_task = bdl.variables.alist_variables()

    levels_df, variables_df = await asyncio.gather(levels_task, variables_task)

    return levels_df, variables_df

asyncio.run(main())
```

Available async methods:

- `alist_levels()`, `alist_variables()`, `alist_subjects()`, etc.
- `aget_level()`, `aget_variable()`, `aget_subject()`, etc.
- `aget_data_by_variable()`, `aget_data_by_unit()`, etc.

## Examples

### Basic Usage

``` python
from pybdl import BDL, BDLConfig

bdl = BDL(BDLConfig(api_key="your-api-key"))

# Get administrative levels
levels = bdl.levels.list_levels()
print(f"Found {len(levels)} administrative levels")

# Get variables related to population
population_vars = bdl.variables.search_variables(name="population")
print(f"Found {len(population_vars)} population-related variables")

# Get data for a specific variable
data = bdl.data.get_data_by_variable(
    variable_id="3643",
    years=[2021],
    unit_level=2  # Voivodeship level
)
print(data.head())
```

### Filtering and Analysis

``` python
# Get all variables
variables = bdl.variables.list_variables()

# Filter using pandas
economic_vars = variables[variables['name'].str.contains('economic', case=False)]

# Get data for multiple variables
for var_id in economic_vars['id'].head(5):
    data = bdl.data.get_data_by_variable(var_id, years=[2021])
    print(f"Variable {var_id}: {len(data)} records")
```

### Getting Data

``` python
# Get data
df = bdl.data.get_data_by_variable("3643", years=[2021])

# DataFrame includes IDs and values
print(df[['unit_name', 'attr_name', 'val']].head())

# Group by attribute name
by_attr = df.groupby('attr_name')['val'].mean()
print(by_attr)
```

### Working with Nested Data

The data endpoints automatically normalize nested structures:

``` python
# API returns nested structure, but access layer flattens it
df = bdl.data.get_data_by_variable("3643", years=[2021])

# Each row represents one data point
# Columns: unit_id, unit_name, year, val, attr_id, attr_name
print(df.head())

# Easy to analyze
avg_by_unit = df.groupby('unit_name')['val'].mean()
print(avg_by_unit)

# Get data for multiple years
multi_year_df = bdl.data.get_data_by_variable("3643", years=[2020, 2021, 2022])
# Analyze trends over time
yearly_avg = multi_year_df.groupby('year')['val'].mean()
print(yearly_avg)
```

See [examples](examples.ipynb) for more comprehensive real-world examples.

## API Reference

### Module `pybdl.access.base`

::: pybdl.access.base

### Module `pybdl.access.enrichment`

::: pybdl.access.enrichment

### Module `pybdl.access.data`

::: pybdl.access.data

### Module `pybdl.access.variables`

::: pybdl.access.variables

### Module `pybdl.access.subjects`

::: pybdl.access.subjects

### Module `pybdl.access.units`

::: pybdl.access.units

### Module `pybdl.access.levels`

::: pybdl.access.levels

### Module `pybdl.access.measures`

::: pybdl.access.measures

### Module `pybdl.access.attributes`

::: pybdl.access.attributes

### Module `pybdl.access.aggregates`

::: pybdl.access.aggregates

### Module `pybdl.access.years`

::: pybdl.access.years

!!! seealso

```markdown
- [Main client](main_client.md) — main client usage
- [API clients](api_clients.md) — low-level API access
- [Examples](examples.ipynb) — real-world examples
- [Configuration](config.md) — configuration options
```
