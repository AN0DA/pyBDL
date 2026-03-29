# API Clients

> [!NOTE]
> **For most users, the access layer is recommended.** This section
> documents the low-level API client interface that returns raw
> dictionaries. The access layer (documented in [access layer](access_layer.md)) returns
> pandas DataFrames and is easier to use for data analysis.
>
> Use the API layer (`bdl.api.*`) when you need: - Raw API response
> structure - Custom response processing - Direct access to API
> metadata - Integration with non-pandas workflows
>
> Use the access layer (`bdl.*`) when you need: - Pandas DataFrames for
> data analysis - Automatic column normalization and type inference -
> Nested data flattening

The pyBDL library provides a comprehensive set of API clients for
interacting with the Local Data Bank (BDL) API. All API endpoints are
accessible through the main client's `.api`
attribute. See [Main client](main_client.md) for details about the main
client.

| Endpoint | Class | Description |
|----|----|----|
| Aggregates | `pybdl.api.aggregates.AggregatesAPI` | Aggregation level metadata and details |
| Attributes | `pybdl.api.attributes.AttributesAPI` | Attribute metadata and details |
| Data | `pybdl.api.data.DataAPI` | Statistical data access (variables, units, localities) |
| Levels | `pybdl.api.levels.LevelsAPI` | Administrative unit aggregation levels |
| Measures | `pybdl.api.measures.MeasuresAPI` | Measure unit metadata |
| Subjects | `pybdl.api.subjects.SubjectsAPI` | Subject hierarchy and metadata |
| Units | `pybdl.api.units.UnitsAPI` | Administrative unit metadata |
| Variables | `pybdl.api.variables.VariablesAPI` | Variable metadata and details |
| Version | `pybdl.api.version.VersionAPI` | API version and build info |
| Years | `pybdl.api.years.YearsAPI` | Available years for data |

Available API Endpoints

> [!NOTE]
> All API clients are accessible via `bdl.api.<endpoint>` (e.g.,
> `bdl.api.data.get_data_by_variable(...)`).

!!! seealso

    - [Configuration](config.md) — configuration options
    - [Main client](main_client.md) — main client usage

## Async Usage

All API clients support async methods for high-performance and
concurrent applications. Async methods are named with an
`a` prefix (e.g.,
`aget_data_by_variable`).

    import asyncio
    from pybdl import BDL

    async def main():
        bdl = BDL()
        data = await bdl.api.data.aget_data_by_variable(variable_id="3643", years=[2021])
        print(data)

    asyncio.run(main())

> [!NOTE]
> Async methods are available for all endpoints. See the API reference
> below for details.

## Format and Language Parameters

API clients support format and language parameters for controlling
response content:

**Format Options** (`FormatLiteral`): - `"json"` - JSON format
(default) - `"jsonapi"` - JSON:API format - `"xml"` - XML format

**Language Options** (`LanguageLiteral`): - `"pl"` - Polish (default if
configured) - `"en"` - English

The format and language parameters automatically set the appropriate
HTTP headers: - `Accept` header is set based on the format parameter -
`Accept-Language` header is set based on the language parameter

    from pybdl import BDL

    bdl = BDL()

    # Request data in XML format
    data = bdl.api.data.get_data_by_variable(
        variable_id="3643",
        years=[2021],
        format="xml"
    )

    # Request data in Polish
    data = bdl.api.data.get_data_by_variable(
        variable_id="3643",
        years=[2021],
        lang="pl"
    )

### Aggregates

::: pybdl.api.aggregates

### Attributes

::: pybdl.api.attributes

### Data

::: pybdl.api.data

### Levels

::: pybdl.api.levels

### Measures

::: pybdl.api.measures

### Subjects

::: pybdl.api.subjects

### Units

::: pybdl.api.units

### Variables

::: pybdl.api.variables

### Version

::: pybdl.api.version

### Years

::: pybdl.api.years
