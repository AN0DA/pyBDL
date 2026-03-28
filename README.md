# pyBDL

`pyBDL` is a Python client for the Polish GUS Local Data Bank (Bank Danych Lokalnych, BDL).
It offers two complementary interfaces:

- `BDL` access layer: convenient `pandas.DataFrame` results for exploratory analysis.
- `BDL.api` layer: low-level endpoint clients that return raw JSON-compatible dictionaries.

## Installation

```bash
pip install pyBDL
```

Optional visualization dependencies used in notebooks and examples:

```bash
pip install "pyBDL[viz]"
```

## Quick Start

```python
from pybdl import BDL, BDLConfig

with BDL(BDLConfig(api_key="your-api-key")) as bdl:
    levels = bdl.levels.list_levels()
    variables = bdl.variables.search_variables(
        name="population",
        enrich=["levels", "subjects"],
    )
    data = bdl.data.get_data_by_variable(
        variable_id="3643",
        years=[2022, 2023],
        unit_level=2,
        enrich=["units", "attributes"],
    )
```

## Configuration

`BDLConfig` supports direct constructor arguments and environment variables:

- `BDL_API_KEY`
- `BDL_LANGUAGE`
- `BDL_FORMAT`
- `BDL_USE_CACHE`
- `BDL_CACHE_EXPIRY`
- `BDL_PROXY_URL`
- `BDL_PROXY_USERNAME`
- `BDL_PROXY_PASSWORD`
- `BDL_PAGE_SIZE`
- `BDL_REQUEST_RETRIES`
- `BDL_RETRY_BACKOFF_FACTOR`
- `BDL_MAX_RETRY_DELAY`
- `BDL_RETRY_STATUS_CODES`

Explicit constructor arguments take precedence over environment variables.

## API Layers

### Access Layer

Use the default `BDL` attributes for `DataFrame` output:

```python
from pybdl import BDL

bdl = BDL()
subjects = bdl.subjects.list_subjects()
data, metadata = bdl.data.get_data_by_variable_with_metadata(
    variable_id="3643",
    max_pages=1,
)
```

### Raw API Layer

Use `bdl.api` when you want direct control over the underlying payloads:

```python
from pybdl import BDL

bdl = BDL()
payload = bdl.api.data.get_data_by_variable(
    variable_id="3643",
    max_pages=1,
)
payload_with_meta = bdl.api.data.get_data_by_variable_with_metadata(
    variable_id="3643",
    max_pages=1,
)
```

## Highlights

- Automatic pagination helpers for collection endpoints
- Sync and async endpoint clients
- Built-in rate limiting with persistent quota tracking
- Configurable retry/backoff for transient HTTP failures
- Reference-data enrichment via `enrich=[...]`
- Context-manager support for clean session lifecycle management

## Documentation

Full documentation is built with Sphinx. To build locally:

```bash
make docs
# or: cd docs && make html
```

The built HTML is written to `docs/_build/html/`.

Key documentation pages:

- [Main Client](docs/main_client.rst) — BDL client, access vs API layer, context manager, async
- [Access Layer](docs/access_layer.rst) — DataFrame interface, enrichment, pagination, metadata
- [API Clients](docs/api_clients.rst) — Low-level raw-JSON clients
- [Configuration](docs/config.rst) — All options and environment variables
- [Rate Limiting](docs/rate_limiting.rst) — Quotas, retry, custom limits
- [Examples](docs/examples.ipynb) — Jupyter notebook with real-world examples
- [Changelog](CHANGELOG.md) — Release history
