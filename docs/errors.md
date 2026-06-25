# Error Handling

pyBDL raises structured exceptions for all error conditions. All exceptions
inherit from `GUSBDLError` so you can catch the entire hierarchy with a single
`except` clause.

## Exception hierarchy

```text
GUSBDLError (base)
├── BDLHTTPError               - HTTP-level error (4xx, 5xx, network failure)
├── BDLResponseError           - Unexpected or invalid API payload
└── RateLimitError             - Client-side quota exceeded
    └── RateLimitDelayExceeded - Wait time would exceed max_delay
```

## BDLHTTPError

Raised when the BDL server responds with an HTTP error or when the request
fails entirely (for example DNS failure or timeout).

```python
from pybdl.api.exceptions import BDLHTTPError

try:
    data = bdl.data.get_data_by_variable("invalid-id", years=[2021])
except BDLHTTPError as e:
    print(f"HTTP {e.status_code} from {e.url}")
    print(f"Body: {e.response_body}")
```

Useful attributes:

- `e.status_code` - HTTP status code (`int | None`)
- `e.url` - request URL (`str | None`)
- `e.response_body` - raw response body

## BDLResponseError

Raised when the API returns a response that pyBDL cannot parse (unexpected
structure, missing required fields).

```python
from pybdl.api.exceptions import BDLResponseError

try:
    data = bdl.data.get_data_by_variable("3643", years=[2021])
except BDLResponseError as e:
    print(f"Unexpected payload: {e.payload}")
```

## RateLimitError

Raised when the client-side quota is exhausted and `raise_on_rate_limit=True`
is configured. The default behavior is to wait, not raise.

```python
from pybdl import BDL, BDLConfig
from pybdl.api.exceptions import RateLimitDelayExceeded, RateLimitError

config = BDLConfig(api_key="...", raise_on_rate_limit=True)
bdl = BDL(config)

try:
    data = bdl.data.get_data_by_variable("3643", years=[2021])
except RateLimitDelayExceeded as e:
    print(f"Would need to wait {e.actual_delay:.1f}s (max allowed: {e.max_delay:.1f}s)")
except RateLimitError as e:
    print(f"Quota exceeded - retry in {e.retry_after:.1f}s")
    print(f"Quota details: {e.limit_info}")
```

`RateLimitDelayExceeded` is a subclass of `RateLimitError`, so catch it first.

## Catching all pyBDL errors

```python
from pybdl.api.exceptions import GUSBDLError

try:
    data = bdl.data.get_data_by_variable("3643", years=[2021])
except GUSBDLError as e:
    print(f"pyBDL error: {e}")
```

## HTTP 429 from the server

When the server responds with HTTP 429 (Too Many Requests), pyBDL retries
automatically using the `http_429_max_retries` / `BDL_HTTP_429_MAX_RETRIES`
budget, honoring `Retry-After` headers. This is separate from
`raise_on_rate_limit` and from `request_retries` (which covers 5xx errors).
See [Rate limiting](rate_limiting.md) for details.

## API reference

::: pybdl.api.exceptions
