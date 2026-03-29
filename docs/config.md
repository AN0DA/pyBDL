# Configuration

The `pybdl.config.BDLConfig` class manages all configuration for
authentication, language, caching, proxy settings, and quota/rate
limiting.

## Common Configuration Scenarios

### Basic Setup

``` python
from pybdl import BDL, BDLConfig

# Minimal configuration (reads API key from environment)
bdl = BDL()

# Or provide API key directly
config = BDLConfig(api_key="your-api-key")
bdl = BDL(config)
```

### Anonymous Access

The API supports anonymous access without an API key. When `api_key` is
explicitly set to `None`, the client operates in anonymous mode with
lower rate limits:

``` python
from pybdl import BDL, BDLConfig

# Anonymous access (explicitly None - overrides environment variables)
config = BDLConfig(api_key=None)
bdl = BDL(config)

# Or simply pass None
bdl = BDL(config=None)  # Creates default config with api_key=None

# Or use dict
bdl = BDL(config={"api_key": None})
```

**Important**: Explicitly passing `api_key=None` is stronger than
environment variables. If you want to use the environment variable
`BDL_API_KEY`, simply don't provide the `api_key` parameter (or use
`BDLConfig()`).

**Note**: Anonymous users have lower rate limits than registered users.
See [rate limiting](rate_limiting.md) for details on quota differences.

### Development Setup

``` python
# Enable caching for faster development
config = BDLConfig(
    api_key="your-api-key",
    use_cache=True,
    cache_expire_after=3600  # 1 hour
)
bdl = BDL(config)
```

### Production Setup

``` python
# Production configuration with rate limiting
config = BDLConfig(
    api_key="your-api-key",
    use_cache=False,  # Disable cache for real-time data
    language="en",
    quota_cache_enabled=True  # Enable quota tracking
)
bdl = BDL(config)
```

### Corporate Network Setup

``` python
# Behind corporate proxy
config = BDLConfig(
    api_key="your-api-key",
    proxy_url="http://proxy.company.com:8080",
    proxy_username="username",  # Or use environment variables
    proxy_password="password"
)
bdl = BDL(config)
```

## Environment Variables

All configuration options can be set via environment variables. Explicit
constructor arguments always take precedence over environment variables.

<!-- pyml disable line-length -->
| Variable | Default | Description |
|----|----|----|
| `BDL_API_KEY` | *(none)* | API key for authenticated access. Omit for anonymous access. |
| `BDL_LANGUAGE` | `en` | Response language: `en` or `pl`. |
| `BDL_FORMAT` | `json` | Response format: `json`, `jsonapi`, or `xml`. |
| `BDL_USE_CACHE` | `true` | Enable HTTP response caching: `true` or `false`. |
| `BDL_CACHE_EXPIRY` | `3600` | Cache expiry time in seconds. |
| `BDL_PAGE_SIZE` | `100` | Default page size for paginated requests. |
| `BDL_PROXY_URL` | *(none)* | Proxy server URL, e.g. `http://proxy.example.com:8080`. |
| `BDL_PROXY_USERNAME` | *(none)* | Username for proxy authentication. |
| `BDL_PROXY_PASSWORD` | *(none)* | Password for proxy authentication. |
| `BDL_REQUEST_RETRIES` | `3` | Number of retry attempts for transient HTTP errors. |
| `BDL_RETRY_BACKOFF_FACTOR` | `0.5` | Base backoff multiplier (seconds) between retries. |
| `BDL_MAX_RETRY_DELAY` | `30.0` | Maximum time in seconds to wait between retries. |
| `BDL_RETRY_STATUS_CODES` | `429,500,502,503,504` | Comma-separated HTTP status codes that trigger a retry. |
| `BDL_RATE_LIMIT_RAISE` | `false` | If `true`, raise `RateLimitError` when client-side quota is exhausted; if `false` (default), wait until a slot is available. |
| `BDL_HTTP_429_MAX_RETRIES` | `12` | Max retries when the **server** returns HTTP 429 (separate from `BDL_REQUEST_RETRIES` for 5xx). Honors `Retry-After` up to `BDL_HTTP_429_MAX_DELAY`; otherwise uses exponential backoff from `BDL_RETRY_BACKOFF_FACTOR`. |
| `BDL_HTTP_429_MAX_DELAY` | `900` | Max seconds to wait between HTTP 429 retries (15 minutes; aligns with common BDL quota windows). |
| `BDL_QUOTAS` | *(BDL defaults)* | JSON object overriding rate-limit quotas, e.g. `'{"1": 20, "900": 500}'`. |
| `BDL_QUOTA_CACHE_ENABLED` | `true` | Persist quota usage across process restarts. |
| `BDL_QUOTA_CACHE` | *(auto)* | Path to the quota cache file. |
| `BDL_USE_GLOBAL_CACHE` | `false` | Store quota cache in the OS-level cache directory instead of the project `.cache/`. |
<!-- pyml enable line-length -->

Example — set environment variables and use defaults in code:

``` bash
export BDL_API_KEY="your-api-key"
export BDL_LANGUAGE="en"
export BDL_USE_CACHE="true"
export BDL_CACHE_EXPIRY="3600"
export BDL_PROXY_URL="http://proxy.example.com:8080"
export BDL_PROXY_USERNAME="user"
export BDL_PROXY_PASSWORD="pass"
export BDL_QUOTAS='{"1": 20, "900": 500}'
```

``` python
# All settings are read from environment variables
bdl = BDL()
```

!!! seealso

```markdown
- [Main client](main_client.md) — main client usage
- [Access layer](access_layer.md) — access layer usage
- [API clients](api_clients.md) — API endpoint usage
- [Rate limiting](rate_limiting.md) — comprehensive rate limiting documentation
```

## Caching

pyBDL supports transparent HTTP response caching to speed up repeated
queries and reduce unnecessary API traffic. The same caching model is
available for both synchronous and asynchronous clients, so repeated
calls made through either interface can reuse previously stored
responses.

At a high level, caching works like this:

1. The first request for a given URL is sent to the BDL API and the
   response is stored.
2. A later request for the same URL can be served from cache instead of
   making another network call.
3. Cached responses expire after `cache_expire_after` seconds.
4. When the response comes from cache, pyBDL refunds the temporary
   quota reservation, so cached reads do not consume rate limit quota.

### Caching basic usage

``` python
# Enable file-backed caching with 10-minute expiry
config = BDLConfig(api_key="...", cache_backend="file", cache_expire_after=600)
bdl = BDL(config)

# First call hits the API
data1 = bdl.data.get_data_by_variable("3643", years=[2021])

# Second call uses cache (if within expiry time)
data2 = bdl.data.get_data_by_variable("3643", years=[2021])
```

### Caching backends

pyBDL supports two cache backends plus a disabled mode:

- `cache_backend="file"`: Stores cache data on disk and is the
  recommended default for most users.
- `cache_backend="memory"`: Uses an in-memory SQLite database that
  exists only for the lifetime of the current process.
- `cache_backend=None`: Disables caching completely.

``` python
from pybdl import BDL, BDLConfig

# File-backed cache shared across sync/async clients
file_config = BDLConfig(
    api_key="...",
    cache_backend="file",
    cache_expire_after=3600,
)

# In-memory cache for short-lived scripts or tests
memory_config = BDLConfig(
    api_key="...",
    cache_backend="memory",
    cache_expire_after=300,
)

# Disable cache entirely
no_cache_config = BDLConfig(
    api_key="...",
    cache_backend=None,
)
```

### Caching configuration fields

- `use_cache`: Backward-compatible boolean toggle for caching
- `cache_backend`: `"file"`, `"memory"`, or `None` to disable caching
- `cache_expire_after`: Cache expiry time in seconds (default: 3600 = 1
  hour)

### Caching behavior

- The first request for a resource is usually slower because it goes to
  the network.
- Repeated requests for the same URL are usually faster because the
  cached response can be reused.
- File-backed cache persists across process restarts.
- Memory-backed cache is cleared when the Python process exits.
- With `cache_backend="file"`, sync and async clients use the same cache
  file and can reuse each other's cached responses.
- With `cache_backend="memory"`, sync and async clients each keep their
  own in-memory cache and do not share entries.
- Cache keys are based on the actual HTTP request, so changing endpoint
  parameters, language, format, or headers that affect representation
  may produce a different cache entry.

### Caching and rate limiting

pyBDL still reserves a quota slot before issuing the request, because at
that moment it does not yet know whether the response will come from
cache. If the response is later identified as a cache hit, that
reservation is released immediately.

In practice this means:

- Real network requests count against quota.
- Cache hits do not reduce available quota.
- You can safely enable caching to reduce rate-limit pressure during
  repeated exploration or batch workflows.

### Choosing a cache backend

Use `"file"` when:

- You want cache reuse between script runs
- You mix sync and async usage and want both to share cache entries
- You do longer exploratory or ETL-style workflows

Use `"memory"` when:

- You want temporary caching only inside one process
- You do not want cache files on disk
- You are running isolated tests or short-lived scripts

Use `None` when:

- You always want fresh data from the API
- You are debugging live responses
- You want the simplest possible request behavior

### When caching helps

- Development and testing: Speed up repeated queries
- Data exploration: Avoid re-fetching the same data
- Batch processing: Cache metadata queries

### When not to use caching

- Real-time data: When you need the latest data
- One-off scripts: No benefit if queries aren't repeated
- Memory-constrained environments: Cache uses disk space

### Caching environment variables

Caching can also be configured through environment variables:

``` bash
export BDL_CACHE_BACKEND="file"
export BDL_CACHE_EXPIRY="1800"
```

``` python
# Reads cache settings from the environment
bdl = BDL(BDLConfig(api_key="..."))
```

For technical details about cache file management, cache locations, and
implementation details, see [appendix](appendix.md).

## Proxy Configuration

The library supports HTTP/HTTPS proxy configuration for environments
behind corporate firewalls or proxies.

### Proxy basic setup

``` python
# Direct configuration
config = BDLConfig(
    api_key="your-api-key",
    proxy_url="http://proxy.example.com:8080",
    proxy_username="user",  # Optional
    proxy_password="pass"   # Optional
)
bdl = BDL(config)
```

### Proxy credentials via environment

For security, prefer environment variables over hardcoded credentials:

``` bash
export BDL_PROXY_URL="http://proxy.example.com:8080"
export BDL_PROXY_USERNAME="user"
export BDL_PROXY_PASSWORD="pass"
```

Then use in Python:

``` python
# Configuration is read from environment variables
config = BDLConfig(api_key="your-api-key")
bdl = BDL(config)
```

### Proxy configuration precedence

Settings are applied in this order: 1. Direct parameter passing (highest
priority) 2. Environment variables 3. Default values (None)

### Proxy common scenarios

- Corporate networks requiring proxy access
- VPN connections
- Development environments behind firewalls

For technical details about proxy implementation, see [appendix](appendix.md).

## Rate Limiting & Quotas

pyBDL enforces API rate limits using both synchronous and asynchronous
rate limiters. These limits are based on the official BDL API provider's
policy, as described in the [BDL API
Manual](https://api.stat.gov.pl/Home/BdlApi) (see the "Manual" tab).

### Rate limiting overview

- **Automatic enforcement**: Rate limiting is built into all API calls
- **Multiple quota periods**: Enforces limits across different time
  windows simultaneously
- **Persistent cache**: Quota usage survives process restarts
- **Sync & async support**: Works seamlessly with both synchronous and
  asynchronous code
- **Wait by default**: `raise_on_rate_limit` defaults to `False` so the
  client waits for quota; set `True` or `BDL_RATE_LIMIT_RAISE` to raise
  immediately (see [rate limiting](rate_limiting.md))

### Default quotas

The following user limits apply. Quotas are automatically selected based
on whether `api_key` is provided:

| Period | Anonymous user | Registered user |
|--------|----------------|-----------------|
| 1s     | 5              | 10              |
| 15m    | 100            | 500             |
| 12h    | 1,000          | 5,000           |
| 7d     | 10,000         | 50,000          |

- **Anonymous user**: `api_key=None` or not provided (no `X-ClientId`
  header sent)
- **Registered user**: `api_key` is provided (`X-ClientId` header sent
  with API key)

The rate limiter automatically selects the appropriate quota limits
based on registration status.

### Custom quota overrides

To override default rate limits, provide a
`custom_quotas` dictionary with integer
keys representing the period in seconds:

``` python
config = BDLConfig(
    api_key="...",
    custom_quotas={1: 10, 900: 200, 43200: 2000, 604800: 20000},
)
```

Or via environment variable:

``` bash
export BDL_QUOTAS='{"1": 20, "900": 500}'
```

### Further rate limiting documentation

For detailed information on rate limiting (errors, wait vs. raise, context
managers, decorators, remaining quota, implementation details), see
[rate limiting](rate_limiting.md).

## Retry Configuration

pyBDL automatically retries requests that fail with transient HTTP
errors. Retries use exponential back-off up to a configurable ceiling.

### Retry options

- `request_retries`: Total retry attempts before raising (default: `3`).
- `retry_backoff_factor`: Back-off multiplier in seconds (default:
  `0.5`). Delay before attempt *n* = `retry_backoff_factor × 2^(n-1)`
  seconds.
- `max_retry_delay`: Upper bound on any individual retry delay in
  seconds (default: `30.0`).
- `retry_status_codes`: HTTP status codes that should trigger a retry
  (default: `429, 500, 502, 503, 504`).

### Retry examples

``` python
from pybdl import BDL, BDLConfig

# Aggressive retry for unreliable networks
config = BDLConfig(
    api_key="your-api-key",
    request_retries=5,
    retry_backoff_factor=1.0,
    max_retry_delay=60.0,
)
bdl = BDL(config)
```

``` python
# Disable retries entirely
config = BDLConfig(api_key="your-api-key", request_retries=0)
bdl = BDL(config)
```

Or via environment variables:

``` bash
export BDL_REQUEST_RETRIES=5
export BDL_RETRY_BACKOFF_FACTOR=1.0
export BDL_MAX_RETRY_DELAY=60.0
export BDL_RETRY_STATUS_CODES="429,500,502,503,504"
```

## API reference

::: pybdl.config
