# Rate Limiting

pyBDL includes a sophisticated rate limiting system that automatically
enforces API quotas to prevent exceeding the BDL API provider's limits.
The rate limiter supports both synchronous and asynchronous operations,
persistent quota tracking, and flexible wait/raise behaviors.

## Overview

The rate limiting system enforces multiple quota periods simultaneously
(per second, per 15 minutes, per 12 hours, per 7 days) as specified by
the BDL API provider. It automatically tracks quota usage and can either
wait for quota to become available or raise exceptions when limits are
exceeded.

## Key Features

- **Automatic enforcement**: Rate limiting is built into all API calls
- **Multiple quota periods**: Enforces limits across different time
  windows simultaneously
- **Persistent cache**: Quota usage survives process restarts
- **Sync & async support**: Works seamlessly with both synchronous and
  asynchronous code
- **Configurable behavior**: Choose to wait or raise exceptions when
  limits are exceeded
- **Shared state**: Sync and async limiters share quota state via
  persistent cache

The `pybdl.config.BDLConfig` used by the API client defaults to
**waiting** when a local quota slot is not yet available
(`raise_on_rate_limit=False`). Set `raise_on_rate_limit=True` (or
environment variable `BDL_RATE_LIMIT_RAISE=true`) to raise
`pybdl.api.exceptions.BDLRateLimitError` immediately instead, for example
in tests that must fail fast.

When the **server** responds with HTTP **429** (Too Many Requests), the
client retries with a separate budget (`http_429_max_retries` /
`BDL_HTTP_429_MAX_RETRIES`). Retry timing is driven primarily by the
**client-side rate limiter**: if local quota is exhausted, the next
`acquire()` waits until a slot opens (sliding-window wait). If the server
returns 429 while local quota still reports an immediate slot (desync),
pyBDL emits `BDLQuotaDesyncWarning` and falls back to **exponential
backoff** (`retry_backoff_factor × 2^attempt`, capped by
`http_429_max_delay`, default 900 seconds). This is independent of
`request_retries`, which applies to other retryable status codes.

See [HTTP 429 retry strategy](#http-429-retry-strategy) and
[Server response headers (undocumented)](#server-response-headers-undocumented).

## Default Quotas

The rate limiter enforces the following default quotas based on user
registration status:

| Period | Anonymous user | Registered user |
|--------|----------------|-----------------|
| 1s     | 5              | 10              |
| 15m    | 100            | 500             |
| 12h    | 1,000          | 5,000           |
| 7d     | 10,000         | 50,000          |

These limits are automatically applied based on whether you provide an
API key (registered user) or not (anonymous user).

### Registration status detection

The library automatically determines your registration status:

- **Anonymous user**: When `api_key` is `None` or not provided in
  `BDLConfig`
- **Registered user**: When `api_key` is provided in `BDLConfig`

The rate limiter uses separate quota tracking for registered and
anonymous users, ensuring that each user type gets the correct limits.

## Server response headers (undocumented)

The BDL API may include `X-Rate-Limit-*` headers on HTTP responses. These
headers are **not described** in the official GUS documentation ([BDL API
portal](https://api.stat.gov.pl/Home/BdlApi?lang=en), [Methods Description
PDF](https://api.stat.gov.pl/Content/files/bdl/Methods_Description_API_BDL_v1.pdf)),
and pyBDL has **not** systematically validated their behaviour (including on
HTTP 429 responses).

Empirical observations (not guaranteed, subject to change):

| Header | Example | Likely meaning |
|--------|---------|----------------|
| `X-Rate-Limit-Limit` | `7d` | Label of the reported quota window (`1s`, `15m`, `12h`, `7d`), not a numeric cap |
| `X-Rate-Limit-Remaining` | `9998` | Remaining requests in that window |
| `X-Rate-Limit-Reset` | `2026-07-02T21:05:39Z` | Window reset time (ISO 8601, UTC) |

Open questions: which window the server reports at a given moment, whether
headers appear on 429 responses, and whether values match registered vs.
anonymous users.

**pyBDL does not parse these headers.** Limits are enforced by the internal
client-side rate limiter (`RateLimiter`, `DEFAULT_QUOTAS`, persistent quota
cache). Default quota values come from the official GUS documentation, not
from response headers.

## HTTP 429 retry strategy

When the server responds with HTTP 429:

1. **Local quota exhausted** — no extra sleep in the 429 handler; the next
   `acquire()` waits based on `_compute_wait()` (sliding window across all
   configured periods).
2. **Desync** (429 but local quota reports an immediate slot) — common when
   multiple processes share no quota cache, or server limits diverge from
   `DEFAULT_QUOTAS`. pyBDL emits `BDLQuotaDesyncWarning` and uses exponential
   backoff before retrying.

The BDL API does **not** document a `Retry-After` header; pyBDL does not
parse it.

To show the warning at most once per process:

```python
import warnings
from pybdl.api.exceptions import BDLQuotaDesyncWarning

warnings.filterwarnings("once", category=BDLQuotaDesyncWarning)
```

## User Guide

### Basic Usage

Rate limiting is automatically handled by the library. Simply use the
API client normally:

``` python
from pybdl import BDL, BDLConfig

config = BDLConfig(api_key="your-api-key")
bdl = BDL(config)

# Rate limiting is automatic - no extra code needed
data = bdl.api.data.get_data_by_variable(variable_id="3643", years=[2021])
```

The rate limiter will automatically:

- Track your API usage across all calls
- Enforce quota limits across all configured time windows
- **Wait** until quota is available (default). Set `raise_on_rate_limit=True`
  in `BDLConfig` to raise `BDLRateLimitError` immediately instead.

### Handling Rate Limit Errors

When `raise_on_rate_limit=True` is set, the client raises `BDLRateLimitError`
instead of waiting. Import it from either location:

``` python
from pybdl.api.exceptions import BDLRateLimitError
# or: from pybdl.utils.rate_limiter import BDLRateLimitError

try:
    data = bdl.api.data.get_data_by_variable(variable_id="3643", years=[2021])
except BDLRateLimitError as e:
    print(f"Rate limit exceeded. Retry after {e.retry_after:.1f} seconds")
    print(f"Limit info: {e.limit_info}")
```

The exception includes: - `retry_after`: Number of seconds to wait
before retrying - `limit_info`: Dictionary with detailed quota
information

### Waiting Instead of Raising

The `BDL` client already waits by default; this section covers direct
`RateLimiter` usage.

You can configure the rate limiter to wait automatically instead of
raising exceptions. This requires creating a custom rate limiter:

``` python
from pybdl.utils.rate_limiter import RateLimiter, PersistentQuotaCache
from pybdl.config import DEFAULT_QUOTAS

# Create a rate limiter that waits up to 30 seconds
cache = PersistentQuotaCache(enabled=True)
quotas = {k: v[1] for k, v in DEFAULT_QUOTAS.items()}  # Registered user quotas
limiter = RateLimiter(
    quotas=quotas,
    is_registered=True,
    cache=cache,
    raise_on_limit=False,  # Wait instead of raising
    max_delay=30.0  # Maximum wait time in seconds
)

# Use the limiter before making API calls
limiter.acquire()
data = bdl.api.data.get_data_by_variable(variable_id="3643", years=[2021])
```

### Using Context Managers

Rate limiters can be used as context managers for cleaner code:

``` python
from pybdl.utils.rate_limiter import RateLimiter, PersistentQuotaCache
from pybdl.config import DEFAULT_QUOTAS

cache = PersistentQuotaCache(enabled=True)
quotas = {k: v[1] for k, v in DEFAULT_QUOTAS.items()}
limiter = RateLimiter(quotas, is_registered=True, cache=cache)

# Automatically acquires quota when entering context
with limiter:
    data = bdl.api.data.get_data_by_variable(variable_id="3643", years=[2021])
```

### Using Decorators

You can decorate functions to automatically rate limit them:

``` python
from pybdl.utils.rate_limiter import rate_limit
from pybdl.config import DEFAULT_QUOTAS

quotas = {k: v[1] for k, v in DEFAULT_QUOTAS.items()}

@rate_limit(quotas=quotas, is_registered=True, max_delay=10)
def fetch_data(variable_id: str, year: int):
    return bdl.api.data.get_data_by_variable(variable_id=variable_id, years=[year])

# Function is automatically rate limited
data = fetch_data("3643", 2021)
```

For async functions:

``` python
from pybdl.utils.rate_limiter import async_rate_limit
from pybdl.config import DEFAULT_QUOTAS

quotas = {k: v[1] for k, v in DEFAULT_QUOTAS.items()}

@async_rate_limit(quotas=quotas, is_registered=True)
async def async_fetch_data(variable_id: str, year: int):
    return await bdl.api.data.aget_data_by_variable(variable_id=variable_id, years=[year])
```

### Checking Remaining Quota

You can check how much quota remains before making API calls:

``` python
from pybdl import BDL, BDLConfig

bdl = BDL(BDLConfig(api_key="your-api-key"))

# Get remaining quota (requires accessing the internal limiter)
remaining = bdl._client._sync_limiter.get_remaining_quota()
print(f"Remaining requests per second: {remaining.get(1, 0)}")
print(f"Remaining requests per 15 minutes: {remaining.get(900, 0)}")
```

### Custom Quotas

You can override default quotas for testing or special deployments:

``` python
from pybdl import BDLConfig

# Custom quotas: period in seconds -> limit
custom_quotas = {
    1: 20,        # 20 requests per second
    900: 500,     # 500 requests per 15 minutes
    43200: 2000,  # 2000 requests per 12 hours
    604800: 20000 # 20000 requests per 7 days
}

config = BDLConfig(api_key="your-api-key", custom_quotas=custom_quotas)
bdl = BDL(config)
```

Or via environment variable:

``` bash
export BDL_QUOTAS='{"1": 20, "900": 500}'
```

### Persistent Cache

The rate limiter uses a persistent cache to track quota usage across
process restarts. The cache is stored in:

- **Project-local**: `.cache/pybdl/quota_cache.json` (default)
- **Global**: Platform-specific cache directory (e.g.,
  `~/.cache/pybdl/quota_cache.json` on Linux)

You can disable persistent caching:

``` python
from pybdl import BDLConfig

config = BDLConfig(api_key="your-api-key", quota_cache_enabled=False)
bdl = BDL(config)
```

### Sync and Async Sharing

Both synchronous and asynchronous rate limiters share the same quota
state via the persistent cache. This means:

- Sync and async API calls count toward the same limits
- Quota usage persists across different execution contexts
- Process restarts maintain quota state

## Technical Details

For technical implementation details, including architecture, algorithm,
thread safety, cache implementation, and configuration options, see
[appendix](appendix.md).

## API Reference

::: pybdl.utils.rate_limiter

## Examples

### Example: Custom Rate Limiter with Wait Behavior

``` python
from pybdl.utils.rate_limiter import RateLimiter, PersistentQuotaCache
from pybdl.config import DEFAULT_QUOTAS

# Create cache
cache = PersistentQuotaCache(enabled=True)

# Get registered user quotas
quotas = {k: v[1] for k, v in DEFAULT_QUOTAS.items()}

# Create limiter that waits up to 30 seconds
limiter = RateLimiter(
    quotas=quotas,
    is_registered=True,
    cache=cache,
    raise_on_limit=False,
    max_delay=30.0
)

# Use limiter
limiter.acquire()  # Will wait if needed, up to 30 seconds
# Make your API call here
```

### Example: Handling Rate Limit Errors

``` python
from pybdl import BDL, BDLConfig
from pybdl.utils.rate_limiter import BDLRateLimitError, BDLRateLimitDelayError

bdl = BDL(BDLConfig(api_key="your-api-key", raise_on_rate_limit=True))

try:
    data = bdl.api.data.get_data_by_variable(variable_id="3643", years=[2021])
except BDLRateLimitError as e:
    if isinstance(e, BDLRateLimitDelayError):
        print(f"Would need to wait {e.actual_delay:.1f}s, exceeds max {e.max_delay:.1f}s")
    else:
        print(f"Rate limit exceeded. Retry after {e.retry_after:.1f}s")
        print(f"Current limits: {e.limit_info}")
```

### Example: Checking Quota Before Making Calls

``` python
from pybdl import BDL, BDLConfig

bdl = BDL(BDLConfig(api_key="your-api-key"))

# Check remaining quota
remaining = bdl._client._sync_limiter.get_remaining_quota()

if remaining.get(1, 0) < 5:
    print("Warning: Low quota remaining for 1-second period")
    # Consider waiting or reducing request rate

# Make API call
data = bdl.api.data.get_data_by_variable(variable_id="3643", years=[2021])
```

### Example: Resetting Quota (for testing)

``` python
from pybdl import BDL, BDLConfig

bdl = BDL(BDLConfig(api_key="your-api-key"))

# Reset quota counters (useful for testing)
bdl._client._sync_limiter.reset()

# Now you can make fresh API calls
```

## Best Practices

1. **Use default behavior**: The default wait behavior handles rate limits
   automatically and is suitable for most applications. Use
   `raise_on_rate_limit=True` only when you need fail-fast behavior
   (e.g. tests, CLI tools with user-facing feedback).
2. **Handle exceptions**: When `raise_on_rate_limit=True` is set, catch
   `BDLRateLimitError` and implement retry logic (or user-facing handling) as
   needed.
3. **Monitor quota**: Check remaining quota periodically to avoid
   hitting limits unexpectedly
4. **Use persistent cache**: Keep `quota_cache_enabled=True` (default)
   to maintain quota state across restarts
5. **Custom quotas for testing**: Use custom quotas when testing to
   avoid hitting production limits
6. **Async operations**: Use async rate limiters for async code to
   avoid blocking the event loop

## Troubleshooting

### BDLRateLimitError despite few calls

The persistent cache may contain old quota data. Try resetting the
quota or clearing the cache file.

### Sync vs async separate limits

Ensure both limiters share the same `PersistentQuotaCache` instance.
This is automatic when using `BDLConfig`.

### Rate limiter feels slow

Consider using async operations or adjusting `max_delay`. The rate
limiter adds minimal overhead (\<1ms per call).

### Corrupted cache file

The cache file is automatically recreated if corrupted. Old quota
data will be lost, but this is usually fine.

!!! seealso

```markdown
- [Configuration](config.md) — configuration options
- [API clients](api_clients.md) — API usage examples
- [Appendix](appendix.md) — technical implementation details
```
