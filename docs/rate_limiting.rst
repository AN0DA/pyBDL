Rate Limiting
==============

pyLDB includes a sophisticated rate limiting system that automatically enforces API quotas to prevent exceeding the BDL API provider's limits. The rate limiter supports both synchronous and asynchronous operations, persistent quota tracking, and flexible wait/raise behaviors.

Overview
--------

The rate limiting system enforces multiple quota periods simultaneously (per second, per 15 minutes, per 12 hours, per 7 days) as specified by the BDL API provider. It automatically tracks quota usage and can either wait for quota to become available or raise exceptions when limits are exceeded.

Key Features
------------

- **Automatic enforcement**: Rate limiting is built into all API calls
- **Multiple quota periods**: Enforces limits across different time windows simultaneously
- **Persistent cache**: Quota usage survives process restarts
- **Sync & async support**: Works seamlessly with both synchronous and asynchronous code
- **Configurable behavior**: Choose to wait or raise exceptions when limits are exceeded
- **Shared state**: Sync and async limiters share quota state via persistent cache

Default Quotas
--------------

The rate limiter enforces the following default quotas based on user registration status:

+---------+------------------+-------------------+
| Period  | Anonymous user   | Registered user   |
+=========+==================+===================+
| 1s      | 5                | 10                |
+---------+------------------+-------------------+
| 15m     | 100              | 500               |
+---------+------------------+-------------------+
| 12h     | 1,000            | 5,000             |
+---------+------------------+-------------------+
| 7d      | 10,000           | 50,000            |
+---------+------------------+-------------------+

These limits are automatically applied based on whether you provide an API key (registered user) or not (anonymous user).

User Guide
----------

Basic Usage
~~~~~~~~~~~

Rate limiting is automatically handled by the library. Simply use the API client normally:

.. code-block:: python

    from pyldb import LDB, LDBConfig
    
    config = LDBConfig(api_key="your-api-key")
    ldb = LDB(config)
    
    # Rate limiting is automatic - no extra code needed
    data = ldb.api.data.get_data_by_variable(variable_id="3643", year=2021)

The rate limiter will automatically:
- Track your API usage across all calls
- Enforce quota limits
- Raise exceptions if limits are exceeded (default behavior)

Handling Rate Limit Errors
~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the rate limiter raises a :class:`RateLimitError` when quota is exceeded:

.. code-block:: python

    from pyldb.api.utils.rate_limiter import RateLimitError
    
    try:
        data = ldb.api.data.get_data_by_variable(variable_id="3643", year=2021)
    except RateLimitError as e:
        print(f"Rate limit exceeded. Retry after {e.retry_after:.1f} seconds")
        print(f"Limit info: {e.limit_info}")

The exception includes:
- ``retry_after``: Number of seconds to wait before retrying
- ``limit_info``: Dictionary with detailed quota information

Waiting Instead of Raising
~~~~~~~~~~~~~~~~~~~~~~~~~~

You can configure the rate limiter to wait automatically instead of raising exceptions. This requires creating a custom rate limiter:

.. code-block:: python

    from pyldb.api.utils.rate_limiter import RateLimiter, PersistentQuotaCache
    from pyldb.config import DEFAULT_QUOTAS
    
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
    data = ldb.api.data.get_data_by_variable(variable_id="3643", year=2021)

Using Context Managers
~~~~~~~~~~~~~~~~~~~~~~

Rate limiters can be used as context managers for cleaner code:

.. code-block:: python

    from pyldb.api.utils.rate_limiter import RateLimiter, PersistentQuotaCache
    from pyldb.config import DEFAULT_QUOTAS
    
    cache = PersistentQuotaCache(enabled=True)
    quotas = {k: v[1] for k, v in DEFAULT_QUOTAS.items()}
    limiter = RateLimiter(quotas, is_registered=True, cache=cache)
    
    # Automatically acquires quota when entering context
    with limiter:
        data = ldb.api.data.get_data_by_variable(variable_id="3643", year=2021)

Using Decorators
~~~~~~~~~~~~~~~~

You can decorate functions to automatically rate limit them:

.. code-block:: python

    from pyldb.api.utils.rate_limiter import rate_limit
    from pyldb.config import DEFAULT_QUOTAS
    
    quotas = {k: v[1] for k, v in DEFAULT_QUOTAS.items()}
    
    @rate_limit(quotas=quotas, is_registered=True, max_delay=10)
    def fetch_data(variable_id: str, year: int):
        return ldb.api.data.get_data_by_variable(variable_id=variable_id, year=year)
    
    # Function is automatically rate limited
    data = fetch_data("3643", 2021)

For async functions:

.. code-block:: python

    from pyldb.api.utils.rate_limiter import async_rate_limit
    from pyldb.config import DEFAULT_QUOTAS
    
    quotas = {k: v[1] for k, v in DEFAULT_QUOTAS.items()}
    
    @async_rate_limit(quotas=quotas, is_registered=True)
    async def async_fetch_data(variable_id: str, year: int):
        return await ldb.api.data.aget_data_by_variable(variable_id=variable_id, year=year)

Checking Remaining Quota
~~~~~~~~~~~~~~~~~~~~~~~~

You can check how much quota remains before making API calls:

.. code-block:: python

    from pyldb import LDB, LDBConfig
    
    ldb = LDB(LDBConfig(api_key="your-api-key"))
    
    # Get remaining quota (requires accessing the internal limiter)
    remaining = ldb._client._sync_limiter.get_remaining_quota()
    print(f"Remaining requests per second: {remaining.get(1, 0)}")
    print(f"Remaining requests per 15 minutes: {remaining.get(900, 0)}")

Custom Quotas
~~~~~~~~~~~~~

You can override default quotas for testing or special deployments:

.. code-block:: python

    from pyldb import LDBConfig
    
    # Custom quotas: period in seconds -> limit
    custom_quotas = {
        1: 20,        # 20 requests per second
        900: 500,     # 500 requests per 15 minutes
        43200: 2000,  # 2000 requests per 12 hours
        604800: 20000 # 20000 requests per 7 days
    }
    
    config = LDBConfig(api_key="your-api-key", custom_quotas=custom_quotas)
    ldb = LDB(config)

Or via environment variable:

.. code-block:: bash

    export LDB_QUOTAS='{"1": 20, "900": 500}'

Persistent Cache
~~~~~~~~~~~~~~~~

The rate limiter uses a persistent cache to track quota usage across process restarts. The cache is stored in:

- **Project-local**: ``.cache/pyldb/quota_cache.json`` (default)
- **Global**: Platform-specific cache directory (e.g., ``~/.cache/pyldb/quota_cache.json`` on Linux)

You can disable persistent caching:

.. code-block:: python

    from pyldb import LDBConfig
    
    config = LDBConfig(api_key="your-api-key", quota_cache_enabled=False)
    ldb = LDB(config)

Sync and Async Sharing
~~~~~~~~~~~~~~~~~~~~~~

Both synchronous and asynchronous rate limiters share the same quota state via the persistent cache. This means:

- Sync and async API calls count toward the same limits
- Quota usage persists across different execution contexts
- Process restarts maintain quota state

Technical Documentation
------------------------

Architecture
~~~~~~~~~~~~

The rate limiting system consists of three main components:

1. **RateLimiter**: Thread-safe synchronous rate limiter
2. **AsyncRateLimiter**: Asyncio-compatible asynchronous rate limiter
3. **PersistentQuotaCache**: Thread-safe persistent storage for quota usage

Algorithm
~~~~~~~~~

The rate limiter uses a **sliding window** algorithm with multiple time periods:

1. Each quota period maintains a deque of timestamps for recent API calls
2. When ``acquire()`` is called:
   - Old timestamps (outside the current window) are removed
   - If current count >= limit, calculate wait time or raise exception
   - Record current timestamp for all periods
   - Save state to persistent cache

3. The longest wait time across all periods is used (most restrictive limit)

Time Handling
~~~~~~~~~~~~~

The rate limiter uses ``time.monotonic()`` instead of ``time.time()`` to ensure:
- Clock adjustments (NTP, daylight saving) don't affect quota calculations
- Accurate elapsed time measurements
- Consistent behavior across different system clock configurations

Thread Safety
~~~~~~~~~~~~~

- **RateLimiter**: Uses ``threading.Lock()`` for thread-safe operations
- **AsyncRateLimiter**: Uses ``asyncio.Lock()`` for async-safe operations
- **PersistentQuotaCache**: Uses ``threading.Lock()`` for thread-safe cache access

Both limiters can be safely used in concurrent environments.

Cache Implementation
~~~~~~~~~~~~~~~~~~~~

The persistent cache uses atomic file writes:

1. Write quota data to a temporary file (``quota_cache.json.tmp``)
2. Atomically rename temp file to final location (``quota_cache.json``)
3. This ensures cache integrity even if the process crashes during write

Cache keys are unified for sync and async limiters:
- Anonymous users: ``anon_<period>``
- Registered users: ``reg_<period>``

This allows sync and async limiters to share quota state.

Exception Hierarchy
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    GUSBDLError (base exception)
    └── RateLimitError
        └── RateLimitDelayExceeded

- **GUSBDLError**: Base exception for all GUS BDL API errors
- **RateLimitError**: Raised when rate limit is exceeded
- **RateLimitDelayExceeded**: Raised when required delay exceeds ``max_delay``

Configuration Options
~~~~~~~~~~~~~~~~~~~~~

RateLimiter and AsyncRateLimiter support the following parameters:

- **quotas**: Dictionary mapping period (seconds) to limit or (anon_limit, reg_limit) tuple
- **is_registered**: Whether the user is registered (affects quota selection)
- **cache**: Optional PersistentQuotaCache instance for persistent storage
- **max_delay**: Maximum seconds to wait (None = wait forever, 0 = raise immediately)
- **raise_on_limit**: If True, raise exception immediately; if False, wait
- **buffer_seconds**: Small buffer time added to wait calculations (default: 0.05s)

API Reference
-------------

.. automodule:: pyldb.api.utils.rate_limiter
    :members:
    :undoc-members:
    :show-inheritance:

Examples
--------

Example: Custom Rate Limiter with Wait Behavior
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pyldb.api.utils.rate_limiter import RateLimiter, PersistentQuotaCache
    from pyldb.config import DEFAULT_QUOTAS
    
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

Example: Handling Rate Limit Errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pyldb import LDB, LDBConfig
    from pyldb.api.utils.rate_limiter import RateLimitError, RateLimitDelayExceeded
    
    ldb = LDB(LDBConfig(api_key="your-api-key"))
    
    try:
        data = ldb.api.data.get_data_by_variable(variable_id="3643", year=2021)
    except RateLimitError as e:
        if isinstance(e, RateLimitDelayExceeded):
            print(f"Would need to wait {e.actual_delay:.1f}s, exceeds max {e.max_delay:.1f}s")
        else:
            print(f"Rate limit exceeded. Retry after {e.retry_after:.1f}s")
            print(f"Current limits: {e.limit_info}")

Example: Checking Quota Before Making Calls
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pyldb import LDB, LDBConfig
    
    ldb = LDB(LDBConfig(api_key="your-api-key"))
    
    # Check remaining quota
    remaining = ldb._client._sync_limiter.get_remaining_quota()
    
    if remaining.get(1, 0) < 5:
        print("Warning: Low quota remaining for 1-second period")
        # Consider waiting or reducing request rate
    
    # Make API call
    data = ldb.api.data.get_data_by_variable(variable_id="3643", year=2021)

Example: Resetting Quota (for testing)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from pyldb import LDB, LDBConfig
    
    ldb = LDB(LDBConfig(api_key="your-api-key"))
    
    # Reset quota counters (useful for testing)
    ldb._client._sync_limiter.reset()
    
    # Now you can make fresh API calls

Best Practices
--------------

1. **Use default behavior**: The default raise-on-limit behavior is usually best for most applications
2. **Handle exceptions**: Always catch ``RateLimitError`` and implement retry logic
3. **Monitor quota**: Check remaining quota periodically to avoid hitting limits unexpectedly
4. **Use persistent cache**: Keep ``quota_cache_enabled=True`` (default) to maintain quota state across restarts
5. **Custom quotas for testing**: Use custom quotas when testing to avoid hitting production limits
6. **Async operations**: Use async rate limiters for async code to avoid blocking the event loop

Troubleshooting
---------------

**Q: I'm getting RateLimitError even though I haven't made many calls**

A: The persistent cache may contain old quota data. Try resetting the quota or clearing the cache file.

**Q: Sync and async calls seem to have separate limits**

A: Ensure both limiters share the same ``PersistentQuotaCache`` instance. This is automatic when using ``LDBConfig``.

**Q: Rate limiter is too slow**

A: Consider using async operations or adjusting ``max_delay``. The rate limiter adds minimal overhead (<1ms per call).

**Q: Cache file is corrupted**

A: The cache file is automatically recreated if corrupted. Old quota data will be lost, but this is usually fine.

.. seealso::
   - :doc:`config` for configuration options
   - :doc:`api_clients` for API usage examples

