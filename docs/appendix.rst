Appendix: Technical Implementation Details
===========================================

This appendix contains technical implementation details for developers and power users who need to understand the internal workings of pyBDL. For user-facing documentation, see the main sections.

Rate Limiting Implementation
----------------------------

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

.. code-block:: text

    GUSBDLError (base exception)
    └── RateLimitError
        └── RateLimitDelayExceeded

- **GUSBDLError**: Base exception for all GUS BDL API errors
- **RateLimitError**: Raised when rate limit is exceeded
- **RateLimitDelayExceeded**: Raised when required delay exceeds ``max_delay``

Rate Limiter Configuration Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RateLimiter and AsyncRateLimiter support the following parameters:

- **quotas**: Dictionary mapping period (seconds) to limit or (anon_limit, reg_limit) tuple
- **is_registered**: Whether the user is registered (affects quota selection)
- **cache**: Optional PersistentQuotaCache instance for persistent storage
- **max_delay**: Maximum seconds to wait (None = wait forever, 0 = raise immediately)
- **raise_on_limit**: If True, raise exception immediately; if False, wait
- **buffer_seconds**: Small buffer time added to wait calculations (default: 0.05s)

Configuration Implementation Details
------------------------------------

Cache File Management
~~~~~~~~~~~~~~~~~~~~~

The request cache system stores responses in JSON files:

**Cache Location**

- **Project-local** (default): ``.cache/pybdl/`` directory in the project root
- **Global**: Platform-specific cache directory:
  - Linux: ``~/.cache/pybdl/``
  - macOS: ``~/Library/Caches/pybdl/``
  - Windows: ``%LOCALAPPDATA%\\pybdl\\cache\\``

**Cache File Structure**

Cache files are named based on request parameters:
- Format: ``{method}_{endpoint_hash}.json``
- Hash includes: URL, query parameters, headers (API key excluded)

**Cache Expiry**

- Responses are cached with timestamps
- Expired entries are automatically ignored
- Cache files are not automatically cleaned (can be manually deleted)

**Internal Functions**

- ``get_default_cache_path()``: Returns platform-appropriate cache directory
- ``get_cache_file_path(url, method, params, headers)``: Generates cache file path for a request

Proxy Configuration Internals
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The proxy configuration is handled at the HTTP client level:

**Implementation**

- Uses ``requests`` library's proxy support for synchronous requests
- Uses ``aiohttp`` library's proxy support for asynchronous requests
- Proxy authentication uses HTTP Basic Auth

**Configuration Precedence**

1. Direct parameter in ``BDLConfig``
2. Environment variables (``BDL_PROXY_URL``, etc.)
3. Default values (None)

**Proxy URL Format**

- HTTP proxy: ``http://proxy.example.com:8080``
- HTTPS proxy: ``https://proxy.example.com:8080``
- SOCKS proxy: Not directly supported (requires additional configuration)

**Authentication**

- Username and password are sent via HTTP Basic Auth headers
- Credentials are not logged or exposed in error messages
- For security, prefer environment variables over hardcoded credentials

Access Layer Implementation
---------------------------

DataFrame Conversion
~~~~~~~~~~~~~~~~~~~~~

The access layer converts API responses to pandas DataFrames through several steps:

1. **Column Name Normalization**: camelCase → snake_case using regex patterns
2. **Data Type Inference**: 
   - Attempts numeric conversion (int/float)
   - Detects boolean values
   - Preserves strings/objects

Nested Data Normalization
~~~~~~~~~~~~~~~~~~~~~~~~~~

For data endpoints with nested ``values`` arrays:

1. Extract parent-level fields (e.g., ``id``, ``name``)
2. Flatten nested array: each nested item becomes a row
3. Combine parent fields with nested fields
4. Rename fields for clarity (e.g., ``id`` → ``unit_id``)

Example transformation:

.. code-block:: python

    # API response:
    [{"id": "1", "name": "Warsaw", "values": [{"year": 2021, "val": 1000}]}]
    
    # Access layer output:
    # DataFrame with columns: unit_id, unit_name, year, val


API Client Architecture
-----------------------

Request Handling
~~~~~~~~~~~~~~~~

All API clients inherit from a base client class that handles:

1. **Rate Limiting**: Automatic quota enforcement before requests
2. **Caching**: Optional response caching (if enabled)
3. **Error Handling**: Converts HTTP errors to Python exceptions
4. **Pagination**: Automatic page fetching and aggregation
5. **Internationalization**: Language parameter handling

HTTP Client Selection
~~~~~~~~~~~~~~~~~~~~~

- **Synchronous**: Uses ``requests`` library
- **Asynchronous**: Uses ``aiohttp`` library
- Both clients share the same configuration and rate limiting state

Response Processing
~~~~~~~~~~~~~~~~~~~

1. Parse JSON response
2. Extract data array or object
3. Handle pagination metadata
4. Return structured data (dict/list)

Error Handling
~~~~~~~~~~~~~~~

- HTTP 4xx/5xx errors → ``GUSBDLError`` or subclasses
- Rate limit errors → ``RateLimitError``
- Network errors → Standard Python exceptions
- JSON parsing errors → ``ValueError``

.. seealso::
   - :doc:`rate_limiting` for user-facing rate limiting documentation
   - :doc:`config` for user-facing configuration documentation
   - :doc:`api_clients` for API client usage
   - :doc:`access_layer` for access layer documentation

