Configuration
=============

.. automodule:: pyldb.config
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:
    :noindex:

The :class:`pyldb.config.LDBConfig` class manages all configuration for authentication, language, caching, proxy settings, and quota/rate limiting.

Common Configuration Scenarios
-------------------------------

Basic Setup
~~~~~~~~~~~

.. code-block:: python

    from pyldb import LDB, LDBConfig
    
    # Minimal configuration (reads API key from environment)
    ldb = LDB()
    
    # Or provide API key directly
    config = LDBConfig(api_key="your-api-key")
    ldb = LDB(config)

Anonymous Access
~~~~~~~~~~~~~~~~

The API supports anonymous access without an API key. When ``api_key`` is explicitly set to ``None``, the client operates in anonymous mode with lower rate limits:

.. code-block:: python

    from pyldb import LDB, LDBConfig
    
    # Anonymous access (explicitly None - overrides environment variables)
    config = LDBConfig(api_key=None)
    ldb = LDB(config)
    
    # Or simply pass None
    ldb = LDB(config=None)  # Creates default config with api_key=None
    
    # Or use dict
    ldb = LDB(config={"api_key": None})

**Important**: Explicitly passing ``api_key=None`` is stronger than environment variables. If you want to use the environment variable ``LDB_API_KEY``, simply don't provide the ``api_key`` parameter (or use ``LDBConfig()``).

**Note**: Anonymous users have lower rate limits than registered users. See :doc:`rate_limiting` for details on quota differences.

Development Setup
~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Enable caching for faster development
    config = LDBConfig(
        api_key="your-api-key",
        use_cache=True,
        cache_expire_after=3600  # 1 hour
    )
    ldb = LDB(config)

Production Setup
~~~~~~~~~~~~~~~~

.. code-block:: python

    # Production configuration with rate limiting
    config = LDBConfig(
        api_key="your-api-key",
        use_cache=False,  # Disable cache for real-time data
        language="en",
        quota_cache_enabled=True  # Enable quota tracking
    )
    ldb = LDB(config)

Corporate Network Setup
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Behind corporate proxy
    config = LDBConfig(
        api_key="your-api-key",
        proxy_url="http://proxy.company.com:8080",
        proxy_username="username",  # Or use environment variables
        proxy_password="password"
    )
    ldb = LDB(config)

Environment Variables
---------------------

All configuration options can be set via environment variables:

.. code-block:: bash

    export LDB_API_KEY="your-api-key"
    export LDB_LANGUAGE="en"
    export LDB_USE_CACHE="true"
    export LDB_CACHE_EXPIRE_AFTER="3600"
    export LDB_PROXY_URL="http://proxy.example.com:8080"
    export LDB_PROXY_USERNAME="user"
    export LDB_PROXY_PASSWORD="pass"
    export LDB_QUOTAS='{"1": 20, "900": 500}'

Then use defaults in code:

.. code-block:: python

    # Reads from environment variables
    ldb = LDB()

.. seealso::
   - :doc:`main_client` for main client usage
   - :doc:`access_layer` for access layer usage
   - :doc:`api_clients` for API endpoint usage
   - :doc:`rate_limiting` for comprehensive rate limiting documentation

Caching
-------

pyLDB supports transparent request caching to speed up repeated queries and reduce API usage. Caching can be enabled or disabled via the `use_cache` option in `LDBConfig`.

**Basic Usage**

.. code-block:: python

    # Enable caching with 10-minute expiry
    config = LDBConfig(api_key="...", use_cache=True, cache_expire_after=600)
    ldb = LDB(config)
    
    # First call hits the API
    data1 = ldb.data.get_data_by_variable("3643", years=[2021])
    
    # Second call uses cache (if within expiry time)
    data2 = ldb.data.get_data_by_variable("3643", years=[2021])

**Configuration Options**

- ``use_cache``: Enable/disable caching (default: ``False``)
- ``cache_expire_after``: Cache expiry time in seconds (default: 3600 = 1 hour)

**When to Use Caching**

- Development and testing: Speed up repeated queries
- Data exploration: Avoid re-fetching the same data
- Batch processing: Cache metadata queries

**When Not to Use Caching**

- Real-time data: When you need the latest data
- One-off scripts: No benefit if queries aren't repeated
- Memory-constrained environments: Cache uses disk space

For technical details about cache file management and locations, see :doc:`appendix`.

Proxy Configuration
-------------------

The library supports HTTP/HTTPS proxy configuration for environments behind corporate firewalls or proxies.

**Basic Configuration**

.. code-block:: python

    # Direct configuration
    config = LDBConfig(
        api_key="your-api-key",
        proxy_url="http://proxy.example.com:8080",
        proxy_username="user",  # Optional
        proxy_password="pass"   # Optional
    )
    ldb = LDB(config)

**Using Environment Variables**

For security, prefer environment variables over hardcoded credentials:

.. code-block:: bash

    export LDB_PROXY_URL="http://proxy.example.com:8080"
    export LDB_PROXY_USERNAME="user"
    export LDB_PROXY_PASSWORD="pass"

Then use in Python:

.. code-block:: python

    # Configuration is read from environment variables
    config = LDBConfig(api_key="your-api-key")
    ldb = LDB(config)

**Configuration Precedence**

Settings are applied in this order:
1. Direct parameter passing (highest priority)
2. Environment variables
3. Default values (None)

**Common Use Cases**

- Corporate networks requiring proxy access
- VPN connections
- Development environments behind firewalls

For technical details about proxy implementation, see :doc:`appendix`.

Rate Limiting & Quotas
----------------------

pyLDB enforces API rate limits using both synchronous and asynchronous rate limiters. These limits are based on the official BDL API provider's policy, as described in the `BDL API Manual <https://api.stat.gov.pl/Home/BdlApi>`_ (see the "Manual" tab).

**Quick Overview**

- **Automatic enforcement**: Rate limiting is built into all API calls
- **Multiple quota periods**: Enforces limits across different time windows simultaneously
- **Persistent cache**: Quota usage survives process restarts
- **Sync & async support**: Works seamlessly with both synchronous and asynchronous code

**Default Quotas**

The following user limits apply. Quotas are automatically selected based on whether ``api_key`` is provided:

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

- **Anonymous user**: ``api_key=None`` or not provided (no ``X-ClientId`` header sent)
- **Registered user**: ``api_key`` is provided (``X-ClientId`` header sent with API key)

The rate limiter automatically selects the appropriate quota limits based on registration status.

**Custom Quotas**

To override default rate limits, provide a `custom_quotas` dictionary with integer keys representing the period in seconds:

.. code-block:: python

    config = LDBConfig(api_key="...", custom_quotas={1: 10, 900: 200, 43200: 2000, 604800: 20000})

Or via environment variable:

.. code-block:: bash

    export LDB_QUOTAS='{"1": 20, "900": 500}'

**Comprehensive Documentation**

For detailed information on rate limiting, including:
- How to handle rate limit errors
- Configuring wait vs. raise behavior
- Using context managers and decorators
- Checking remaining quota
- Technical implementation details

See :doc:`rate_limiting` for the complete guide.
