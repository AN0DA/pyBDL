Configuration
=============

.. automodule:: pyldb.config
    :members:
    :undoc-members:
    :show-inheritance:
    :inherited-members:
    :noindex:

The :class:`pyldb.config.LDBConfig` class manages all configuration for authentication, language, caching, proxy settings, and quota/rate limiting.

.. seealso::
   - :doc:`main_client` for main client usage
   - :doc:`api_clients` for API endpoint usage
   - :doc:`rate_limiting` for comprehensive rate limiting documentation

Caching
-------

pyLDB supports transparent request caching to speed up repeated queries and reduce API usage. Caching can be enabled or disabled via the `use_cache` option in `LDBConfig`.

- **Cache location**: By default, cache is stored in a project-local `.cache/pyldb` directory. You can use a global cache or specify a custom path.
- **Cache expiry**: Set `cache_expire_after` (seconds) to control how long responses are cached.
- **Cache file management**: See :func:`pyldb.utils.cache.get_default_cache_path` and :func:`pyldb.utils.cache.get_cache_file_path`.

.. code-block:: python

    config = LDBConfig(api_key="...", use_cache=True, cache_expire_after=600)
    ldb = LDB(config)

Proxy Configuration
-------------------

The library supports HTTP/HTTPS proxy configuration through the following settings:

- ``proxy_url``: The URL of the proxy server (e.g., "http://proxy.example.com:8080")
- ``proxy_username``: Optional username for proxy authentication
- ``proxy_password``: Optional password for proxy authentication

These settings can be configured in three ways (in order of precedence):

1. Direct parameter passing when creating the config
2. Environment variables:
   - ``LDB_PROXY_URL``
   - ``LDB_PROXY_USERNAME``
   - ``LDB_PROXY_PASSWORD``
3. Default values (all None)

Example:

.. code-block:: python

    # Direct configuration
    config = LDBConfig(
        api_key="your-api-key",
        proxy_url="http://proxy.example.com:8080",
        proxy_username="user",
        proxy_password="pass"
    )

    # Or through environment variables
    # export LDB_PROXY_URL="http://proxy.example.com:8080"
    # export LDB_PROXY_USERNAME="user"
    # export LDB_PROXY_PASSWORD="pass"

Rate Limiting & Quotas
----------------------

pyLDB enforces API rate limits using both synchronous and asynchronous rate limiters. These limits are based on the official BDL API provider's policy, as described in the `BDL API Manual <https://api.stat.gov.pl/Home/BdlApi>`_ (see the "Manual" tab).

**Quick Overview**

- **Automatic enforcement**: Rate limiting is built into all API calls
- **Multiple quota periods**: Enforces limits across different time windows simultaneously
- **Persistent cache**: Quota usage survives process restarts
- **Sync & async support**: Works seamlessly with both synchronous and asynchronous code

**Default Quotas**

The following user limits apply:

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
