from typing import Any, Literal, overload

from pyldb.api.client import (
    FormatLiteral,
    LanguageLiteral,
    BaseAPIClient,
)


class DataAPI(BaseAPIClient):
    """
    Client for all LDB /data endpoints.

    Provides Pythonic, paginated, and DataFrame-ready access to all public
    data endpoints in the Local Data Bank (LDB) API. Supports flexible
    parameterization, pagination, and format options for robust data retrieval.

    Methods map directly to documented LDB endpoints under the /data namespace,
    enabling users to fetch statistical data by variable, unit, and locality.
    """

    @overload
    def get_data_by_variable(
        self,
        variable_id: str,
        year: list[int] | None = None,
        unit_parent_id: str | None = None,
        unit_level: int | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[True] = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    @overload
    def get_data_by_variable(
        self,
        variable_id: str,
        year: list[int] | None = None,
        unit_parent_id: str | None = None,
        unit_level: int | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    def get_data_by_variable(
        self,
        variable_id: str,
        year: list[int] | None = None,
        unit_parent_id: str | None = None,
        unit_level: int | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]:
        """
        Retrieve statistical data for a specific variable.

        Maps to: GET /data/by-variable/{var-id}

        Args:
            variable_id: Identifier of the variable.
            year: Optional list of years to filter by.
            unit_parent_id: Optional parent administrative unit ID.
            unit_level: Optional administrative unit aggregation level.
            aggregate_id: Optional aggregate ID.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            format: Expected response content type (defaults to config.format).
            lang: Expected response content language (defaults to config.language).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.
            return_metadata: If True, include metadata in the response.

        Returns:
            List of results, optionally with metadata dict.
        """
        extra_params: dict[str, Any] = {}
        if year:
            extra_params["year"] = year
        if unit_parent_id:
            extra_params["unit-parent-id"] = unit_parent_id
        if unit_level is not None:
            extra_params["unit-level"] = unit_level
        if aggregate_id is not None:
            extra_params["aggregate-id"] = aggregate_id
        if page is not None:
            extra_params["page"] = page
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        endpoint = f"data/by-variable/{variable_id}"

        result: tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]
        if all_pages:
            if return_metadata:
                result = self.fetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=True,
                )
            else:
                result = self.fetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=False,
                )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            if return_metadata:
                result = self.fetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=True,
                )
            else:
                result = self.fetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=False,
                )
        return result

    @overload
    def get_data_by_unit(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: Literal[True] = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    @overload
    def get_data_by_unit(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    def get_data_by_unit(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]:
        """
        Retrieve statistical data for a specific administrative unit.

        Maps to: GET /data/by-unit/{unit-id}

        Args:
            unit_id: Identifier of the administrative unit.
            var_id: List of variable IDs (required).
            year: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            format: Expected response content type (defaults to config.format).
            lang: Expected response content language (defaults to config.language).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            return_metadata: If True, include metadata in the response.

        Returns:
            List of results, optionally with metadata dict.
        """
        extra_params: dict[str, Any] = {"var-id": var_id}
        if year:
            extra_params["year"] = year
        if aggregate_id is not None:
            extra_params["aggregate-id"] = aggregate_id
        if page is not None:
            extra_params["page"] = page
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        params_with_page_size = params.copy()
        params_with_page_size["page-size"] = page_size

        endpoint = f"data/by-unit/{unit_id}"

        result: tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]
        if return_metadata:
            result = self.fetch_single_result(
                endpoint,
                results_key="results",
                params=params_with_page_size,
                headers=headers if headers else None,
                return_metadata=True,
            )
        else:
            result = self.fetch_single_result(
                endpoint,
                results_key="results",
                params=params_with_page_size,
                headers=headers if headers else None,
                return_metadata=False,
            )
        return result

    @overload
    def get_data_by_variable_locality(
        self,
        variable_id: str,
        unit_parent_id: str,
        year: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[True] = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    @overload
    def get_data_by_variable_locality(
        self,
        variable_id: str,
        unit_parent_id: str,
        year: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    def get_data_by_variable_locality(
        self,
        variable_id: str,
        unit_parent_id: str,
        year: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]:
        """
        Retrieve data for statistical localities for a single variable.

        Maps to: GET /data/localities/by-variable/{var-id}

        Args:
            variable_id: Identifier of the variable.
            unit_parent_id: Parent unit ID (required).
            year: Optional list of years to filter by.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            format: Expected response content type (defaults to config.format).
            lang: Expected response content language (defaults to config.language).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.
            return_metadata: If True, include metadata in the response.

        Returns:
            List of results, optionally with metadata dict.
        """
        extra_params: dict[str, Any] = {"unit-parent-id": unit_parent_id}
        if year:
            extra_params["year"] = year
        if page is not None:
            extra_params["page"] = page
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        endpoint = f"data/localities/by-variable/{variable_id}"

        result: tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]
        if all_pages:
            if return_metadata:
                result = self.fetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=True,
                )
            else:
                result = self.fetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=False,
                )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            if return_metadata:
                result = self.fetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=True,
                )
            else:
                result = self.fetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=False,
                )
        return result

    @overload
    def get_data_by_unit_locality(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[True] = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    @overload
    def get_data_by_unit_locality(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    def get_data_by_unit_locality(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]:
        """
        Retrieve data for a single statistical locality by unit.

        Maps to: GET /data/localities/by-unit/{unit-id}

        Args:
            unit_id: Identifier of the statistical locality.
            var_id: List of variable IDs (required).
            year: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            format: Expected response content type (defaults to config.format).
            lang: Expected response content language (defaults to config.language).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.
            return_metadata: If True, include metadata in the response.

        Returns:
            List of results, optionally with metadata dict.
        """
        extra_params: dict[str, Any] = {"var-id": var_id}
        if year:
            extra_params["year"] = year
        if aggregate_id is not None:
            extra_params["aggregate-id"] = aggregate_id
        if page is not None:
            extra_params["page"] = page
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        endpoint = f"data/localities/by-unit/{unit_id}"

        result: tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]
        if all_pages:
            if return_metadata:
                result = self.fetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=True,
                )
            else:
                result = self.fetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=False,
                )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            if return_metadata:
                result = self.fetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=True,
                )
            else:
                result = self.fetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=False,
                )
        return result

    def get_data_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve general metadata for the /data endpoint.

        Maps to: GET /data/metadata

        Args:
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            dict: Metadata describing the /data resource, fields, and parameters.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return self.fetch_single_result(
            "data/metadata", params=params if params else None, headers=headers if headers else None
        )

    # ASYNC VERSIONS
    @overload
    async def aget_data_by_variable(
        self,
        variable_id: str,
        year: list[int] | None = None,
        unit_parent_id: str | None = None,
        unit_level: int | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[True] = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    @overload
    async def aget_data_by_variable(
        self,
        variable_id: str,
        year: list[int] | None = None,
        unit_parent_id: str | None = None,
        unit_level: int | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    async def aget_data_by_variable(
        self,
        variable_id: str,
        year: list[int] | None = None,
        unit_parent_id: str | None = None,
        unit_level: int | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]:
        """
        Asynchronously retrieve statistical data for a specific variable.

        Maps to: GET /data/by-variable/{var-id}

        Args:
            variable_id: Identifier of the variable.
            year: Optional list of years to filter by.
            unit_parent_id: Optional parent administrative unit ID.
            unit_level: Optional administrative unit aggregation level.
            aggregate_id: Optional aggregate ID.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            format: Expected response content type (defaults to config.format).
            lang: Expected response content language (defaults to config.language).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.
            return_metadata: If True, include metadata in the response.

        Returns:
            List of results, optionally with metadata dict.
        """
        extra_params: dict[str, Any] = {}
        if year:
            extra_params["year"] = year
        if unit_parent_id:
            extra_params["unit-parent-id"] = unit_parent_id
        if unit_level is not None:
            extra_params["unit-level"] = unit_level
        if aggregate_id is not None:
            extra_params["aggregate-id"] = aggregate_id
        if page is not None:
            extra_params["page"] = page
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        endpoint = f"data/by-variable/{variable_id}"

        result: tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]
        if all_pages:
            if return_metadata:
                result = await self.afetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=True,
                )
            else:
                result = await self.afetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=False,
                )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            if return_metadata:
                result = await self.afetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=True,
                )
            else:
                result = await self.afetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=False,
                )
        return result

    @overload
    async def aget_data_by_unit(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: Literal[True] = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    @overload
    async def aget_data_by_unit(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    async def aget_data_by_unit(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]:
        """
        Asynchronously retrieve statistical data for a specific administrative unit.

        Maps to: GET /data/by-unit/{unit-id}

        Args:
            unit_id: Identifier of the administrative unit.
            var_id: List of variable IDs (required).
            year: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            format: Expected response content type (defaults to config.format).
            lang: Expected response content language (defaults to config.language).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            return_metadata: If True, include metadata in the response.

        Returns:
            List of results, optionally with metadata dict.
        """
        extra_params: dict[str, Any] = {"var-id": var_id}
        if year:
            extra_params["year"] = year
        if aggregate_id is not None:
            extra_params["aggregate-id"] = aggregate_id
        if page is not None:
            extra_params["page"] = page
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        params_with_page_size = params.copy()
        params_with_page_size["page-size"] = page_size

        endpoint = f"data/by-unit/{unit_id}"

        result: tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]
        if return_metadata:
            result = await self.afetch_single_result(
                endpoint,
                results_key="results",
                params=params_with_page_size,
                headers=headers if headers else None,
                return_metadata=True,
            )
        else:
            result = await self.afetch_single_result(
                endpoint,
                results_key="results",
                params=params_with_page_size,
                headers=headers if headers else None,
                return_metadata=False,
            )
        return result

    @overload
    async def aget_data_by_variable_locality(
        self,
        variable_id: str,
        unit_parent_id: str,
        year: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[True] = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    @overload
    async def aget_data_by_variable_locality(
        self,
        variable_id: str,
        unit_parent_id: str,
        year: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    async def aget_data_by_variable_locality(
        self,
        variable_id: str,
        unit_parent_id: str,
        year: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]:
        """
        Asynchronously retrieve data for statistical localities for a single variable.

        Maps to: GET /data/localities/by-variable/{var-id}

        Args:
            variable_id: Identifier of the variable.
            unit_parent_id: Parent unit ID (required).
            year: Optional list of years to filter by.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            format: Expected response content type (defaults to config.format).
            lang: Expected response content language (defaults to config.language).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.
            return_metadata: If True, include metadata in the response.

        Returns:
            List of results, optionally with metadata dict.
        """
        extra_params: dict[str, Any] = {"unit-parent-id": unit_parent_id}
        if year:
            extra_params["year"] = year
        if page is not None:
            extra_params["page"] = page
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        endpoint = f"data/localities/by-variable/{variable_id}"

        result: tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]
        if all_pages:
            if return_metadata:
                result = await self.afetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=True,
                )
            else:
                result = await self.afetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=False,
                )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            if return_metadata:
                result = await self.afetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=True,
                )
            else:
                result = await self.afetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=False,
                )
        return result

    @overload
    async def aget_data_by_unit_locality(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[True] = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]: ...

    @overload
    async def aget_data_by_unit_locality(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    async def aget_data_by_unit_locality(
        self,
        unit_id: str,
        var_id: list[int],
        year: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        all_pages: bool = True,
        return_metadata: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]:
        """
        Asynchronously retrieve data for a single statistical locality by unit.

        Maps to: GET /data/localities/by-unit/{unit-id}

        Args:
            unit_id: Identifier of the statistical locality.
            var_id: List of variable IDs (required).
            year: Optional list of years to filter by.
            aggregate_id: Optional aggregate ID.
            page: Optional page number to fetch.
            page_size: Number of results per page.
            max_pages: Maximum number of pages to fetch (None for all).
            format: Expected response content type (defaults to config.format).
            lang: Expected response content language (defaults to config.language).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.
            all_pages: If True, fetch all pages; otherwise, fetch only the first.
            return_metadata: If True, include metadata in the response.

        Returns:
            List of results, optionally with metadata dict.
        """
        extra_params: dict[str, Any] = {"var-id": var_id}
        if year:
            extra_params["year"] = year
        if aggregate_id is not None:
            extra_params["aggregate-id"] = aggregate_id
        if page is not None:
            extra_params["page"] = page
        if extra_query:
            extra_params.update(extra_query)

        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

        endpoint = f"data/localities/by-unit/{unit_id}"

        result: tuple[list[dict[str, Any]], dict[str, Any]] | list[dict[str, Any]]
        if all_pages:
            if return_metadata:
                result = await self.afetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=True,
                )
            else:
                result = await self.afetch_all_results(
                    endpoint,
                    params=params,
                    headers=headers if headers else None,
                    page_size=page_size,
                    max_pages=max_pages,
                    results_key="results",
                    return_metadata=False,
                )
        else:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            if return_metadata:
                result = await self.afetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=True,
                )
            else:
                result = await self.afetch_single_result(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers if headers else None,
                    return_metadata=False,
                )

        return result

    async def aget_data_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Asynchronously retrieve general metadata for the /data endpoint.

        Maps to: GET /data/metadata

        Args:
            lang: Expected response content language (defaults to config.language).
            format: Expected response content type (defaults to config.format).
            if_none_match: Conditional request header If-None-Match (entity tag).
            if_modified_since: Conditional request header If-Modified-Since.
            extra_query: Additional query parameters.

        Returns:
            dict: Metadata describing the /data resource, fields, and parameters.
        """
        params, headers = self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_query,
        )

        return await self.afetch_single_result(
            "data/metadata", params=params if params else None, headers=headers if headers else None
        )
