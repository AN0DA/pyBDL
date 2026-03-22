from collections.abc import Sequence
from typing import Any, cast

from pybdl.api.client import (
    BaseAPIClient,
    FormatLiteral,
    LanguageLiteral,
)

# Payload + optional metadata as returned by BaseAPIClient fetch helpers for /data collection routes
_DataJsonPayload = dict[str, Any] | list[dict[str, Any]]
_DataWithMetadata = tuple[_DataJsonPayload, dict[str, Any]]
_DataCollectionResult = list[dict[str, Any]] | _DataWithMetadata


class DataAPI(BaseAPIClient):
    """
    Client for all BDL /data endpoints.

    Provides Pythonic, paginated, and DataFrame-ready access to all public
    data endpoints in the Local Data Bank (BDL) API. Supports flexible
    parameterization, pagination, and format options for robust data retrieval.

    Methods map directly to documented BDL endpoints under the /data namespace,
    enabling users to fetch statistical data by variable, unit, and locality.
    """

    @staticmethod
    def _normalize_variable_ids(
        variable_ids: Sequence[str | int] | str | int | None,
        variable_id: Sequence[str | int] | str | int | None,
    ) -> list[int]:
        if variable_ids is not None and variable_id is not None:
            raise TypeError("Use either 'variable_ids' or the legacy 'variable_id' parameter, not both.")
        resolved = variable_ids if variable_ids is not None else variable_id
        if resolved is None:
            raise TypeError("'variable_ids' is required.")
        raw_values = [resolved] if isinstance(resolved, (str, int)) else list(resolved)
        return [int(item) for item in raw_values]

    @staticmethod
    def _data_by_variable_params(
        years: list[int] | None,
        unit_parent_id: str | None,
        unit_level: int | None,
        aggregate_id: int | None,
        page: int | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if years:
            params["year"] = years
        if unit_parent_id:
            params["unit-parent-id"] = unit_parent_id
        if unit_level is not None:
            params["unit-level"] = unit_level
        if aggregate_id is not None:
            params["aggregate-id"] = aggregate_id
        if page is not None:
            params["page"] = page
        if extra_query:
            params.update(extra_query)
        return params

    def _data_by_unit_params(
        self,
        variable_ids: Sequence[str | int] | str | int | None,
        variable_id: Sequence[str | int] | str | int | None,
        years: list[int] | None,
        aggregate_id: int | None,
        page: int | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"var-id": self._normalize_variable_ids(variable_ids, variable_id)}
        if years:
            params["year"] = years
        if aggregate_id is not None:
            params["aggregate-id"] = aggregate_id
        if page is not None:
            params["page"] = page
        if extra_query:
            params.update(extra_query)
        return params

    @staticmethod
    def _data_by_variable_locality_params(
        unit_parent_id: str,
        years: list[int] | None,
        page: int | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"unit-parent-id": unit_parent_id}
        if years:
            params["year"] = years
        if page is not None:
            params["page"] = page
        if extra_query:
            params.update(extra_query)
        return params

    def _data_by_unit_locality_params(
        self,
        variable_ids: Sequence[str | int] | str | int | None,
        variable_id: Sequence[str | int] | str | int | None,
        years: list[int] | None,
        aggregate_id: int | None,
        page: int | None,
        extra_query: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return self._data_by_unit_params(variable_ids, variable_id, years, aggregate_id, page, extra_query)

    def _prepare_collection_request(
        self,
        *,
        extra_params: dict[str, Any],
        format: FormatLiteral | None,
        lang: LanguageLiteral | None,
        if_none_match: str | None,
        if_modified_since: str | None,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        return self._prepare_api_params_and_headers(
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            extra_params=extra_params,
        )

    def _fetch_single_page_data_collection(
        self,
        endpoint: str,
        *,
        params: dict[str, Any],
        headers: dict[str, str] | None,
        page_size: int,
        return_metadata: bool,
    ) -> _DataCollectionResult:
        params_with_page_size = params.copy()
        params_with_page_size["page-size"] = page_size
        if return_metadata:
            return self.fetch_single_result_with_metadata(
                endpoint,
                results_key="results",
                params=params_with_page_size,
                headers=headers,
            )
        return self.fetch_single_result(
            endpoint,
            results_key="results",
            params=params_with_page_size,
            headers=headers,
        )

    async def _afetch_single_page_data_collection(
        self,
        endpoint: str,
        *,
        params: dict[str, Any],
        headers: dict[str, str] | None,
        page_size: int,
        return_metadata: bool,
    ) -> _DataCollectionResult:
        params_with_page_size = params.copy()
        params_with_page_size["page-size"] = page_size
        if return_metadata:
            return await self.afetch_single_result_with_metadata(
                endpoint,
                results_key="results",
                params=params_with_page_size,
                headers=headers,
            )
        return await self.afetch_single_result(
            endpoint,
            results_key="results",
            params=params_with_page_size,
            headers=headers,
        )

    def _fetch_data_collection(
        self,
        endpoint: str,
        *,
        params: dict[str, Any],
        headers: dict[str, str] | None,
        page_size: int,
        max_pages: int | None,
        return_metadata: bool,
    ) -> _DataCollectionResult:
        if max_pages == 1:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            if return_metadata:
                return self.fetch_single_result_with_metadata(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers,
                )
            return self.fetch_single_result(
                endpoint,
                results_key="results",
                params=params_with_page_size,
                headers=headers,
            )

        if return_metadata:
            return self.fetch_all_results_with_metadata(
                endpoint,
                params=params,
                headers=headers,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        return self.fetch_all_results(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
        )

    async def _afetch_data_collection(
        self,
        endpoint: str,
        *,
        params: dict[str, Any],
        headers: dict[str, str] | None,
        page_size: int,
        max_pages: int | None,
        return_metadata: bool,
    ) -> _DataCollectionResult:
        if max_pages == 1:
            params_with_page_size = params.copy()
            params_with_page_size["page-size"] = page_size
            if return_metadata:
                return await self.afetch_single_result_with_metadata(
                    endpoint,
                    results_key="results",
                    params=params_with_page_size,
                    headers=headers,
                )
            return await self.afetch_single_result(
                endpoint,
                results_key="results",
                params=params_with_page_size,
                headers=headers,
            )

        if return_metadata:
            return await self.afetch_all_results_with_metadata(
                endpoint,
                params=params,
                headers=headers,
                page_size=page_size,
                max_pages=max_pages,
                results_key="results",
            )
        return await self.afetch_all_results(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            results_key="results",
        )

    def get_data_by_variable(
        self,
        variable_id: str,
        years: list[int] | None = None,
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
        return_metadata: bool = False,
    ) -> _DataCollectionResult:
        endpoint = f"data/by-variable/{variable_id}"
        params, headers = self._prepare_collection_request(
            extra_params=self._data_by_variable_params(
                years,
                unit_parent_id,
                unit_level,
                aggregate_id,
                page,
                extra_query,
            ),
            format=format,
            lang=lang,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
        return self._fetch_data_collection(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            return_metadata=return_metadata,
        )

    def get_data_by_variable_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Retrieve data by variable and always return `(results, metadata)`."""
        kwargs["return_metadata"] = True
        return cast(
            tuple[list[dict[str, Any]], dict[str, Any]],
            self.get_data_by_variable(*args, **kwargs),
        )

    def get_data_by_unit(
        self,
        unit_id: str,
        variable_ids: Sequence[str | int] | str | int | None = None,
        *,
        variable_id: Sequence[str | int] | str | int | None = None,
        years: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: bool = False,
    ) -> _DataCollectionResult:
        endpoint = f"data/by-unit/{unit_id}"
        params, headers = self._prepare_collection_request(
            extra_params=self._data_by_unit_params(variable_ids, variable_id, years, aggregate_id, page, extra_query),
            format=format,
            lang=lang,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
        return self._fetch_single_page_data_collection(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            return_metadata=return_metadata,
        )

    def get_data_by_unit_with_metadata(self, *args: Any, **kwargs: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Retrieve data by unit and always return `(results, metadata)`."""
        kwargs["return_metadata"] = True
        return cast(
            tuple[list[dict[str, Any]], dict[str, Any]],
            self.get_data_by_unit(*args, **kwargs),
        )

    def get_data_by_variable_locality(
        self,
        variable_id: str,
        unit_parent_id: str,
        years: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: bool = False,
    ) -> _DataCollectionResult:
        endpoint = f"data/localities/by-variable/{variable_id}"
        params, headers = self._prepare_collection_request(
            extra_params=self._data_by_variable_locality_params(unit_parent_id, years, page, extra_query),
            format=format,
            lang=lang,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
        return self._fetch_data_collection(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            return_metadata=return_metadata,
        )

    def get_data_by_variable_locality_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Retrieve locality data by variable and always return `(results, metadata)`."""
        kwargs["return_metadata"] = True
        return cast(
            tuple[list[dict[str, Any]], dict[str, Any]],
            self.get_data_by_variable_locality(*args, **kwargs),
        )

    def get_data_by_unit_locality(
        self,
        unit_id: str,
        variable_ids: Sequence[str | int] | str | int | None = None,
        *,
        variable_id: Sequence[str | int] | str | int | None = None,
        years: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: bool = False,
    ) -> _DataCollectionResult:
        endpoint = f"data/localities/by-unit/{unit_id}"
        params, headers = self._prepare_collection_request(
            extra_params=self._data_by_unit_locality_params(
                variable_ids,
                variable_id,
                years,
                aggregate_id,
                page,
                extra_query,
            ),
            format=format,
            lang=lang,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
        return self._fetch_data_collection(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            return_metadata=return_metadata,
        )

    def get_data_by_unit_locality_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Retrieve locality data by unit and always return `(results, metadata)`."""
        kwargs["return_metadata"] = True
        return cast(
            tuple[list[dict[str, Any]], dict[str, Any]],
            self.get_data_by_unit_locality(*args, **kwargs),
        )

    def get_data_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._fetch_detail_endpoint(
            "data/metadata",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )

    async def aget_data_by_variable(
        self,
        variable_id: str,
        years: list[int] | None = None,
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
        return_metadata: bool = False,
    ) -> _DataCollectionResult:
        endpoint = f"data/by-variable/{variable_id}"
        params, headers = self._prepare_collection_request(
            extra_params=self._data_by_variable_params(
                years,
                unit_parent_id,
                unit_level,
                aggregate_id,
                page,
                extra_query,
            ),
            format=format,
            lang=lang,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
        return await self._afetch_data_collection(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            return_metadata=return_metadata,
        )

    async def aget_data_by_variable_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Asynchronously retrieve data by variable and always return `(results, metadata)`."""
        kwargs["return_metadata"] = True
        return cast(
            tuple[list[dict[str, Any]], dict[str, Any]],
            await self.aget_data_by_variable(*args, **kwargs),
        )

    async def aget_data_by_unit(
        self,
        unit_id: str,
        variable_ids: Sequence[str | int] | str | int | None = None,
        *,
        variable_id: Sequence[str | int] | str | int | None = None,
        years: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: bool = False,
    ) -> _DataCollectionResult:
        endpoint = f"data/by-unit/{unit_id}"
        params, headers = self._prepare_collection_request(
            extra_params=self._data_by_unit_params(variable_ids, variable_id, years, aggregate_id, page, extra_query),
            format=format,
            lang=lang,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
        return await self._afetch_single_page_data_collection(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            return_metadata=return_metadata,
        )

    async def aget_data_by_unit_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Asynchronously retrieve data by unit and always return `(results, metadata)`."""
        kwargs["return_metadata"] = True
        return cast(
            tuple[list[dict[str, Any]], dict[str, Any]],
            await self.aget_data_by_unit(*args, **kwargs),
        )

    async def aget_data_by_variable_locality(
        self,
        variable_id: str,
        unit_parent_id: str,
        years: list[int] | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: bool = False,
    ) -> _DataCollectionResult:
        endpoint = f"data/localities/by-variable/{variable_id}"
        params, headers = self._prepare_collection_request(
            extra_params=self._data_by_variable_locality_params(unit_parent_id, years, page, extra_query),
            format=format,
            lang=lang,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
        return await self._afetch_data_collection(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            return_metadata=return_metadata,
        )

    async def aget_data_by_variable_locality_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Asynchronously retrieve locality data by variable and always return `(results, metadata)`."""
        kwargs["return_metadata"] = True
        return cast(
            tuple[list[dict[str, Any]], dict[str, Any]],
            await self.aget_data_by_variable_locality(*args, **kwargs),
        )

    async def aget_data_by_unit_locality(
        self,
        unit_id: str,
        variable_ids: Sequence[str | int] | str | int | None = None,
        *,
        variable_id: Sequence[str | int] | str | int | None = None,
        years: list[int] | None = None,
        aggregate_id: int | None = None,
        page: int | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
        format: FormatLiteral | None = None,
        lang: LanguageLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
        return_metadata: bool = False,
    ) -> _DataCollectionResult:
        endpoint = f"data/localities/by-unit/{unit_id}"
        params, headers = self._prepare_collection_request(
            extra_params=self._data_by_unit_locality_params(
                variable_ids,
                variable_id,
                years,
                aggregate_id,
                page,
                extra_query,
            ),
            format=format,
            lang=lang,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
        return await self._afetch_data_collection(
            endpoint,
            params=params,
            headers=headers,
            page_size=page_size,
            max_pages=max_pages,
            return_metadata=return_metadata,
        )

    async def aget_data_by_unit_locality_with_metadata(
        self, *args: Any, **kwargs: Any
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Asynchronously retrieve locality data by unit and always return `(results, metadata)`."""
        kwargs["return_metadata"] = True
        return cast(
            tuple[list[dict[str, Any]], dict[str, Any]],
            await self.aget_data_by_unit_locality(*args, **kwargs),
        )

    async def aget_data_metadata(
        self,
        lang: LanguageLiteral | None = None,
        format: FormatLiteral | None = None,
        if_none_match: str | None = None,
        if_modified_since: str | None = None,
        extra_query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._afetch_detail_endpoint(
            "data/metadata",
            extra_params=extra_query,
            lang=lang,
            format=format,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
