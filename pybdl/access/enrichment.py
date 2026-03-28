"""Reusable enrichment decorator for access layer DataFrames."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import wraps
from typing import Any

import pandas as pd

from pybdl.access.base import BaseAccess

EnrichmentLoader = Callable[[BaseAccess], Any]
AsyncEnrichmentLoader = Callable[[BaseAccess], Awaitable[Any]]


@dataclass(frozen=True)
class EnrichmentSpec:
    """Configuration for a single enrichment dimension."""

    flag: str
    id_column: str
    cache_key: str
    sync_loader: EnrichmentLoader
    async_loader: AsyncEnrichmentLoader
    rename_map: dict[str, str] = field(default_factory=dict)
    lookup_id_column: str = "id"


def _normalize_lookup_dataframe(access: BaseAccess, data: Any) -> pd.DataFrame:
    """Convert lookup payload to a normalized DataFrame."""
    if isinstance(data, dict):
        data = [data]
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df.columns = [access._camel_to_snake(col) for col in df.columns]
    df = access._infer_dtypes(df)
    return df


def _fetch_levels_sync(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "list_levels"):
        data = access.api_client.list_levels(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.levels import LevelsAPI

        data = LevelsAPI(access.api_client.config).list_levels(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


async def _fetch_levels_async(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "alist_levels"):
        data = await access.api_client.alist_levels(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.levels import LevelsAPI

        data = await LevelsAPI(access.api_client.config).alist_levels(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


def _fetch_measures_sync(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "list_measures"):
        data = access.api_client.list_measures(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.measures import MeasuresAPI

        data = MeasuresAPI(access.api_client.config).list_measures(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


async def _fetch_measures_async(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "alist_measures"):
        data = await access.api_client.alist_measures(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.measures import MeasuresAPI

        data = await MeasuresAPI(access.api_client.config).alist_measures(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


def _fetch_attributes_sync(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "list_attributes"):
        data = access.api_client.list_attributes(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.attributes import AttributesAPI

        data = AttributesAPI(access.api_client.config).list_attributes(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


async def _fetch_attributes_async(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "alist_attributes"):
        data = await access.api_client.alist_attributes(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.attributes import AttributesAPI

        data = await AttributesAPI(access.api_client.config).alist_attributes(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


def _fetch_units_sync(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "list_units"):
        data = access.api_client.list_units(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.units import UnitsAPI

        data = UnitsAPI(access.api_client.config).list_units(page_size=access._get_default_page_size(), max_pages=None)
    return _normalize_lookup_dataframe(access, data)


async def _fetch_units_async(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "alist_units"):
        data = await access.api_client.alist_units(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.units import UnitsAPI

        data = await UnitsAPI(access.api_client.config).alist_units(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


def _fetch_aggregates_sync(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "list_aggregates"):
        data = access.api_client.list_aggregates(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.aggregates import AggregatesAPI

        data = AggregatesAPI(access.api_client.config).list_aggregates(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


async def _fetch_aggregates_async(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "alist_aggregates"):
        data = await access.api_client.alist_aggregates(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.aggregates import AggregatesAPI

        data = await AggregatesAPI(access.api_client.config).alist_aggregates(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


def _fetch_subjects_sync(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "list_subjects"):
        data = access.api_client.list_subjects(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.subjects import SubjectsAPI

        data = SubjectsAPI(access.api_client.config).list_subjects(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


async def _fetch_subjects_async(access: BaseAccess) -> pd.DataFrame:
    if hasattr(access.api_client, "alist_subjects"):
        data = await access.api_client.alist_subjects(page_size=access._get_default_page_size(), max_pages=None)
    else:
        from pybdl.api.subjects import SubjectsAPI

        data = await SubjectsAPI(access.api_client.config).alist_subjects(
            page_size=access._get_default_page_size(), max_pages=None
        )
    return _normalize_lookup_dataframe(access, data)


LEVELS_SPEC = EnrichmentSpec(
    flag="enrich_levels",
    id_column="level",
    cache_key="levels",
    sync_loader=_fetch_levels_sync,
    async_loader=_fetch_levels_async,
    rename_map={"name": "level_name"},
)

MEASURES_SPEC = EnrichmentSpec(
    flag="enrich_measures",
    id_column="measure_unit_id",
    cache_key="measures",
    sync_loader=_fetch_measures_sync,
    async_loader=_fetch_measures_async,
    rename_map={"description": "measure_unit_description"},
)

ATTRIBUTES_SPEC = EnrichmentSpec(
    flag="enrich_attributes",
    id_column="attr_id",
    cache_key="attributes",
    sync_loader=_fetch_attributes_sync,
    async_loader=_fetch_attributes_async,
    rename_map={
        "name": "attr_name",
        "symbol": "attr_symbol",
        "description": "attr_description",
    },
)

UNITS_SPEC = EnrichmentSpec(
    flag="enrich_units",
    id_column="unit_id",
    cache_key="units",
    sync_loader=_fetch_units_sync,
    async_loader=_fetch_units_async,
    rename_map={
        "name": "unit_name_enriched",
        "level": "unit_level",
        "parent_id": "unit_parent_id",
        "kind": "unit_kind",
        "has_description": "unit_has_description",
    },
    lookup_id_column="id",
)

AGGREGATES_SPEC = EnrichmentSpec(
    flag="enrich_aggregates",
    id_column="aggregate_id",
    cache_key="aggregates",
    sync_loader=_fetch_aggregates_sync,
    async_loader=_fetch_aggregates_async,
    rename_map={
        "name": "aggregate_name",
        "description": "aggregate_description",
        "level": "aggregate_level",
    },
)

SUBJECTS_SPEC = EnrichmentSpec(
    flag="enrich_subjects",
    id_column="subject_id",
    cache_key="subjects",
    sync_loader=_fetch_subjects_sync,
    async_loader=_fetch_subjects_async,
    rename_map={"name": "subject_name"},
    lookup_id_column="id",
)


def _merge_enrichment(df: pd.DataFrame, lookup: pd.DataFrame, spec: EnrichmentSpec) -> pd.DataFrame:
    if df.empty or spec.id_column not in df.columns or lookup.empty:
        return df

    lookup_df = lookup.copy()
    # Retain only relevant columns
    keep_cols = {spec.lookup_id_column, *spec.rename_map.keys()}
    lookup_df = lookup_df[[col for col in lookup_df.columns if col in keep_cols]]
    lookup_df = lookup_df.rename(columns=spec.rename_map)

    # Avoid overwriting existing columns except for id column
    lookup_df = lookup_df[[col for col in lookup_df.columns if col == spec.lookup_id_column or col not in df.columns]]

    merged = df.merge(
        lookup_df,
        how="left",
        left_on=spec.id_column,
        right_on=spec.lookup_id_column,
    )
    if spec.lookup_id_column != spec.id_column and spec.lookup_id_column in merged.columns:
        merged = merged.drop(columns=[spec.lookup_id_column])
    return merged


def _split_result(result: Any) -> tuple[pd.DataFrame, Any]:
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], pd.DataFrame):
        return result[0], result[1]
    return result, None


def _recombine_result(df: pd.DataFrame, metadata: Any) -> Any:
    return (df, metadata) if metadata is not None else df


def _get_lookup(access: BaseAccess, spec: EnrichmentSpec) -> pd.DataFrame:
    cache = getattr(access, "_enrichment_cache", {})
    if spec.cache_key in cache:
        return cache[spec.cache_key]
    lookup_df = spec.sync_loader(access)
    cache[spec.cache_key] = lookup_df
    access._enrichment_cache = cache
    return lookup_df


async def _get_lookup_async(access: BaseAccess, spec: EnrichmentSpec) -> pd.DataFrame:
    cache = getattr(access, "_enrichment_cache", {})
    if spec.cache_key in cache:
        return cache[spec.cache_key]
    lookup_df = await spec.async_loader(access)
    cache[spec.cache_key] = lookup_df
    access._enrichment_cache = cache
    return lookup_df


def _normalize_requested_enrichments(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value}
    if isinstance(value, (list, tuple, set, frozenset)):
        normalized = {str(item) for item in value}
        return normalized
    raise TypeError("enrich must be a string or a collection of strings")


def _resolve_enrichment_flags(specs: tuple[EnrichmentSpec, ...], kwargs: dict[str, Any]) -> dict[str, bool]:
    requested = _normalize_requested_enrichments(kwargs.pop("enrich", None))
    flags: dict[str, bool] = {}
    for spec in specs:
        short_name = spec.flag.removeprefix("enrich_")
        flags[spec.flag] = bool(kwargs.pop(spec.flag, False) or short_name in requested or spec.flag in requested)
    return flags


def with_enrichment(*specs: EnrichmentSpec) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to enrich DataFrame results with reference metadata.

    Adds boolean flags (e.g., enrich_levels) to the wrapped function's kwargs,
    fetches lookup tables once per access instance, and left-joins enriched columns.
    """

    def _updated_signature(func: Callable[..., Any]) -> inspect.Signature:
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        existing = set(sig.parameters.keys())
        insert_at = next(
            (index for index, param in enumerate(params) if param.kind is inspect.Parameter.VAR_KEYWORD),
            len(params),
        )
        if "enrich" not in existing:
            params.insert(
                insert_at,
                inspect.Parameter(
                    "enrich",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=None,
                    annotation=list[str] | str | None,
                ),
            )
            insert_at += 1
        for spec in specs:
            if spec.flag in existing:
                continue
            params.insert(
                insert_at,
                inspect.Parameter(
                    spec.flag,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=False,
                    annotation=bool,
                ),
            )
            insert_at += 1
        return inspect.Signature(parameters=params, return_annotation=sig.return_annotation)

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(self: BaseAccess, *args: Any, **kwargs: Any) -> Any:
                flags = _resolve_enrichment_flags(specs, kwargs)
                result = await func(self, *args, **kwargs)
                df, metadata = _split_result(result)
                for spec in specs:
                    if flags.get(spec.flag):
                        lookup_df = await _get_lookup_async(self, spec)
                        df = _merge_enrichment(df, lookup_df, spec)
                return _recombine_result(df, metadata)

            async_wrapper.__signature__ = _updated_signature(func)  # type: ignore[attr-defined]
            return async_wrapper

        @wraps(func)
        def wrapper(self: BaseAccess, *args: Any, **kwargs: Any) -> Any:
            flags = _resolve_enrichment_flags(specs, kwargs)
            result = func(self, *args, **kwargs)
            df, metadata = _split_result(result)
            for spec in specs:
                if flags.get(spec.flag):
                    lookup_df = _get_lookup(self, spec)
                    df = _merge_enrichment(df, lookup_df, spec)
            return _recombine_result(df, metadata)

        wrapper.__signature__ = _updated_signature(func)  # type: ignore[attr-defined]
        return wrapper

    return decorator
