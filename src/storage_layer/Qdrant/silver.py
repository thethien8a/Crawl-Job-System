from __future__ import annotations

import logging

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_url import (
    build_unique_url_expr,
    clean_url_expr,
)
from src.storage_layer.MinIO_S3.layer.silver.utils.reader import get_jobs_silver_by_site
from src.storage_layer.Qdrant.config import SILVER_ENTITY_NAME
from src.storage_layer.Qdrant.schema import (
    DateRange,
    DOCUMENT_TEXT_FIELDS,
    JOB_URL_FIELD,
    LIST_PAYLOAD_FIELDS,
    SCALAR_PAYLOAD_FIELDS,
    SILVER_DATE_FIELD,
    SKILL_FIELDS,
    UNIQUE_URL_FIELD,
)

logger = logging.getLogger(__name__)


def load_latest_silver_jobs(site: str, date_range: DateRange) -> pl.DataFrame | None:
    lazy_df = get_jobs_silver_by_site(
        site,
        SILVER_ENTITY_NAME,
        date_range.from_date,
        date_range.to_date,
    )
    if lazy_df is None:
        logger.info("No Silver data for %s in %s..%s", site, date_range.from_date, date_range.to_date)
        return None

    available_columns = set(lazy_df.collect_schema().names())
    return _deduplicate_latest_jobs(lazy_df, available_columns, site)


def _deduplicate_latest_jobs(
    lazy_df: pl.LazyFrame,
    available_columns: set[str],
    site: str,
) -> pl.DataFrame:
    return (
        lazy_df
        .select(_select_exprs(available_columns, site))
        .filter(
            pl.col(UNIQUE_URL_FIELD).is_not_null()
            & (pl.col(UNIQUE_URL_FIELD).str.strip_chars() != "")
        )
        .sort([SILVER_DATE_FIELD, "job_deadline"], descending=True, nulls_last=True)
        .unique(subset=[UNIQUE_URL_FIELD], keep="first", maintain_order=True)
        .collect()
    )


def _select_exprs(available_columns: set[str], site: str) -> list[pl.Expr]:
    exprs = [
        _unique_url_expr(available_columns, site),
        _optional_column(available_columns, JOB_URL_FIELD),
        _coalesce_columns(available_columns, ("clean_job_title", "job_title"), "job_title"),
        _coalesce_columns(available_columns, ("clean_company_name", "company_name"), "company_name"),
        _coalesce_columns(available_columns, ("location", "clean_location"), "location"),
        _coalesce_columns(available_columns, ("clean_location", "location"), "clean_location"),
        _optional_column(available_columns, "job_deadline"),
        _optional_column(available_columns, "min_exp_level"),
        _optional_column(available_columns, "max_exp_level"),
        pl.lit(site).alias("source_site"),
        _silver_date_expr(available_columns),
    ]

    exprs.extend(_optional_column(available_columns, field) for field in SKILL_FIELDS)
    exprs.extend(_optional_column(available_columns, field) for field in DOCUMENT_TEXT_FIELDS)
    exprs.extend(_optional_column(available_columns, field) for field in LIST_PAYLOAD_FIELDS)
    exprs.extend(_optional_column(available_columns, field) for field in SCALAR_PAYLOAD_FIELDS)
    return exprs


def _optional_column(available_columns: set[str], column_name: str) -> pl.Expr:
    if column_name in available_columns:
        return pl.col(column_name).alias(column_name)

    return pl.lit(None).alias(column_name)


def _coalesce_columns(
    available_columns: set[str],
    candidates: tuple[str, ...],
    alias: str,
) -> pl.Expr:
    exprs = [pl.col(column) for column in candidates if column in available_columns]
    if not exprs:
        return pl.lit(None).alias(alias)

    return pl.coalesce(exprs).alias(alias)


def _unique_url_expr(available_columns: set[str], site: str) -> pl.Expr:
    cleaned_url = (
        clean_url_expr(pl.col(JOB_URL_FIELD))
        if JOB_URL_FIELD in available_columns
        else pl.lit(None, dtype=pl.String)
    )
    derived_unique_url = build_unique_url_expr(cleaned_url, pl.lit(site.lower().strip()))

    if UNIQUE_URL_FIELD not in available_columns:
        return derived_unique_url.alias(UNIQUE_URL_FIELD)

    existing_unique_url = pl.col(UNIQUE_URL_FIELD).str.strip_chars()
    return (
        pl.when(existing_unique_url.is_null() | (existing_unique_url == ""))
        .then(derived_unique_url)
        .otherwise(existing_unique_url)
        .alias(UNIQUE_URL_FIELD)
    )


def _silver_date_expr(available_columns: set[str]) -> pl.Expr:
    if not {"year", "month", "day"}.issubset(available_columns):
        return pl.lit(None, dtype=pl.Date).alias(SILVER_DATE_FIELD)

    return pl.date(
        pl.col("year").cast(pl.Int32),
        pl.col("month").cast(pl.Int32),
        pl.col("day").cast(pl.Int32),
    ).alias(SILVER_DATE_FIELD)
