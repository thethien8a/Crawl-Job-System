"""Load Silver-layer job data from MinIO into Supabase Postgres.

Per-site flow:
    1. Lazily scan Silver Parquet for the requested date range.
    2. Project only the columns declared on `JobData`.
    3. Drop duplicate rows by the configured conflict key so each logical job
       is upserted once per run.
    4. Stream rows in `BATCH_SIZE` chunks and UPSERT them on the conflict key.

Per-site commits give partial progress on failure: a broken site rolls back
its own batch but does not block the remaining sites.

Run:
    python -m src.storage_layer.Supabase.scripts.load_silver_to_supabase \
        --from_date 2025-01-01 --to_date 2025-01-07
"""

import argparse
import logging
from contextlib import closing

from psycopg2.extras import execute_values
import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_url import (
    build_unique_url_expr,
    clean_url_expr,
)
from src.storage_layer.MinIO_S3.layer.silver.utils.reader import get_jobs_silver_by_site

from .config import (
    BATCH_SIZE, 
    CONFLICT_KEY, 
    CREATE_TABLE_SQL,
    SCHEMA_MIGRATION_SQLS,
    SILVER_ENTITY_NAME, 
    SITES, 
    JOB_DATA_COLUMNS, 
    UPSERT_SQL
)
from .connection_config import get_connection
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_CLEANED_TO_TARGET = {
    "clean_job_title": "job_title",
    "clean_location": "location",
    "clean_company_name": "company_name",
}

_FRESHNESS_COLUMNS = (
    "year",
    "month",
    "day",
)
_DEDUP_SORT_PREFIX = "__dedup_sort_"


def _load_site(conn, site: str, from_date: str, to_date: str) -> int:
    """Upsert one site's Silver slice into Supabase. Returns rows upserted."""
    lazy_df = get_jobs_silver_by_site(site, SILVER_ENTITY_NAME, from_date, to_date)
    if lazy_df is None:
        logger.info("No Silver data for %s in %s..%s", site, from_date, to_date)
        return 0

    available_columns = set(lazy_df.collect_schema().names())
    select_exprs = _job_data_select_exprs(available_columns, site)
    df = _deduplicate_latest_jobs(lazy_df, select_exprs, available_columns)

    if df.is_empty():
        logger.info("Silver returned 0 rows for %s after dedup", site)
        return 0

    upserted = 0
    with conn.cursor() as cursor:
        for chunk in df.iter_slices(BATCH_SIZE):
            execute_values(cursor, UPSERT_SQL, chunk.rows(), page_size=BATCH_SIZE)
            upserted += chunk.height
            logger.debug("Currently upserted %d rows for %s", upserted, site)
    conn.commit()
    logger.info("Successfully upserted %d rows for %s", upserted, site)
    return upserted


def _job_data_select_exprs(available_columns: set[str], site: str) -> list[pl.Expr]:
    select_exprs = []
    for col in JOB_DATA_COLUMNS:
        cleaned_src = next(
            (c for c, t in _CLEANED_TO_TARGET.items() if t == col), None
        )
        if cleaned_src:
            select_exprs.append(pl.col(cleaned_src).alias(col))
        elif col == CONFLICT_KEY:
            select_exprs.append(_unique_url_select_expr(available_columns, site))
        else:
            select_exprs.append(pl.col(col))
    return select_exprs


def _deduplicate_latest_jobs(
    lazy_df: pl.LazyFrame,
    select_exprs: list[pl.Expr],
    available_columns: set[str],
) -> pl.DataFrame:
    sort_column_aliases = _dedup_sort_column_aliases(available_columns)
    projected_exprs = [
        *select_exprs,
        *(
            pl.col(column).alias(alias)
            for column, alias in sort_column_aliases
        ),
    ]

    filtered = (
        lazy_df
        .select(projected_exprs)
        .filter(
            pl.col(CONFLICT_KEY).is_not_null()
            & (pl.col(CONFLICT_KEY).str.strip_chars() != "")
        )
    )

    if sort_column_aliases:
        filtered = filtered.sort(
            [alias for _, alias in sort_column_aliases],
            descending=True,
            nulls_last=True,
        )

    return (
        filtered
        .unique(subset=[CONFLICT_KEY], keep="first", maintain_order=True)
        .select(JOB_DATA_COLUMNS)
        .collect()
    )


def _dedup_sort_column_aliases(available_columns: set[str]) -> list[tuple[str, str]]:
    return [
        (column, f"{_DEDUP_SORT_PREFIX}{column}")
        for column in _FRESHNESS_COLUMNS
        if column in available_columns
    ]


def _unique_url_select_expr(available_columns: set[str], site: str) -> pl.Expr:
    cleaned_url = clean_url_expr(pl.col("job_url"))
    source_site = pl.lit(site.lower().strip())
    derived_unique_url = build_unique_url_expr(cleaned_url, source_site)

    if CONFLICT_KEY not in available_columns:
        return derived_unique_url.alias(CONFLICT_KEY)

    existing_unique_url = pl.col(CONFLICT_KEY).str.strip_chars()
    return (
        pl.when(existing_unique_url.is_null() | (existing_unique_url == ""))
        .then(derived_unique_url)
        .otherwise(existing_unique_url)
        .alias(CONFLICT_KEY)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from_date", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--to_date", required=True, help="Inclusive YYYY-MM-DD")
    args = parser.parse_args()

    sites_to_load = SITES
    grand_total = 0
    with closing(get_connection()) as conn:
        # Ensure target table exists (idempotent)
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            for sql in SCHEMA_MIGRATION_SQLS:
                cur.execute(sql)
        conn.commit()
        
        for site in sites_to_load:
            try:
                grand_total += _load_site(conn, site, args.from_date, args.to_date)
            except Exception:
                # Rollback the aborted transaction so the next site starts clean.
                conn.rollback()
                logger.exception("Failed loading site %s; skipping", site)

    logger.info("Total upserted across all sites: %d", grand_total)


if __name__ == "__main__":
    main()

