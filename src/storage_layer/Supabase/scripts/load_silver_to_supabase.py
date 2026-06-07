"""Load Silver-layer job data from MinIO into Supabase Postgres.

Per-site flow:
    1. Lazily scan Silver Parquet for the requested date range.
    2. Project only the columns declared on `JobData`.
    3. Drop duplicate rows by `job_url` so the UPSERT touches each job once per run.
    4. Stream rows in `BATCH_SIZE` chunks and UPSERT them on `job_url`.

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
from src.storage_layer.MinIO_S3.layer.silver.utils.reader import get_jobs_silver_by_site

from .config import (
    BATCH_SIZE, 
    CONFLICT_KEY, 
    CREATE_TABLE_SQL,
    SILVER_ENTITY_NAME, 
    SITES, 
    JOB_DATA_COLUMNS, 
    UPSERT_SQL
)
from .connection_config import get_connection
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _load_site(conn, site: str, from_date: str, to_date: str) -> int:
    """Upsert one site's Silver slice into Supabase. Returns rows upserted."""
    lazy_df = get_jobs_silver_by_site(site, SILVER_ENTITY_NAME, from_date, to_date)
    if lazy_df is None:
        logger.info("No Silver data for %s in %s..%s", site, from_date, to_date)
        return 0

    # Silver layer produces both raw and cleaned columns (e.g. job_title + clean_job_title).
    # Map cleaned column names to the target Supabase column names so the frontend
    # receives cleaned values while the DB schema stays unchanged.
    _CLEANED_TO_TARGET = {
        "clean_job_title": "job_title",
        "clean_location": "location",
        "clean_company_name": "company_name",
    }

    select_exprs = []
    for col in JOB_DATA_COLUMNS:
        cleaned_src = next(
            (c for c, t in _CLEANED_TO_TARGET.items() if t == col), None
        )
        if cleaned_src:
            select_exprs.append(pl.col(cleaned_src).alias(col))
        else:
            select_exprs.append(pl.col(col))

    df = (
        lazy_df
        .select(select_exprs)
        .unique(subset=[CONFLICT_KEY], keep="any")
        .collect()
    )

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

