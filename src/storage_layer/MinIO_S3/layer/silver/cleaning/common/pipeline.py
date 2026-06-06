"""Shared CLI runner for Silver-layer site pipelines.

Each site (TopCV, ITViec, VietnamWorks) provides a `clean_<site>_jobs(df)`
function and delegates argparse + Bronze read + Parquet write here so the
three site-specific `main_process.py` modules stay tiny and focused on the
cleaning DAG that is genuinely different per source.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

import polars as pl

from src.storage_layer.MinIO_S3.config.path import DEFAULT_ENTITY_NAME
from src.storage_layer.MinIO_S3.layer.silver.data_model.data_class import (
    silver_schema_to_polars,
)
from src.storage_layer.MinIO_S3.layer.silver.utils.loader import (
    upload_silver_parquet,
)
from src.storage_layer.MinIO_S3.layer.silver.utils.reader import (
    get_jobs_data_from_bronze,
)

logger = logging.getLogger(__name__)

# Silver schema derived from SilverJobItem dataclass -- computed once at import.
SILVER_SCHEMA = silver_schema_to_polars()


def enforce_silver_schema(
    df: pl.DataFrame,
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    """Conform a cleaned DataFrame to the SilverJobItem Polars schema.

    - Casts existing columns to target dtype (uncastable values become null).
    - Adds missing columns with null defaults of the correct dtype.
    - Drops columns not defined in the schema.
    - Returns columns in schema definition order.

    Polars does not support String -> Boolean cast directly, so Boolean
    columns originating from string data go through Int64 first (uncastable
    strings become null, then null stays null through the Boolean cast).
    """
    existing = set(df.columns)
    exprs = []
    for col, dtype in schema.items():
        if col in existing:
            src_dtype = df.schema[col]
            # String -> Boolean is unsupported in Polars; bridge via Int64.
            if dtype == pl.Boolean and src_dtype == pl.String():
                exprs.append(
                    pl.col(col).cast(pl.Int64, strict=False).cast(pl.Boolean)
                )
            else:
                exprs.append(pl.col(col).cast(dtype, strict=False))
        else:
            exprs.append(pl.lit(None).cast(dtype).alias(col))
    return df.select(exprs)


# Columns that must be non-null for a row to be worth processing.
# Rows missing any of these are dropped before cleaning starts.
ESSENTIAL_COLUMNS = ("job_title", "company_name")


def filter_essential_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Drop rows where any *essential* column is null.

    A job without a title or company name is not useful downstream, so we
    discard it early to avoid wasting cleaning work on garbage rows.
    """
    before = df.height
    # Only filter on columns that actually exist in the DataFrame --
    # some sites may not carry every essential column yet.
    cols_present = [c for c in ESSENTIAL_COLUMNS if c in df.columns]
    if cols_present:
        df = df.filter(~pl.any_horizontal(pl.col(c).is_null() for c in cols_present))
    after = df.height
    dropped = before - after
    if dropped:
        logger.info(
            "filter_essential_rows: dropped %d/%d rows missing %s",
            dropped, before, cols_present,
        )
    return df


# Local directory for CSV debug dumps; created on demand.
SILVER_DEBUG_DIR = Path(__file__).parents[2] / "debug_output"

def build_argument_parser(site: str) -> argparse.ArgumentParser:
    """Standard CLI shape: --from_date / --to_date / --entity_name / --no_save / --export_parquet."""
    parser = argparse.ArgumentParser(description=f"Silver cleaner for {site}")
    parser.add_argument("--from_date", required=True, help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--to_date", required=True, help="Inclusive end date YYYY-MM-DD")
    parser.add_argument("--entity_name", default=DEFAULT_ENTITY_NAME)
    parser.add_argument(
        "--no_save",
        action="store_true",
        help="Skip writing parquet (useful for dry runs / interactive debugging).",
    )
    parser.add_argument(
        "--export_parquet",
        action="store_true",
        help=(
            "Dump each day's cleaned DataFrame to a local parquet file "
            f"(written to {SILVER_DEBUG_DIR}/) for visual debugging. "
            "Filename pattern: <site>_cleaned_<date>.parquet"
        ),
    )
    return parser

def run_pipeline(
    site: str,
    clean_fn: Callable[[pl.DataFrame], pl.DataFrame],
    args: argparse.Namespace,
) -> None:
    """Read Bronze day-by-day -> clean -> upload to MinIO Silver.

    Iterates through each date from from_date to to_date, processing data for
    that specific day.
    """
    start_dt = datetime.strptime(args.from_date, "%Y-%m-%d")
    end_dt = datetime.strptime(args.to_date, "%Y-%m-%d")
    
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        logger.info("Processing Silver for %s: %s", site, date_str)
        
        # Read only for the specific day
        lazy = get_jobs_data_from_bronze(
            site=site,
            entity_name=args.entity_name,
            from_date=date_str,
            to_date=date_str,
        )
        
        if lazy is None:
            logger.info("No Bronze data for %s on %s", site, date_str)
            current_dt += timedelta(days=1)
            continue
            
        df = lazy.collect()
        df = filter_essential_rows(df)
        cleaned = clean_fn(df)
        cleaned = enforce_silver_schema(cleaned, SILVER_SCHEMA)

        if args.export_parquet:
            SILVER_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
            parquet_name = f"{site}_cleaned_{date_str}.parquet"
            parquet_path = SILVER_DEBUG_DIR / parquet_name
            cleaned.write_parquet(parquet_path)
            logger.info("Exported cleaned parquet: %s (%d rows)", parquet_path, cleaned.height)

        if not args.no_save:
            s3_key = upload_silver_parquet(
                df=cleaned,
                entity_name=args.entity_name,
                source_site=site,
                date_str=date_str,
            )
            logger.info("Uploaded cleaned data to silver bucket: %s", s3_key)
        else:
            logger.info("Dry run: cleaned %d rows for %s", cleaned.height, date_str)
            
        current_dt += timedelta(days=1)


def main_for_site(
    site: str,
    clean_fn: Callable[[pl.DataFrame], pl.DataFrame],
    description: str | None = None,
) -> None:
    """Entry-point glue: same logging format as the crawler CLI for consistency."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = build_argument_parser(site)
    if description:
        parser.description = description
    args = parser.parse_args()
    run_pipeline(site, clean_fn, args)
