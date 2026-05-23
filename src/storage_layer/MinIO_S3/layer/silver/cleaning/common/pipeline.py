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
from typing import Callable

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.utils.loader import (
    upload_silver_parquet,
)
from src.storage_layer.MinIO_S3.layer.silver.utils.reader import (
    get_jobs_data_from_bronze,
)

logger = logging.getLogger(__name__)



def build_argument_parser(site: str) -> argparse.ArgumentParser:
    """Standard CLI shape: --from_date / --to_date / --entity_name / --no_save."""
    parser = argparse.ArgumentParser(description=f"Silver cleaner for {site}")
    parser.add_argument("--from_date", required=True, help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--to_date", required=True, help="Inclusive end date YYYY-MM-DD")
    parser.add_argument("--entity_name", default="jobs")
    parser.add_argument(
        "--no_save",
        action="store_true",
        help="Skip writing parquet (useful for dry runs / interactive debugging).",
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
        cleaned = clean_fn(df)
        
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
