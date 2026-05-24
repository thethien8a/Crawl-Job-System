"""Utilities for loading cleaned data into the Silver layer (MinIO S3)."""
from __future__ import annotations

import io
from datetime import datetime

import polars as pl
from src.storage_layer.MinIO_S3.config.path import SilverBucketPaths
from src.storage_layer.MinIO_S3.utils.minio_connect import get_s3_client

def upload_silver_parquet(
    df: pl.DataFrame,
    entity_name: str,
    source_site: str,
    date_str: str,
) -> str:
    """Upload a Polars DataFrame as Parquet to MinIO Silver bucket.

    Path structure: {entity_name}/source_site={source_site}/year=YYYY/month=MM/day=DD/clean_/clean_bronze_{timestamp}.parquet
    Bucket name is resolved from bucket.yml via SilverBucketPaths.
    """
    # Parse date for directory structure
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Delegate path + bucket resolution to centralized SilverBucketPaths
    silver_paths = SilverBucketPaths(
        source_site=source_site,
        entity_name=entity_name,
        year=year,
        month=month,
        day=day,
    )
    s3_key = silver_paths.get_file_key(timestamp)
    bucket = silver_paths.silver_bucket_name

    # Write Parquet to an in-memory buffer
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    # Upload to S3
    s3 = get_s3_client()

    # Create bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue()
    )

    return s3_key
