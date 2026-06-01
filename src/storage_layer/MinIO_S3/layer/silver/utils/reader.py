import polars as pl
from datetime import datetime, timedelta
from src.storage_layer.MinIO_S3.utils.minio_connect import get_s3_client
from src.storage_layer.MinIO_S3.config.path import BronzeBucketPaths, SilverBucketPaths
from src.storage_layer.MinIO_S3.config.key import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_storage_options() -> dict:
    # Polars/object_store reads region via "aws_region" (not "region_name").
    return {
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "aws_region": AWS_REGION,
    }

def get_jobs_data_from_bronze(site: str, entity_name: str, from_date: str, to_date: str) -> pl.LazyFrame | None:
    """
    Reads unprocessed job data from Bronze layer (MinIO S3) between from_date and to_date.
    Dates should be in format 'YYYY-MM-DD'.
    Returns a Polars LazyFrame or None if no paths exist.
    """
    s3_client = get_s3_client()
    storage_options = _get_storage_options()

    start = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")
    
    valid_s3_paths = []
    current_date = start
    while current_date <= end:
        year_str = current_date.strftime("%Y")
        month_str = current_date.strftime("%m")
        day_str = current_date.strftime("%d")
        
        bucket_paths = BronzeBucketPaths(site, entity_name, year_str, month_str, day_str)
        
        # Dùng boto3 list_objects để pre-check, tránh polars throw khi prefix rỗng
        prefix = bucket_paths.get_prefix()
        
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_paths.bronze_bucket_name, Prefix=prefix)
            if 'Contents' in response:
                s3_path = bucket_paths.get_files_path_json_gz()
                valid_s3_paths.append(s3_path)

        except Exception as e:
            logger.error(f"Error checking S3 prefix {prefix}: {e}")
            
        current_date += timedelta(days=1)

    if not valid_s3_paths:
        logger.info(f"No data found for site {site} between {from_date} and {to_date}")
        return None

    try:
        df_lazy = pl.scan_ndjson(
            valid_s3_paths,
            storage_options=storage_options,
            ignore_errors=True
        )
        return df_lazy
    except Exception as e:
        logger.error(f"Error reading from MinIO: {e}")
        return None


def get_jobs_silver_by_site(site: str, entity_name: str, from_date: str, to_date: str) -> pl.LazyFrame | None:
    """
    Reads processed job data from Silver layer (MinIO S3) between from_date and to_date.
    Dates should be in format 'YYYY-MM-DD'.

    Per-day re-runs of the cleaning pipeline append a new
    ``clean_bronze_<timestamp>.parquet`` under the same Hive partition, so a
    naive ``*.parquet`` scan would return stale snapshots alongside the latest
    one. For each day we therefore keep only the most recently written object
    (by ``LastModified``).

    ``hive_partitioning=True`` exposes the Hive-style partition columns encoded
    in the object key (``source_site``, ``year``, ``month``, ``day``) as real
    columns in the returned LazyFrame.

    Returns a Polars LazyFrame, or ``None`` if no Silver data exists in range.
    """
    s3_client = get_s3_client()
    storage_options = _get_storage_options()

    start = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")

    valid_s3_paths = []
    current_date = start
    while current_date <= end:
        year_str = current_date.strftime("%Y")
        month_str = current_date.strftime("%m")
        day_str = current_date.strftime("%d")

        bucket_paths = SilverBucketPaths(site, entity_name, year_str, month_str, day_str)
        prefix = bucket_paths.get_prefix()

        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_paths.silver_bucket_name, Prefix=prefix
            )

            parquet_objs = [
                obj for obj in response.get("Contents", [])
                if obj["Key"].endswith(".parquet")
            ]

            if parquet_objs:
                latest = max(parquet_objs, key=lambda obj: obj["LastModified"])
                latest_path = f"s3://{bucket_paths.silver_bucket_name}/{latest['Key']}"
                valid_s3_paths.append(latest_path)

        except Exception as e:
            logger.error(f"Error checking S3 prefix {prefix}: {e}")

        current_date += timedelta(days=1)

    if not valid_s3_paths:
        logger.info(f"No data found for site {site} between {from_date} and {to_date}")
        return None

    try:
        df_lazy = pl.scan_parquet(
            valid_s3_paths,
            storage_options=storage_options,
            hive_partitioning=True,
        )
        return df_lazy
    except Exception as e:
        logger.error(f"Error reading from MinIO: {e}")
        return None