import polars as pl
from datetime import datetime, timedelta
from src.storage_layer.MinIO_S3.utils.minio_connect import get_s3_client
from src.storage_layer.MinIO_S3.config.path import BronzeBucketPaths, SilverBucketPaths
from src.storage_layer.MinIO_S3.config.key import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
import logging

logger = logging.getLogger(__name__)


def _get_storage_options() -> dict:
    return {
        "endpoint_url": MINIO_ENDPOINT,
        "aws_access_key_id": MINIO_ACCESS_KEY,
        "aws_secret_access_key": MINIO_SECRET_KEY,
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


def get_jobs_data_from_silver(
    site: str, entity_name: str = "jobs", from_date: str | None = None, to_date: str | None = None
) -> pl.LazyFrame | None:
    """
    Đọc Silver parquet từ MinIO. Khi from_date/to_date là None => quét toàn bộ partition của site
    (dùng cho fuzzy review định kỳ trên toàn bộ population). Khi có ngày => quét theo range để
    incremental processing.
    """
    storage_options = _get_storage_options()

    if from_date is None or to_date is None:
        # Wildcard toàn bộ partition; rely on s3 listing để biết prefix có data hay không
        bucket_paths = SilverBucketPaths(site, entity_name)
        if not _prefix_has_objects(bucket_paths.silver_bucket_name, bucket_paths.get_prefix()):
            logger.info(f"No silver data found for site {site}")
            return None
        return _scan_silver_parquet([bucket_paths.get_files_path_parquet()], storage_options)

    valid_s3_paths = _collect_silver_paths_by_date(site, entity_name, from_date, to_date)
    if not valid_s3_paths:
        logger.info(f"No silver data found for site {site} between {from_date} and {to_date}")
        return None
    return _scan_silver_parquet(valid_s3_paths, storage_options)


def _prefix_has_objects(bucket: str, prefix: str) -> bool:
    s3_client = get_s3_client()
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return 'Contents' in response
    except Exception as e:
        logger.error(f"Error checking S3 prefix {prefix} in bucket {bucket}: {e}")
        return False


def _collect_silver_paths_by_date(site: str, entity_name: str, from_date: str, to_date: str) -> list[str]:
    start = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")
    paths: list[str] = []
    current = start
    while current <= end:
        bucket_paths = SilverBucketPaths(
            site, entity_name, current.strftime("%Y"), current.strftime("%m"), current.strftime("%d")
        )
        if _prefix_has_objects(bucket_paths.silver_bucket_name, bucket_paths.get_prefix()):
            paths.append(bucket_paths.get_files_path_parquet())
        current += timedelta(days=1)
    return paths


def _scan_silver_parquet(paths: list[str], storage_options: dict) -> pl.LazyFrame | None:
    try:
        return pl.scan_parquet(paths, storage_options=storage_options)
    except Exception as e:
        logger.error(f"Error reading silver parquet from MinIO: {e}")
        return None