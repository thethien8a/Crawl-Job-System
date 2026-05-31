import json
import gzip
from datetime import datetime
from typing import Union, List, Dict
from src.crawl_layer.config.path import TEMP_DIR
from src.crawl_layer.utils.clean_temp import clean_temp_directory
from src.storage_layer.MinIO_S3.utils.minio_connect import get_s3_client
import logging
logger = logging.getLogger(__name__)

def save_to_temp(data: Union[Dict, List[Dict]], source_name: str, entity_name: str = 'jobs'):
    """
    Save the scraped data to the local temp directory as JSON Lines (.jsonl).
    The data will be automatically grouped by the current date.
    
    :param data: Data to save (dict or list of dicts).
    :param source_name: Source name (e.g., 'itviec', 'linkedin', 'topcv').
    :param entity_name: Entity type (e.g., 'jobs', 'companies'). Default is 'jobs'.
    """
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    
    # Get the current date (YYYYMMDD) to group files by date right from local
    today_str = datetime.now().strftime("%Y%m%d")

    # Format file name: source_entity_YYYYMMDD.jsonl (e.g., itviec_jobs_20260512.jsonl)
    file_name = f"{source_name}_{entity_name}_{today_str}.jsonl"
    file_path = TEMP_DIR / file_name
    
    # Ensure data is always a list for convenient loop processing
    if isinstance(data, dict):
        data = [data] # Lưu ý: Bỏ asdict() nếu data đã là dict, hoặc check kỹ type.
        
    # Open file in append mode to write new data to the end of the file
    with open(str(file_path), 'a', encoding='utf-8') as f:
        for item in data:
            # Write each dict as a JSON string on one line
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
            
    return file_path

def load_to_bronze(bucket_name: str = "bronze", source_filter: str | None = None):
    """
    Scan the temp directory, compress file into .gz and upload to MinIO according to the standard bronze architecture.
    The architecture is: bronze/<source_name>/<entity_name>/year=YYYY/month=MM/day=DD/<filename>.jsonl.gz

    :param source_filter: Only process files whose name starts with this source prefix
        (e.g. ``"topcv"``). When ``None``, all ``*.jsonl`` files are processed.
    """
    s3_client = get_s3_client()
    
    # Create bucket if it doesn't exist
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)

    if not TEMP_DIR.exists():
        logger.info(f"Directory {TEMP_DIR} does not exist. Skip.")
        return
    if not any(TEMP_DIR.iterdir()):
        logger.info("No file to load to minio")
        return

    # Get the timestamp to use it as a suffix for the file (to avoid overwriting old files on the same day)
    timestamp = datetime.now().strftime("%H%M%S")

    for file_path in TEMP_DIR.glob("*.jsonl"):
        # Parse file name: example 'itviec_jobs_20260512.jsonl'
        parts = file_path.stem.split('_')
        if len(parts) >= 3:
            source_name = parts[0]
            # Skip files that don't match the requested source filter
            if source_filter and source_name != source_filter:
                logger.debug("Skipping %s (source_filter=%s)", file_path.name, source_filter)
                continue
            entity_name = parts[1]
            date_str = parts[2] # 20260512
            
            year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
            
            # Compress .jsonl file into .jsonl.gz
            gz_file_path = file_path.with_suffix('.jsonl.gz')
            with open(file_path, 'rb') as f_in:
                with gzip.open(gz_file_path, 'wb') as f_out:
                    f_out.writelines(f_in)
                    
            # Create S3 Key according to the standard Data Lake architecture
            # Example: itviec/jobs/year=2026/month=05/day=12/itviec_jobs_20260512_170702.jsonl.gz
            s3_file_name = f"{source_name}_{entity_name}_{date_str}_{timestamp}.jsonl.gz"
            s3_key = f"{source_name}/{entity_name}/year={year}/month={month}/day={day}/{s3_file_name}"
            
            # Upload to MinIO
            logger.info(f"Uploading to MinIO: {s3_key}")
            s3_client.upload_file(str(gz_file_path), bucket_name, s3_key)
            
            # Delete the compressed file on local after successfully uploading all of them
            gz_file_path.unlink()
            
    # Clean up the files in the temp directory after successfully uploading all of them.
    # When a source_filter is given, only remove matching files so other sites
    # are left untouched.
    clean_temp_directory(prefix=source_filter)
    logger.info("Finish uploading data to MinIO and clean local temp directory!")