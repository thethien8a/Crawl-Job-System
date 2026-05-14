import os
import json
import logging
from datetime import datetime
from pathlib import Path
import polars as pl
from s3fs import S3FileSystem

# Setup logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("BronzeToSilver")

# S3 Configuration from environment variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")

# Storage options for Polars to connect to MinIO/S3
storage_options = {
    "endpoint_url": MINIO_ENDPOINT,
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "aws_region": "us-east-1", # MinIO usually ignores this but sometimes required by s3 client
}

def process_bronze_to_silver():
    """
    Reads data from Bronze (JSONL), applies Unified Schema, and saves to Silver (Parquet) 
    with partitioning by source, year, and month.
    """
    logger.info("Starting Bronze to Silver ETL...")
    
    # Initialize s3fs for listing files
    fs = S3FileSystem(
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY
    )
    
    # Bucket names
    bronze_bucket = "bronze"
    silver_bucket = "silver"
    
    # List all jsonl.gz files in Bronze bucket
    # Assuming bronze architecture: bronze/<source_name>/<entity_name>/year=YYYY/month=MM/day=DD/<filename>.jsonl.gz
    try:
        # We can use glob to find all jobs files
        files = fs.glob(f"{bronze_bucket}/**/jobs/**/*.jsonl.gz") + fs.glob(f"{bronze_bucket}/**/jobs/**/*.jsonl")
    except Exception as e:
        logger.error(f"Error accessing MinIO: {e}")
        return

    if not files:
        logger.info("No files found in Bronze layer for jobs.")
        return

    logger.info(f"Found {len(files)} files to process.")

    # Process all files into a single Polars DataFrame
    # Polars can read directly from S3 using the storage_options
    df_list = []
    
    # Define our Unified Schema (SilverJobItem fields)
    unified_schema = {
        "job_title": pl.Utf8,
        "company_name": pl.Utf8,
        "location": pl.Utf8,
        "job_industry": pl.Utf8,
        "job_description": pl.Utf8,
        "source_site": pl.Utf8,
        "job_url": pl.Utf8,
        "search_keyword": pl.Utf8,
        "scraped_at": pl.Utf8,
        "salary": pl.Utf8,
        "benefits": pl.Utf8,
        "requirements": pl.Utf8,
        "company_size": pl.Utf8,
        "job_type": pl.Utf8,
        "experience_level": pl.Utf8,
        "education_level": pl.Utf8,
        "job_position": pl.Utf8,
        "job_deadline": pl.Utf8
    }

    for file in files:
        s3_path = f"s3://{file}"
        logger.info(f"Reading {s3_path}")
        try:
            # Read JSONL file
            df = pl.read_ndjson(s3_path, storage_options=storage_options)
            
            # Align schema: Add missing columns with null values
            for col_name, col_type in unified_schema.items():
                if col_name not in df.columns:
                    df = df.with_columns(pl.lit(None).cast(col_type).alias(col_name))
            
            # Select only the columns in our unified schema to ensure order and cleanliness
            df = df.select(list(unified_schema.keys()))
            df_list.append(df)
        except Exception as e:
            logger.error(f"Error reading file {s3_path}: {e}")

    if not df_list:
        logger.info("No valid data to write.")
        return

    # Concatenate all DataFrames
    final_df = pl.concat(df_list, how="vertical")
    
    # Data Cleaning & Transformation
    logger.info("Transforming data...")
    
    # 1. Deduplication based on job_url (or combination of job_title + company_name + source_site)
    # Assuming job_url is unique per job posting
    final_df = final_df.unique(subset=["job_url"], keep="last")
    
    # 2. Extract Year and Month for partitioning based on scraped_at or a posted_date if available
    # Assuming scraped_at is in format like "YYYY-MM-DD..." or we can use current date as fallback
    # If your crawler saves 'scraped_at' in ISO format, we parse it:
    final_df = final_df.with_columns(
        pl.col("scraped_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).alias("parsed_date")
    )
    
    # Fallback to current date if parsing fails
    final_df = final_df.with_columns(
        pl.col("parsed_date").fill_null(datetime.now())
    )
    
    final_df = final_df.with_columns([
        pl.col("parsed_date").dt.year().cast(pl.Utf8).alias("year"),
        pl.col("parsed_date").dt.month().cast(pl.Utf8).str.pad_start(2, '0').alias("month")
    ])
    
    # Handle missing source_site for partitioning
    final_df = final_df.with_columns(
        pl.col("source_site").fill_null("unknown").str.to_lowercase()
    )

    # 3. Write partitioned Parquet files to Silver
    logger.info("Writing to Silver layer (Parquet partitioned by source/year/month)...")
    
    # Polars supports writing partitioned datasets directly (pyarrow under the hood)
    # But for writing to S3 partitioned, it's often easiest to use pyarrow dataset API directly, 
    # or iterate through partitions in Polars if pyarrow isn't fully configured for S3 partitioned writes.
    
    # We will use pyarrow dataset to write partitioned data to S3
    import pyarrow as pa
    import pyarrow.dataset as ds
    
    table = final_df.drop("parsed_date").to_arrow()
    
    s3_uri = f"s3://{silver_bucket}/jobs/"
    
    ds.write_dataset(
        data=table,
        base_dir=s3_uri,
        format="parquet",
        partitioning=["source_site", "year", "month"],
        partitioning_flavor="hive", # Gives us source_site=itviec/year=2026/month=05
        existing_data_behavior="overwrite_or_ignore", # Handle existing data
        file_system=fs
    )
    
    logger.info(f"Successfully wrote {len(final_df)} records to Silver layer: {s3_uri}")

if __name__ == "__main__":
    process_bronze_to_silver()
