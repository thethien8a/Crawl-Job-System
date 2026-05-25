"""ITViec Silver cleaning pipeline.

Run:
    python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_itviec.main_process \
        --from_date 2026-05-20 --to_date 2026-05-23
"""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_itviec.clean_benefit import (
    main as clean_itviec_benefit,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_itviec.clean_description import (
    clean_itviec_description,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_itviec.clean_requirement import (
    clean_itviec_requirement,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_company_name import (
    main_clean_company,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_company_size import (
    clean_company_size,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_industry import (
    apply_industry_cleaning,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_title import (
    process_job_title_pipeline,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_location import (
    clean_location,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_salary import (
    clean_salary,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.pipeline import (
    main_for_site,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_url import clean_job_url
from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import read_seeds
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.drop_cols import drop_unecessary_cols

def clean_itviec_jobs(df: pl.DataFrame) -> pl.DataFrame:
    """Apply cleaners relevant to ITViecJobItem.

    ITViec rows usually lack `job_type`, `experience_level`, `education_level`,
    `job_position`, and `job_deadline` entirely, so those cleaners are
    intentionally skipped — calling them would error out on a missing column
    even though the equivalent fields exist in TopCV / VietnamWorks rows.
    """
    df = drop_unecessary_cols(df)
    industry_taxonomy = read_seeds("industry_taxonomy.csv")

    df = main_clean_company(df, column_name="company_name")
    df = clean_job_url(df, column_name="job_url")
    df = process_job_title_pipeline(df, title_col="job_title")
    df = clean_location(df, column_name="location")
    
    # ITViec stores a single industry string per row (no separator).
    df = apply_industry_cleaning(df, industry_taxonomy, sep=None)
    df = clean_itviec_description(df)
    df = clean_salary(df, column_name="salary")
    # FlashText-based ITViec benefit cleaner; signature: (df, extra_noise_patterns=None).
    df = clean_itviec_benefit(df)
    df = clean_itviec_requirement(df)
    df = clean_company_size(df, column_name="company_size")
    
    return df


def main() -> None:
    main_for_site("itviec", clean_itviec_jobs, description="ITViec Silver cleaner")


if __name__ == "__main__":
    main()
