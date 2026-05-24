"""VietnamWorks Silver cleaning pipeline.

Run:
    python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_vnworks.main_process \
        --from_date 2026-05-20 --to_date 2026-05-23

Bronze site identifier is "vietnamworks" (matches the temp filename prefix in
src/crawl_layer/utils/loader.py), NOT the "vietnamworks.com" value that lives
inside the JSONL `source_site` field.
"""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_vnworks.clean_benefit import (
    clean_vnworks_benefit,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_vnworks.clean_description import (
    clean_vnworks_description,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_vnworks.clean_requirement import (
    clean_vnworks_requirement,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_company_name import (
    main_clean_company,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_education_lv import (
    clean_education_lv,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_exp_level import (
    clean_exp_level,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_deadline import (
    clean_job_deadline,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_industry import (
    apply_industry_cleaning,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_position import (
    clean_job_position,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_title import (
    process_job_title_pipeline,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_type import (
    clean_job_type,
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
from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import read_seeds

def clean_vnworks_jobs(df: pl.DataFrame) -> pl.DataFrame:
    """Apply cleaners relevant to VietnamWorksJobItem.

    VietnamWorks rows often have null `job_industry`, `job_type`, `*_level`,
    `job_position`, and `job_deadline`, but the columns still exist in the
    JSONL so every cleaner is safe to call. There is no `company_size` field.
    """
    industry_taxonomy = read_seeds("industry_taxonomy.csv")

    df = main_clean_company(df, column_name="company_name")

    df = process_job_title_pipeline(df, title_col="job_title")
    df = clean_location(df, column_name="location")
    df = apply_industry_cleaning(df, industry_taxonomy, sep=None)
    df = clean_vnworks_description(df)
    df = clean_salary(df, column_name="salary")
    df = clean_vnworks_benefit(df)
    df = clean_vnworks_requirement(df)

    df = clean_job_type(df, column_name="job_type")
    df = clean_exp_level(df, column_name="experience_level")
    df = clean_education_lv(df, column_name="education_level")
    df = clean_job_position(df, column_name="job_position")
    df = clean_job_deadline(df, column_name="job_deadline")
    return df


def main() -> None:
    main_for_site(
        "vietnamworks", clean_vnworks_jobs, description="VietnamWorks Silver cleaner"
    )


if __name__ == "__main__":
    main()
