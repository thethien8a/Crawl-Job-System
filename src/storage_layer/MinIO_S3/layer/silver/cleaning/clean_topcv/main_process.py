"""TopCV Silver cleaning pipeline.

Run:
    python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_topcv.main_process \
        --from_date 2026-05-20 --to_date 2026-05-23
"""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_topcv.clean_benefit import (
    clean_topcv_benefit,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_topcv.clean_description import (
    clean_topcv_description,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_topcv.clean_requirement import (
    clean_topcv_requirement,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_company_name import (
    main_clean_company,
)
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_company_size import (
    clean_company_size,
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
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_url import clean_job_url
from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import read_seeds
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.drop_cols import drop_unecessary_cols

def clean_topcv_jobs(df: pl.DataFrame) -> pl.DataFrame:
    """Apply every cleaner relevant to TopCVJobItem.

    Order matters for the company-name flow (NFC -> clean_2 -> clean_1 ->
    map_canonical) and for the column-dropping cleaners (location, industry):
    nothing downstream is allowed to reference `location` or `job_industry`
    after those calls.
    """
    df = drop_unecessary_cols(df)
    industry_taxonomy = read_seeds("industry_taxonomy.csv")
    df = clean_job_url(df, column_name="job_url")
    df = main_clean_company(df, column_name="company_name")

    df = process_job_title_pipeline(df, title_col="job_title")
    df = clean_location(df, column_name="location")
    df = apply_industry_cleaning(df, industry_taxonomy, sep=",")
    df = clean_topcv_description(df)
    df = clean_salary(df, column_name="salary")
    df = clean_topcv_benefit(df)
    df = clean_topcv_requirement(df)

    df = clean_company_size(df, column_name="company_size")
    df = clean_job_type(df, column_name="job_type")
    df = clean_exp_level(df, column_name="experience_level")
    df = clean_education_lv(df, column_name="education_level")
    df = clean_job_position(df, column_name="job_position")
    df = clean_job_deadline(df, column_name="job_deadline")
    return df


def main() -> None:
    main_for_site("topcv", clean_topcv_jobs, description="TopCV Silver cleaner")


if __name__ == "__main__":
    main()
