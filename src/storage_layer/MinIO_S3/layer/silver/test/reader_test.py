from src.storage_layer.MinIO_S3.layer.silver.utils.reader import get_jobs_data_from_bronze
from src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_itviec.clean_benefit import (
    apply_benefit_cleaning,
    _ITVIEC_NOISE_PATTERNS,
)
import polars as pl

df = get_jobs_data_from_bronze("itviec", "jobs", "2026-05-14", "2026-05-14")

taxonomy_df = pl.read_csv(r"D:\Practice\Scrapy\Lakehouse-Lite\src\storage_layer\MinIO_S3\layer\silver\seeds\benefit_taxonomy.csv")

result = apply_benefit_cleaning(
    df.select("job_url", "benefits"),
    taxonomy_df,
    extra_noise_patterns=_ITVIEC_NOISE_PATTERNS,
)
print(result.filter(pl.col("benefits_categories_vi").list.len() == 0).select("job_url").sink_csv("ket_qua.csv"))