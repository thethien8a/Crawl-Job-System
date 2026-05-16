from __future__ import annotations
import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import remove_accents_col

def clean_job_type(df: pl.DataFrame, column_name: str = "job_type") -> pl.DataFrame:
    """
    Chuẩn hóa job_type thành: Toàn thời gian, Bán thời gian, Khác.
    """

    df = remove_accents_col(df, column_name, "job_type_no_accent")
    t = pl.col("job_type_no_accent").str.to_lowercase()

    df = df.with_columns(
        pl.when(t.str.contains(r"(?i)(part|ban thoi gian)"))
        .then(pl.lit("Bán thời gian"))
        .when(t.str.contains(r"(?i)(full|toan thoi gian)"))
        .then(pl.lit("Toàn thời gian"))
        .otherwise(pl.lit("Khác"))
        .alias("job_type")
    )

    df = df.drop("job_type_no_accent")
    return df
