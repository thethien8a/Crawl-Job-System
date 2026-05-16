from __future__ import annotations
import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import remove_accents_col

def clean_education_lv(df: pl.DataFrame, column_name: str = "education_level") -> pl.DataFrame:
    """
    Chuẩn hóa education_level thành: Cao học, Đại học, Cao đẳng, Trung cấp / Nghề, Trung học phổ thông, Trung học cơ sở, Không đề cập.
    """

    df = remove_accents_col(df, column_name, "education_level_no_accent")
    t = pl.col("education_level_no_accent").str.to_lowercase()

    df = df.with_columns(
        pl.when(t.str.contains(r"(?i)(cao hoc|thac si|master|tien si|doctor|prof)"))
        .then(pl.lit("Cao học"))
        .when(t.str.contains(r"(?i)(dai hoc|bachelor|cu nhan)"))
        .then(pl.lit("Đại học"))
        .when(t.str.contains(r"(?i)(cao dang|associate)"))
        .then(pl.lit("Cao đẳng"))
        .when(t.str.contains(r"(?i)(trung cap|nghe|vocational)"))
        .then(pl.lit("Trung cấp / Nghề"))
        .when(t.str.contains(r"(?i)(thpt|pho thong|cap 3|high school)"))
        .then(pl.lit("Trung học phổ thông"))
        .when(t.str.contains(r"(?i)(co so|cap 2)"))
        .then(pl.lit("Trung học cơ sở"))
        .when(t.str.contains(r"(?i)(khong gioi han|khong yeu cau)"))
        .then(pl.lit("Không giới hạn"))
        .otherwise(pl.lit("Không đề cập"))
        .alias("education_level")
    )

    df = df.drop("education_level_no_accent")
    return df