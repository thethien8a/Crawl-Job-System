from __future__ import annotations
import polars as pl


def clean_job_deadline(
    df: pl.DataFrame,
    column_name: str = "job_deadline",
) -> pl.DataFrame:
    """
    Chuyển đổi cột job_deadline thành định dạng ngày thống nhất (YYYY-MM-DD).
    """
    # Ưu tiên trích xuất YYYY-MM-DD trước, sau đó đến DD/MM/YYYY
    yyyy_mm_dd = pl.col(column_name).str.extract(r'(\d{4}-\d{1,2}-\d{1,2})', 1)
    dd_mm_yyyy = pl.col(column_name).str.extract(r'(\d{1,2}/\d{1,2}/\d{4})', 1)

    # Chuyển đổi định dạng ngày
    yyyy_mm_dd_clean = yyyy_mm_dd.str.to_datetime("%Y-%m-%d", strict=False).dt.strftime("%Y-%m-%d")
    dd_mm_yyyy_clean = dd_mm_yyyy.str.to_datetime("%d/%m/%Y", strict=False).dt.strftime("%Y-%m-%d")

    # Kết hợp kết quả
    df = df.with_columns(
        pl.coalesce([yyyy_mm_dd_clean, dd_mm_yyyy_clean]).alias(column_name)
    )

    return df
