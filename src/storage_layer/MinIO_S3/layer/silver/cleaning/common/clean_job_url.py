import polars as pl

def clean_job_url(df: pl.DataFrame, column_name: str = "job_url") -> pl.DataFrame:
    return df.with_columns(
        pl.col(column_name)
        .str.replace(r"\?.*$", "")
        .alias(column_name)
    )   