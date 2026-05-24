import polars as pl

UNECESSARY_COLS = [
    "scraped_at",
]

def drop_unecessary_cols(df: pl.DataFrame) -> pl.DataFrame:
    return df.drop(UNECESSARY_COLS)
