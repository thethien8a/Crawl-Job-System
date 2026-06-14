import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import read_seeds
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import remove_vietnamese_accents


def load_location_taxonomy() -> pl.DataFrame:
    return read_seeds("location_mapping.csv")


def build_location_mapping_expr(taxonomy_df: pl.DataFrame, source_col: str = "location_no_accent") -> pl.Expr:
    expr = None
    for row in taxonomy_df.iter_rows(named=True):
        keywords = row.get("keywords", "")
        if not keywords:
            continue

        canonical_vi = row["canonical_vi"]
        pattern = f"(?i)({keywords})"

        condition = pl.col(source_col).str.contains(pattern)
        result = pl.lit(canonical_vi)

        if expr is None:
            expr = pl.when(condition).then(result)
        else:
            expr = expr.when(condition).then(result)

    if expr is None:
        return pl.lit(None)

    return expr.otherwise(pl.lit(None))


def clean_location(df: pl.DataFrame, column_name: str = "location", new_column_name: str = "clean_location") -> pl.DataFrame:
    taxonomy_df = load_location_taxonomy()

    temp_col = f"{column_name}_no_accent"

    df = df.with_columns(
        pl.col(column_name)
        .cast(pl.String)
        .map_elements(remove_vietnamese_accents, return_dtype=pl.String)
        .alias(temp_col)
    )

    mapping_expr = build_location_mapping_expr(taxonomy_df, source_col=temp_col)

    df = df.with_columns(
        mapping_expr.alias(new_column_name)
    )

    df = df.with_columns(
        pl.when((pl.col(new_column_name).is_null() & pl.col(temp_col).str.contains("(?i)vietnam")) | pl.col(new_column_name).is_not_null())
        .then(pl.lit("Việt Nam"))
        .when(pl.col(new_column_name).is_null() & pl.col(temp_col).str.contains("(?i)nuoc ngoai"))
        .then(pl.lit("Nước ngoài"))
        .otherwise(pl.col(new_column_name).fill_null("Không xác định"))
        .alias("is_vietnam")
    )

    df = df.with_columns(
        pl.col(new_column_name).fill_null("Không xác định").alias(new_column_name)
    )

    df = df.drop([temp_col])

    return df
