import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import remove_vietnamese_accents

def build_industry_mapping_expr(taxonomy_df: pl.DataFrame, source_col: str = "industry_split_norm") -> pl.Expr:
    """
    Builds a flat when-then expression for Polars based on the taxonomy dataframe.
    """
    expr = None
    for row in taxonomy_df.iter_rows(named=True):
        keywords = row.get("keywords", "")
        if not keywords:
            continue
            
        canonical_en = row["canonical_en"]
        # Create regex pattern. Keywords are separated by |
        pattern = f"(?i)({keywords})"
        
        condition = pl.col(source_col).str.contains(pattern)
        result = pl.lit(canonical_en)
        
        if expr is None:
            expr = pl.when(condition).then(result)
        else:
            expr = expr.when(condition).then(result)
            
    if expr is None:
        return pl.lit(None)
        
    return expr.otherwise(pl.lit(None))

def apply_industry_cleaning(df: pl.DataFrame, taxonomy_df: pl.DataFrame, sep: str = None) -> pl.DataFrame:
    """
    Generic function to clean job_industry.
    - If sep is provided, splits the string (e.g., ',' for TopCV).
    - Otherwise, treats the whole string as a single element.
    Outputs: job_industry_clean (List[String]), job_industry_unmapped (List[String])
    """
    # Ensure we have a temporary row index to group back correctly
    df = df.with_row_index("temp_row_idx")
    
    # 1. Split & Explode
    if sep:
        df_exploded = df.with_columns(
            pl.col("job_industry").cast(pl.String).str.split(sep).alias("industry_split")
        )
    else:
        # Wrap single string into list to explode uniformly
        df_exploded = df.with_columns(
            pl.col("job_industry").cast(pl.String).map_elements(lambda x: [x] if x else [], return_dtype=pl.List(pl.String)).alias("industry_split")
        )
        
    # Trim spaces and filter out nulls/empties
    df_exploded = df_exploded.explode("industry_split").with_columns(
        pl.col("industry_split").str.strip_chars()
    ).filter(
        pl.col("industry_split").is_not_null() & (pl.col("industry_split") != "")
    )

    # 2. Normalize and Map
    # Create normalized column for matching: remove accents, lowercase
    df_mapped = df_exploded.with_columns(
        pl.col("industry_split").map_elements(remove_vietnamese_accents, return_dtype=pl.String).str.to_lowercase().alias("industry_split_norm")
    )
    
    mapping_expr = build_industry_mapping_expr(taxonomy_df, "industry_split_norm")
    
    df_mapped = df_mapped.with_columns(
        mapping_expr.alias("mapped_industry")
    )
    
    # 3. Handle unmapped and group back
    df_handled = df_mapped.with_columns(
        pl.when(pl.col("mapped_industry").is_null())
          .then(pl.col("industry_split"))
          .otherwise(pl.lit(None))
          .alias("unmapped_element"),
        pl.col("mapped_industry").fill_null("Others").alias("clean_industry_element"),
    )
    
    df_cleaned = df_handled.group_by("temp_row_idx").agg(
        pl.col("clean_industry_element").drop_nulls().unique().alias("job_industry_clean"),
        pl.col("unmapped_element").drop_nulls().unique().alias("job_industry_unmapped")
    )
    
    # 4. Join back to original
    df_final = df.join(df_cleaned, on="temp_row_idx", how="left").drop(["temp_row_idx"])
    
    # For rows that had completely null/empty job_industry, fill with empty lists to maintain schema
    empty_list_expr = pl.Series([[]], dtype=pl.List(pl.String))
    df_final = df_final.with_columns(
        pl.when(pl.col("job_industry_clean").is_null()).then(empty_list_expr).otherwise(pl.col("job_industry_clean")).alias("job_industry_clean"),
        pl.when(pl.col("job_industry_unmapped").is_null()).then(empty_list_expr).otherwise(pl.col("job_industry_unmapped")).alias("job_industry_unmapped")
    )
    
    return df_final