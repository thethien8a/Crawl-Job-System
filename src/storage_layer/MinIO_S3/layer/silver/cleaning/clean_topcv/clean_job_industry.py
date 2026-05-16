import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_job_industry import apply_industry_cleaning

def clean_topcv_job_industry(df: pl.DataFrame, taxonomy_df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the job_industry column specifically for TopCV data.
    TopCV industries are comma-separated strings (e.g., 'Kế toán, Marketing').
    
    Returns a DataFrame with 2 new columns:
    - job_industry_clean: List[String]
    - job_industry_unmapped: List[String]
    """
    return apply_industry_cleaning(df, taxonomy_df, sep=",")
