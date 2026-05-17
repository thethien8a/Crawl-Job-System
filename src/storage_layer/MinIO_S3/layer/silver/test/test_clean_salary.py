from pathlib import Path
import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_salary import clean_salary

FIXTURES_DIR = Path(__file__).parent / "fixtures"

df = pl.read_csv(FIXTURES_DIR / "output_jobs.csv")

# Apply the cleaning
df_cleaned = clean_salary(df)

print(df_cleaned.head(15))