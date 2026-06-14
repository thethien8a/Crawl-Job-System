import yaml
import polars as pl
from pathlib import Path
import logging

from src.storage_layer.MinIO_S3.layer.silver.utils.google_sheets import (
    GoogleSheetsError,
    google_sheets_config_is_available,
    read_worksheet_as_polars,
    worksheet_title_for_csv,
)

logger = logging.getLogger(__name__)

def load_config_yaml(yaml_path):
    with open(yaml_path, "r", encoding="utf-8") as file:
        # Sử dụng safe_load để đảm bảo an toàn bảo mật
        config = yaml.safe_load(file)
    
    return config

SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"


def read_seeds(file_name_csv: str) -> pl.DataFrame:
    """Load seed data from Google Sheets first, then local CSV fallback."""
    if google_sheets_config_is_available():
        try:
            return read_worksheet_as_polars(worksheet_title_for_csv(file_name_csv))
        except GoogleSheetsError as exc:
            logger.warning("Failed to read %s from Google Sheets. Falling back to local CSV: %s", file_name_csv, exc)

    return read_local_seed(file_name_csv)


def read_local_seed(file_name_csv: str) -> pl.DataFrame:
    path = SEEDS_DIR / file_name_csv
    return pl.read_csv(path)
