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

# Seed taxonomy is static within a process run, yet every cleaning step used to
# fetch it from Google Sheets afresh. The repeated reads exhaust the Sheets API
# per-minute quota and trigger the rate-limit fallback to local CSV mid-run.
# Memoize each file's first successful load so the API/disk is hit once per
# process, not once per cleaning step. The cached frame is treated as read-only;
# callers receive a clone so they cannot mutate the shared entry.
_seed_cache: dict[str, pl.DataFrame] = {}


def read_seeds(file_name_csv: str) -> pl.DataFrame:
    """Load seed data from Google Sheets first, then local CSV fallback.

    Results are memoized per ``file_name_csv`` for the process lifetime: the
    first successful load (Google Sheets or local CSV) is reused by every later
    caller, and a clone is returned so callers cannot mutate the shared cache.
    """
    cached = _seed_cache.get(file_name_csv)
    if cached is not None:
        return cached.clone()

    if google_sheets_config_is_available():
        try:
            df = read_worksheet_as_polars(worksheet_title_for_csv(file_name_csv))
        except GoogleSheetsError as exc:
            logger.warning("Failed to read %s from Google Sheets. Falling back to local CSV: %s", file_name_csv, exc)
            df = read_local_seed(file_name_csv)
    else:
        df = read_local_seed(file_name_csv)

    _seed_cache[file_name_csv] = df
    return df.clone()


def read_local_seed(file_name_csv: str) -> pl.DataFrame:
    path = SEEDS_DIR / file_name_csv
    return pl.read_csv(path)
