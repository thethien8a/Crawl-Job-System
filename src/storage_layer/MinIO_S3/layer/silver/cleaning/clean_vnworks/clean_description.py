"""VietnamWorks-specific wrapper for the common description cleaner."""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_descritpion import (
    apply_description_cleaning,
)

# VietnamWorks has no site-specific description noise — the common cleaner
# handles HTML/whitespace stripping, which is sufficient.


def clean_vnworks_description(df: pl.DataFrame) -> pl.DataFrame:
    """Add `job_description_cleaned` for VietnamWorks rows."""
    return apply_description_cleaning(df)