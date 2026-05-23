"""TopCV-specific wrapper for the common description cleaner."""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_descritpion import (
    apply_description_cleaning,
)

# TopCV puts a "Mô tả công việc" header above the description block.
# ACCENTED pattern — cleaning runs before accent normalization.
_TOPCV_DESCRIPTION_NOISE = [r"(?i)mô tả công việc"]


def clean_topcv_description(df: pl.DataFrame) -> pl.DataFrame:
    """Add `job_description_cleaned` for TopCV rows."""
    return apply_description_cleaning(df, extra_noise_patterns=_TOPCV_DESCRIPTION_NOISE)