"""ITViec-specific wrapper for the common description cleaner."""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_descritpion import (
    apply_description_cleaning,
)

# ITViec puts a "Job description" header above the description block.
# ACCENTED pattern — cleaning runs before accent normalization.
_ITVIEC_DESCRIPTION_NOISE = [r"(?i)job description"]


def clean_itviec_description(df: pl.DataFrame) -> pl.DataFrame:
    """Add `job_description_cleaned` for ITViec rows."""
    return apply_description_cleaning(df, extra_noise_patterns=_ITVIEC_DESCRIPTION_NOISE)