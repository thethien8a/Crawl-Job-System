"""ITViec-specific wrapper for the common requirements cleaner."""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_requirement import (
    main_clean_requirement,
)

# ITViec wraps every requirements block with an English "Your skills and
# experience" hero — pure copy, no signal for taxonomy matching.
_ITVIEC_REQUIREMENT_NOISE = [r"(?i)your skills and experience"]


def clean_itviec_requirement(df: pl.DataFrame) -> pl.DataFrame:
    """Add `requirements_cleaned` + every `require_*` list column for ITViec rows."""
    return main_clean_requirement(df, extra_noise_patterns=_ITVIEC_REQUIREMENT_NOISE)
