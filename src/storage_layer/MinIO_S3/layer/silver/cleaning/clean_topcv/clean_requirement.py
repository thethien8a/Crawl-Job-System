"""TopCV-specific wrapper for the common requirements cleaner."""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_requirement import (
    main_clean_requirement,
)

# TopCV puts a "Yêu cầu ứng viên" header above the requirements block.
# ACCENTED pattern — cleaning runs before accent normalization.
_TOPCV_REQUIREMENT_NOISE = [r"(?i)yêu cầu ứng viên"]


def clean_topcv_requirement(df: pl.DataFrame) -> pl.DataFrame:
    """Add `requirements_cleaned` + every `require_*` list column for TopCV rows."""
    return main_clean_requirement(df, extra_noise_patterns=_TOPCV_REQUIREMENT_NOISE)
