"""VietnamWorks-specific wrapper for the common requirements cleaner."""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_requirement import (
    main_clean_requirement,
)

# VietnamWorks appends a UI prompt about matching score. The trailing "?"
# is escaped to keep the pattern a literal sentence. ACCENTED — cleaning runs
# before accent normalization.
_VNWORKS_REQUIREMENT_NOISE = [
    r"(?i)mức độ phù hợp và xếp hạng của bạn so với ứng viên khác như thế nào\?*",
]


def clean_vnworks_requirement(df: pl.DataFrame) -> pl.DataFrame:
    """Add `requirements_cleaned` + every `require_*` list column for VietnamWorks rows."""
    return main_clean_requirement(df, extra_noise_patterns=_VNWORKS_REQUIREMENT_NOISE)
