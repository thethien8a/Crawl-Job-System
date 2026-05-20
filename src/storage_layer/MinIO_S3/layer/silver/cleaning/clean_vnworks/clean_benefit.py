"""VietnamWorks-specific wrapper for the common benefits cleaner."""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_benefit import (
    apply_benefit_cleaning,
)

# VietnamWorks closes every benefits block with an "Xem thêm" expand toggle
# captured by the scraper. The Vietnamese section headers it emits
# (Thưởng, Chăm sóc sức khoẻ, Nghỉ phép có lương, Hoạt động nhóm) are
# INTENTIONALLY kept — they boost downstream keyword matching, not pollute it.
_VNWORKS_NOISE_PATTERNS = [
    r"(?i)xem thêm",
    r"(?i)see more|read more",
]


def clean_vnworks_benefit(df: pl.DataFrame, taxonomy_df: pl.DataFrame) -> pl.DataFrame:
    """Add `benefits_text_clean` and `benefits_categories_vi` for VietnamWorks rows."""
    return apply_benefit_cleaning(
        df,
        taxonomy_df,
        column_name="benefits",
        extra_noise_patterns=_VNWORKS_NOISE_PATTERNS,
    )
