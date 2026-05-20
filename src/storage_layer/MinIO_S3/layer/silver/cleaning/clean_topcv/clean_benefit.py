"""TopCV-specific wrapper for the common benefits cleaner."""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_benefit import (
    apply_benefit_cleaning,
)

# TopCV prepends a sales banner about its "Data support" service plus the
# section label "Quyền lợi". Both add noise to the user-facing cleaned text
# (the no-accent matching layer already ignores them safely, but readers don't).
_TOPCV_NOISE_PATTERNS = [
    r"(?i)có hỗ trợ data xem chi tiết",
    r"(?i)có hỗ trợ data",
    r"(?i)xem chi tiết",
    r"(?i)^\s*quyền lợi\b",
]


def clean_topcv_benefit(df: pl.DataFrame, taxonomy_df: pl.DataFrame) -> pl.DataFrame:
    """Add `benefits_text_clean` and `benefits_categories_vi` for TopCV rows."""
    return apply_benefit_cleaning(
        df,
        taxonomy_df,
        column_name="benefits",
        extra_noise_patterns=_TOPCV_NOISE_PATTERNS,
    )
