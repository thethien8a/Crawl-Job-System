"""TopCV-specific wrapper for the common benefits cleaner."""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.cleaning.common.clean_benefit import (
    main_clean_benefit,
)

# Section labels / sales banners scraped from TopCV. ACCENTED Vietnamese
# spelling — text cleaning runs BEFORE accent normalization.
_TOPCV_BENEFIT_NOISE = [
    r"(?i)có hỗ trợ data xem chi tiết",
    r"(?i)có hỗ trợ data",
    r"(?i)xem chi tiết",
    r"(?i)^\s*quyền lợi\b",
]


def clean_topcv_benefit(df: pl.DataFrame) -> pl.DataFrame:
    """Add `benefits_text_clean` and `benefits_categories_vi` for TopCV rows."""
    return main_clean_benefit(df, extra_noise_patterns=_TOPCV_BENEFIT_NOISE)
