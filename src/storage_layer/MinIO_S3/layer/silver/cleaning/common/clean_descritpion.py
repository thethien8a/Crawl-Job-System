from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.utils.clean_text import clean_text


def apply_description_cleaning(
    df: pl.DataFrame,
    text_col: str = "job_description",
    cleaned_col: str = "job_description_cleaned",
    extra_noise_patterns: list[str] | None = None,
) -> pl.DataFrame:
    """Basic cleaning for `job_description` — no taxonomy extraction.

    Strips HTML tags/entities and bullet glyphs, collapses whitespace, and
    optionally removes site-specific noise (e.g. ITviec's "Job description"
    section header). Site-specific patterns are passed via `extra_noise_patterns`
    by the entity wrapper and should use the ORIGINAL (accented) spelling since
    they run before any normalization.
    """
    return df.with_columns(
        clean_text(text_col, extra_noise_patterns).alias(cleaned_col)
    )
