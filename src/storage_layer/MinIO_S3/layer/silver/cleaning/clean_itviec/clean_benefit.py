"""ITViec benefits cleaner — FlashText + regex fallback (parity with clean_requirement)."""
from __future__ import annotations

import polars as pl

from src.storage_layer.MinIO_S3.layer.silver.utils.clean_text import clean_text
from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import read_seeds
from src.storage_layer.MinIO_S3.layer.silver.utils.flashtext_extractor import (
    build_extractor,
    extract_unique_labels,
)
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import (
    remove_vietnamese_accents,
)

# ITViec wraps every benefits block with a marketing hero "Why you'll love
# working here". Pure copy, no signal for taxonomy matching — strip it.
_ITVIEC_NOISE_PATTERNS = [
    r"(?i)why you\S?ll love working here",
]


def apply_benefit_cleaning(
    df: pl.DataFrame,
    taxonomy_df: pl.DataFrame,
    text_col: str = "benefits",
    cleaned_col: str = "benefits_text_clean",
    label_col: str = "canonical_vi",
    output_col: str = "benefits_categories_vi",
    extra_noise_patterns: list[str] | None = None,
) -> pl.DataFrame:
    """Clean `benefits` text and extract canonical Vietnamese benefit labels.

    Mirrors `apply_requirement_cleaning`: FlashText handles the literal majority
    in O(n) per row, while genuinely regex alternatives in the taxonomy keep
    full parity via the compiled fallback list inside `HybridKeywordExtractor`.

    Matching runs on an accent-stripped, lower-cased copy of the cleaned text
    so Vietnamese taxonomy entries like `bao hiem`, `nghi phep`, `luong thang 13`
    still hit the original diacriticised text. The user-facing
    `benefits_text_clean` keeps the original Vietnamese spelling.
    """
    df = df.with_columns(
        clean_text(text_col, extra_noise_patterns).alias(cleaned_col)
    )

    # Accent-stripped & lowered view used ONLY for keyword matching.
    df = df.with_columns(
        pl.col(cleaned_col)
        .map_elements(remove_vietnamese_accents, return_dtype=pl.String)
        .str.to_lowercase()
        .alias("_benefits_norm")
    )

    # Empty/invalid taxonomy — still emit the column with empty lists so the
    # Silver schema stays stable for downstream consumers.
    if taxonomy_df is None or taxonomy_df.is_empty():
        return df.with_columns(
            pl.lit([], dtype=pl.List(pl.String)).alias(output_col)
        ).drop("_benefits_norm")

    extractor = build_extractor(taxonomy_df, label_col=label_col)

    # `skip_nulls=False` lets the helper turn null benefits into `[]`,
    # keeping the Silver schema stable for downstream joins/serialization.
    return df.with_columns(
        pl.col("_benefits_norm")
        .map_elements(
            lambda text, ex=extractor: extract_unique_labels(text, ex),
            return_dtype=pl.List(pl.String),
            skip_nulls=False,
        )
        .alias(output_col)
    ).drop("_benefits_norm")


def main(df: pl.DataFrame, extra_noise_patterns: list[str] | None = None) -> pl.DataFrame:
    taxonomy_df = read_seeds("benefit_taxonomy.csv")
    patterns = list(_ITVIEC_NOISE_PATTERNS)
    if extra_noise_patterns:
        patterns.extend(extra_noise_patterns)
    return apply_benefit_cleaning(df, taxonomy_df, extra_noise_patterns=patterns)
