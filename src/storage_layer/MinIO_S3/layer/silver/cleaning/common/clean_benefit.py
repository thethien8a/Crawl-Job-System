from __future__ import annotations

import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.utils.clean_text import clean_text
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import remove_vietnamese_accents


def _build_label_exprs(
    taxonomy_df: pl.DataFrame,
    source_col: str,
    label_col: str,
) -> list[pl.Expr]:
    """One Polars expression per taxonomy row → label or null.

    Feed the resulting list into `pl.concat_list().list.drop_nulls()` so a
    single benefit block can carry every category whose keywords it matches.
    """
    exprs: list[pl.Expr] = []
    for row in taxonomy_df.iter_rows(named=True):
        keywords = row.get("keywords") or ""
        label = row.get(label_col)
        if not keywords or not label:
            continue
        pattern = f"(?i)({keywords})"
        exprs.append(
            pl.when(pl.col(source_col).str.contains(pattern))
            .then(pl.lit(label))
            .otherwise(pl.lit(None, dtype=pl.String))
        )
    return exprs


def apply_benefit_cleaning(
    df: pl.DataFrame,
    taxonomy_df: pl.DataFrame,
    column_name: str = "benefits",
    extra_noise_patterns: list[str] | None = None,
) -> pl.DataFrame:
    """Clean `benefits` and add the multi-label Vietnamese category column.

    Keeps the raw `benefits` column untouched so the Silver pipeline can be
    re-run after taxonomy updates (same reprocessing strategy as job_industry).
    Site-specific prefixes/suffixes should be passed via `extra_noise_patterns`
    by the entity wrapper (use ACCENTED patterns — they run before normalization).
    """
    df = df.with_columns(
        clean_text(column_name, extra_noise_patterns).alias("benefits_text_clean")
    )

    # Accent-stripped, lower-cased view used ONLY for keyword matching. The
    # user-facing `benefits_text_clean` keeps original Vietnamese diacritics.
    df = df.with_columns(
        pl.col("benefits_text_clean")
        .map_elements(remove_vietnamese_accents, return_dtype=pl.String)
        .str.to_lowercase()
        .alias("_benefits_norm")
    )

    vi_exprs = _build_label_exprs(taxonomy_df, "_benefits_norm", "canonical_vi")

    if not vi_exprs:
        # Empty/invalid taxonomy — still emit the column with empty lists so the
        # Silver schema stays stable for downstream consumers.
        empty = pl.lit([], dtype=pl.List(pl.String))
        return df.with_columns(empty.alias("benefits_categories_vi")).drop("_benefits_norm")

    return df.with_columns(
        pl.concat_list(vi_exprs).list.drop_nulls().list.unique().alias("benefits_categories_vi"),
    ).drop("_benefits_norm")
