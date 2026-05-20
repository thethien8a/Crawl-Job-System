from __future__ import annotations
import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.utils.clean_text import clean_text
from src.storage_layer.MinIO_S3.layer.silver.utils.flashtext_extractor import (
    build_extractor,
    extract_unique_labels,
)
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import (
    remove_vietnamese_accents,
)
from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import read_seeds

# One List[String] column per taxonomy file. Output names follow the
# `require_*` convention already used in SilverJobItem so downstream code
# can attach them onto the data class with minimal renaming.
_TAXONOMY_TO_OUTPUT_COL: dict[str, str] = {
    "program_lang": "require_programming_languages",
    "framework": "require_frameworks",
    "tools": "require_tools",
    "cloud_skill": "require_cloud_skills",
    "knowledge": "require_knowledge",
    "domain": "require_domain_knowledge",
    "language": "require_foreign_languages",
    "domain_university": "require_domain_university",
}


def apply_requirement_cleaning(
    df: pl.DataFrame,
    taxonomies: dict[str, pl.DataFrame],
    text_col: str = "requirements",
    cleaned_col: str = "requirements_cleaned",
    extra_noise_patterns: list[str] | None = None,
) -> pl.DataFrame:
    """Clean `requirements` text and extract canonical skill lists with FlashText.

    `taxonomies` is keyed by taxonomy short-name (`program_lang`, `framework`,
    `tools`, `cloud_skill`, `knowledge`, `domain`, `language`) and the values
    are DataFrames returned by `read_seeds(<file>.csv)`. Missing keys are
    tolerated — their output column is filled with empty lists so the Silver
    schema stays stable across sites.

    Matching runs on an accent-stripped, lower-cased copy of the cleaned
    text so Vietnamese taxonomy entries like `ngan hang`, `chuoi thoi gian`,
    `tieng anh` still hit the original diacriticised text. The user-facing
    `requirements_cleaned` keeps the original Vietnamese spelling.
    """
    df = df.with_columns(
        clean_text(text_col, extra_noise_patterns).alias(cleaned_col)
    )

    # Accent-stripped & lowered view used ONLY for keyword matching.
    df = df.with_columns(
        pl.col(cleaned_col)
        .map_elements(remove_vietnamese_accents, return_dtype=pl.String)
        .str.to_lowercase()
        .alias("_requirements_norm")
    )

    new_columns: list[pl.Expr] = []
    for tax_key, out_col in _TAXONOMY_TO_OUTPUT_COL.items():
        taxonomy_df = taxonomies.get(tax_key)
        if taxonomy_df is None or taxonomy_df.is_empty():
            new_columns.append(
                pl.lit([], dtype=pl.List(pl.String)).alias(out_col)
            )
            continue

        extractor = build_extractor(taxonomy_df)
        # `skip_nulls=False` lets the helper turn null requirements into `[]`,
        # keeping the Silver schema stable for downstream joins/serialization.
        # Default-arg captures `extractor` per iteration so each closure binds its own.
        new_columns.append(
            pl.col("_requirements_norm")
            .map_elements(
                lambda text, ex=extractor: extract_unique_labels(text, ex),
                return_dtype=pl.List(pl.String),
                skip_nulls=False,
            )
            .alias(out_col)
        )

    return df.with_columns(new_columns).drop("_requirements_norm")

def main(df: pl.DataFrame, extra_noise_patterns: list[str] | None = None):
    # Build taxonomies dict from seed CSV files
    taxonomies = {
        "program_lang": read_seeds("program_lang_taxonomy.csv"),
        "framework": read_seeds("framework_taxonomy.csv"),
        "tools": read_seeds("tools_taxonomy.csv"),
        "cloud_skill": read_seeds("cloud_skill_taxonomy.csv"),
        "knowledge": read_seeds("knowledge_taxonomy.csv"),
        "domain": read_seeds("domain_taxonomy.csv"),
        "language": read_seeds("language_taxonomy.csv"),
        "domain_university": read_seeds("domain_university_taxonomy.csv"),
    }
    
    return apply_requirement_cleaning(df, taxonomies, extra_noise_patterns=extra_noise_patterns)
