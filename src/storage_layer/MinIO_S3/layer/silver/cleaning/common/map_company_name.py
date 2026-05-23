import polars as pl
import unicodedata

from src.storage_layer.MinIO_S3.layer.silver.utils.config_loader import read_seeds

SEED_FILE_NAME = "company_mapping.csv"
CANONICAL_COLUMN = "company_name_canonical"
IS_MAPPED_COLUMN = "is_mapped"


def _normalize_name(value: str | None) -> str | None:
    if value is None:
        return None
    return unicodedata.normalize("NFC", value)


def _load_normalized_seed() -> pl.DataFrame:
    """
    Seed phải cùng dạng chuẩn hoá với cleaned company_name (NFC + UPPER + strip).
    Nếu không cùng dạng thì exact-join sẽ miss các variant chỉ khác nhau ở dạng tổ hợp dấu.

    Convention seed hiện tại không nhất quán (vài canonical có self-variant, vài không) =>
    auto bơm thêm self-variant cho mọi canonical để input "FPT TELECOM" có thể match canonical
    "FPT TELECOM" mà không cần sửa CSV thủ công.
    """
    seed = read_seeds(SEED_FILE_NAME).with_columns(
        [
            pl.col("variant_name")
            .cast(pl.String)
            .map_elements(_normalize_name, return_dtype=pl.String)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("variant_name"),
            pl.col("canonical_name")
            .cast(pl.String)
            .map_elements(_normalize_name, return_dtype=pl.String)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("canonical_name"),
        ]
    )

    self_variants = seed.select(
        pl.col("canonical_name").alias("variant_name"),
        pl.col("canonical_name"),
    ).unique()

    # Concat explicit variants TRƯỚC self-variants để keep="first" giữ ánh xạ tường minh
    # khi 1 string vừa là canonical vừa là variant (ví dụ "FPT" hiện ánh xạ -> "TẬP ĐOÀN FPT").
    return pl.concat([seed, self_variants], how="vertical").unique(
        subset=["variant_name"], keep="first"
    )


def map_canonical_company(
    df: pl.DataFrame, source_col: str = "company_name"
) -> pl.DataFrame:
    """
    Join cleaned company_name với seed company_mapping.csv để gắn canonical name + is_mapped flag.

    is_mapped = 1 khi variant_name khớp exact với source_col, ngược lại 0.
    Khi không khớp, company_name_canonical = NULL để downstream phân biệt với chuỗi rỗng.
    """
    seed = _load_normalized_seed()

    mapped = df.join(
        seed, left_on=source_col, right_on="variant_name", how="left"
    ).rename({"canonical_name": CANONICAL_COLUMN})

    return mapped.with_columns(
        pl.when(pl.col(CANONICAL_COLUMN).is_not_null())
        .then(1)
        .otherwise(0)
        .cast(pl.Int8)
        .alias(IS_MAPPED_COLUMN)
    )
