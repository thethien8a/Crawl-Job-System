from __future__ import annotations
import polars as pl
from src.storage_layer.MinIO_S3.layer.silver.utils.normalize_data import remove_accents_col


def clean_exp_level(
    df: pl.DataFrame,
    column_name: str = "experience_level",
    min_col: str = "min_exp_level",
    max_col: str = "max_exp_level",
) -> pl.DataFrame:
    """
    Tách cột experience_level thành 2 cột số (đơn vị: NĂM):
        - min_exp_level: kinh nghiệm tối thiểu
        - max_exp_level: kinh nghiệm tối đa

    Quy tắc phân loại:
        1. NULL / không có chữ số và không khớp keyword đặc biệt   -> MIN=0,        MAX=NULL
        2. "Không yêu cầu", "Khong YC kinh nghiem", "no exp"        -> MIN=0,        MAX=0
        3. "Fresh", "fresher", "entry level", "moi tot nghiep"      -> MIN=0,        MAX=0
        4. "Dưới X", "less than X", "under X", "below X"            -> MIN=0,        MAX=X
        5. "Trên X", "over X", "more than X", "X+", "X tro len"     -> MIN=X,        MAX=NULL
        6. "X tháng", "X month(s)", "occupational X"                -> MIN=X/12,     MAX=X/12  (đổi tháng -> năm)
        7. "X - Y năm"                                              -> MIN=X,        MAX=Y
        8. "X năm" (đơn lẻ)                                         -> MIN=X,        MAX=NULL

    Khác biệt so với macro SQL gốc (clean_exp.sql):
        - "Không yêu cầu" (không có hậu tố "KN") nay đặt MAX=0 thay vì NULL → nhất quán semantic.
        - Bổ sung "Trên X", "X+", "X trở lên" tường minh (SQL gốc xếp vào case mặc định).
        - Bổ sung đơn vị "tháng / month" (SQL gốc chỉ xử lý keyword "occupational").
        - Khi đơn vị là tháng, MAX cũng được tính (giá trị xác định), không bỏ NULL.
        - Bổ sung nhóm "Fresh / fresher / entry level" làm đồng nghĩa "không yêu cầu".
    """

    df = remove_accents_col(df, column_name, "exp_no_accent")
    raw = pl.col(column_name).cast(pl.String)
    txt = pl.col("exp_no_accent").str.to_lowercase()

    # Chuẩn hóa dấu phẩy thập phân kiểu Việt Nam (1,5 -> 1.5) để extract số float chính xác.
    normalized = txt.str.replace_all(",", ".")

    # Cờ phân loại — tách riêng giúp biểu thức when/then dễ đọc và tránh lặp regex.
    has_digit = txt.str.contains(r"\d")
    is_no_req = txt.str.contains(
        r"(khong yeu cau|khong yc|ko yc|khong ycau|no experience|no exp|not required|"
        r"\bfresh\b|fresher|entry[ -]?level|moi tot nghiep|new grad)"
    )
    is_less = txt.str.contains(r"(less than|under|below|duoi|it hon)")
    is_over = txt.str.contains(r"(over|more than|tren|tro len|\d+\s*\+|\+\s*$)")
    is_month = txt.str.contains(r"(thang|month|occupational)")
    has_range = txt.str.contains(r"-|\bto\b|den|toi")

    # Trích xuất số đầu tiên và số thứ hai trong chuỗi đã chuẩn hóa.
    first_number = normalized.str.extract(r"(\d+\.?\d*)", 1).cast(pl.Float64, strict=False)
    second_number = normalized.str.extract(r"\d+\.?\d*\D+(\d+\.?\d*)", 1).cast(pl.Float64, strict=False)

    # Hệ số quy đổi đơn vị: tháng -> năm.
    unit_div = pl.when(is_month).then(pl.lit(12.0)).otherwise(pl.lit(1.0))

    null_float = pl.lit(None, dtype=pl.Float64)
    zero_float = pl.lit(0.0)

    # MIN: ưu tiên kiểm tra "không yêu cầu" và "dưới X" trước để cho 0 đúng quy ước.
    min_expr = (
        pl.when(raw.is_null())
        .then(zero_float)
        .when(is_no_req)
        .then(zero_float)
        .when(is_less)
        .then(zero_float)
        .when(~has_digit)
        .then(zero_float)
        .otherwise(first_number / unit_div)
    )

    # MAX: thứ tự ưu tiên cần phân biệt range (có 2 số) vs less (chỉ có 1 số là upper bound).
    max_expr = (
        pl.when(raw.is_null())
        .then(null_float)
        .when(is_no_req)
        .then(zero_float)
        .when(has_range & second_number.is_not_null())
        .then(second_number / unit_div)
        .when(is_less)
        .then(first_number / unit_div)
        .when(is_month & ~is_over)
        .then(first_number / unit_div)
        .otherwise(null_float)
    )

    df = df.with_columns([
        min_expr.alias(min_col),
        max_expr.alias(max_col),
    ])
    
    df = df.drop("exp_no_accent")
    return df
