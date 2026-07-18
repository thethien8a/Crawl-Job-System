from __future__ import annotations
import polars as pl


# Tỷ giá USD->VND
USD_TO_VND_RATE = 26_000


def clean_salary(
    df: pl.DataFrame,
    column_name: str = "salary",
    min_col: str = "min_monthly_salary",
    max_col: str = "max_monthly_salary",
) -> pl.DataFrame:
    """
    Tách cột salary thành 2 cột: min_monthly_salary và max_monthly_salary (VND/tháng).

    Quy đổi đơn vị:
    - USD / $          -> *USD_TO_VND_RATE; nếu có dạng "Nk" (vd "5k usd") thì *1.000 nữa
    - "k" / "nghìn"    -> *1.000
    - "tr" / "triệu" / "m" -> *1.000.000
    - "tỷ"             -> *1.000.000.000

    Quy đổi thời gian (về tháng):
    - "ngày" -> *30, "tuần" -> *4, "năm" / "year" -> *0.083, còn lại -> *1

    Quy ước phạm vi:
    - Khoảng "10 - 20"                        -> min=10, max=20
    - "từ X" / "trên X" (không có dấu '-')    -> max=null
    - "đến X" / "tới X" / "upto X" (không có "từ|trên") -> min=null
    - Số đơn không có đơn vị mà < 1000 (vd "15") -> ngầm hiểu là triệu VND
    """
    raw = pl.col(column_name).cast(pl.String)
    salary = raw.str.to_lowercase()

    # Các flag phân loại — đặt riêng để biểu thức dưới dễ đọc và tránh lặp regex
    has_digit = salary.str.contains(r"\d")
    is_usd = salary.str.contains(r"(usd|\$)")
    
    # Dùng cho bước normalize: chỉ những đơn vị "lớn" (tr/triệu/tỷ) thì giữ dấu thập phân
    has_decimal_unit = salary.str.contains(r"(tr|triệu|tỷ)")
    has_thousand_text = salary.str.contains(r"(nghìn|\d+(?:[.,]\d+)?\s*k\b)")

    # Bản gốc SQL dùng (tr|triệu|m) cho hệ số *1.000.000 — giữ nguyên (kể cả ký tự 'm' lẻ)
    has_million_text = salary.str.contains(r"(tr|triệu|m)")
    has_billion_text = salary.str.contains(r"tỷ")
    has_any_unit = salary.str.contains(
        r"(usd|\$|nghìn|\d+(?:[.,]\d+)?\s*k\b|tr|triệu|tỷ)"
    )
    has_usd_k = salary.str.contains(r"\d+(?:[.,]\d+)?\s*k\b")

    has_day = salary.str.contains(r"ngày")
    has_week = salary.str.contains(r"tuần")
    has_year = salary.str.contains(r"năm|year")

    has_upto = salary.str.contains(r"(tới|đến|upto|up to)")
    has_from = salary.str.contains(r"(từ|trên)")
    has_dash = salary.str.contains(r"-")

    # 1. Chuẩn hóa chuỗi để trích xuất số:
    #    - USD/$           -> bỏ dấu phẩy (1,500 -> 1500)
    #    - tr/triệu/tỷ     -> đổi dấu phẩy thành dấu chấm (1,5 triệu -> 1.5 triệu)
    #    - còn lại         -> bỏ cả dấu phẩy lẫn dấu chấm (1.500.000 -> 1500000)
    normalized = (
        pl.when(has_decimal_unit)
        .then(salary.str.replace_all(",", "."))
        .when(is_usd)
        .then(salary.str.replace_all(",", ""))
        .otherwise(salary.str.replace_all(",", "").str.replace_all(r"\.", ""))
    )

    # 2. Hệ số thời gian (quy đổi về tháng)
    time_mult = (
        pl.when(has_day).then(pl.lit(30.0))
        .when(has_week).then(pl.lit(4.0))
        .when(has_year).then(pl.lit(0.083))
        .otherwise(pl.lit(1.0))
    )

    # 3. Hệ số đơn vị tiền tệ / độ lớn (thứ tự CASE phải khớp SQL gốc)
    unit_mult = (
        pl
        .when(is_usd)
        .then(
            pl.when(has_usd_k)
            .then(pl.lit(1_000.0 * USD_TO_VND_RATE))
            .otherwise(pl.lit(float(USD_TO_VND_RATE)))
        )
        .when(has_thousand_text).then(pl.lit(1_000.0))
        .when(has_million_text).then(pl.lit(1_000_000.0))
        .when(has_billion_text).then(pl.lit(1_000_000_000.0))
        .otherwise(pl.lit(1.0))
    )

    # 4. Trích xuất số đầu tiên và số thứ hai trong chuỗi đã chuẩn hóa
    first_number = normalized.str.extract(r"(\d+\.?\d*)", 1).cast(pl.Float64, strict=False)
    second_number = normalized.str.extract(r"\d+\.?\d*\D+(\d+\.?\d*)", 1).cast(pl.Float64, strict=False)

    null_float = pl.lit(None, dtype=pl.Float64)

    # 5. MIN SALARY
    min_base = (
        pl.when(has_upto & ~has_from)
        .then(null_float)
        .otherwise(first_number)
    )
    # "Triệu ngầm" cho MIN: không có đơn vị text và số đầu < 1000 (vd "15" -> 15 triệu)
    min_unit_mult = (
        pl.when(~has_any_unit & (first_number < 1000))
        .then(pl.lit(1_000_000.0))
        .otherwise(unit_mult)
    )
    min_expr = (
        pl.when(raw.is_null() | ~has_digit)
        .then(null_float)
        .otherwise(min_base * time_mult * min_unit_mult)
    )

    # 6. MAX SALARY
    max_base = (
        pl.when(has_from & ~has_dash)
        .then(null_float)
        .when(has_dash)
        .then(second_number)
        .otherwise(first_number)
    )
    # Số tham chiếu để xét "triệu ngầm" cho MAX (số thứ 2 nếu có dấu '-', nếu không là số đầu)
    max_reference = pl.when(has_dash).then(second_number).otherwise(first_number)
    max_unit_mult = (
        pl.when(~has_any_unit & (max_reference < 1000))
        .then(pl.lit(1_000_000.0))
        .otherwise(unit_mult)
    )
    max_expr = (
        pl.when(raw.is_null() | ~has_digit)
        .then(null_float)
        .otherwise(max_base * time_mult * max_unit_mult)
    )

    return df.with_columns([
        min_expr.alias(min_col),
        max_expr.alias(max_col),
    ])
