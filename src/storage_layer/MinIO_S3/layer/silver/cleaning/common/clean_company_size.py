import polars as pl


def clean_company_size(df: pl.DataFrame, column_name: str = "company_size", min_col: str = "min_company_size", max_col: str = "max_company_size") -> pl.DataFrame:
    """
    Tách cột company_size thành 2 cột min_company_size và max_company_size.
    
    Xử lý các case:
    - "50+" -> min=50, max=null
    - "50-100" hoặc "50 - 100" -> min=50, max=100
    - Không có số -> min=null, max=null
    """
    temp_col = f"{column_name}_clean"

    df = df.with_columns(
        pl.col(column_name).cast(pl.String).str.strip_chars().alias(temp_col)
    )

    has_plus = pl.col(temp_col).str.contains(r'\+')
    has_dash = pl.col(temp_col).str.contains(r'-')

    numbers = pl.col(temp_col).str.extract_all(r'\d+')

    min_expr = (
        pl.when(has_plus)
        .then(numbers.list.get(0, null_on_oob=True))
        .when(has_dash)
        .then(numbers.list.get(0, null_on_oob=True))
        .otherwise(None)
    )

    max_expr = (
        pl.when(has_dash)
        .then(numbers.list.get(1, null_on_oob=True))
        .otherwise(None)
    )

    df = df.with_columns([
        min_expr.cast(pl.Int64, strict=False).alias(min_col),
        max_expr.cast(pl.Int64, strict=False).alias(max_col),
    ])

    df = df.drop([temp_col])

    return df
