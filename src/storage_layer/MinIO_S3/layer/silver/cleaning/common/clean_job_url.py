import polars as pl

ITVIEC_SOURCE_SITE = "itviec"
VIETNAMWORKS_SOURCE_SITE = "vietnamworks"
URL_DECORATION_PATTERN = r"[?#].*$"
ITVIEC_NUMERIC_SUFFIX_PATTERN = r"-\d+/?$"
VIETNAMWORKS_JOB_ID_SUFFIX_PATTERN = r"--\d+-jv/?$"


def clean_job_url(
    df: pl.DataFrame,
    column_name: str = "job_url",
    unique_column_name: str = "unique_url",
    source_site_column: str = "source_site",
) -> pl.DataFrame:
    cleaned_url = clean_url_expr(pl.col(column_name))
    source_site = _source_site_expr(df, source_site_column)

    return df.with_columns(
        cleaned_url.alias(column_name),
        build_unique_url_expr(cleaned_url, source_site).alias(unique_column_name),
    )


def clean_url_expr(url_expr: pl.Expr) -> pl.Expr:
    return url_expr.str.replace(URL_DECORATION_PATTERN, "")


def build_unique_url_expr(cleaned_url: pl.Expr, source_site: pl.Expr) -> pl.Expr:
    normalized_source_site = source_site.fill_null("")

    return (
        pl.when(normalized_source_site == ITVIEC_SOURCE_SITE)
        .then(cleaned_url.str.replace(ITVIEC_NUMERIC_SUFFIX_PATTERN, ""))
        .when(normalized_source_site == VIETNAMWORKS_SOURCE_SITE)
        .then(cleaned_url.str.replace(VIETNAMWORKS_JOB_ID_SUFFIX_PATTERN, ""))
        .otherwise(cleaned_url)
    )


def _source_site_expr(df: pl.DataFrame, source_site_column: str) -> pl.Expr:
    if source_site_column not in df.columns:
        return pl.lit(None, dtype=pl.String)

    return pl.col(source_site_column).str.to_lowercase().str.strip_chars()
