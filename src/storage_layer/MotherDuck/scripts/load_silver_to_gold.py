import logging
from dataclasses import fields
from typing import Union, get_args, get_origin

from src.storage_layer.MotherDuck.client import MotherDuckClient
from src.storage_layer.MotherDuck.config import (
    GOLD_BENEFITS_TABLE,
    GOLD_DATE_COLUMNS,
    GOLD_DIM_DATE_TABLE,
    GOLD_INDUSTRIES_TABLE,
    GOLD_JOBS_COLUMNS,
    GOLD_JOBS_TABLE,
    GOLD_REQUIREMENTS_TABLE,
    GOLD_SCHEMA,
    LIST_FIELD_TO_CHILD,
    MOTHERDUCK_DATABASE,
    SILVER_PARQUET_GLOB,
)
from src.storage_layer.MotherDuck.schema.data_class import GoldJobItem

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NONE_TYPE = type(None)
DUCKDB_TYPE_BY_PYTHON_TYPE = {
    str: "VARCHAR",
    int: "BIGINT",
    float: "DOUBLE",
    bool: "BOOLEAN",
}
LEGACY_COLUMN_FALLBACKS = {
    "clean_company_name": ("company_name_canonical", "company_name"),
    "clean_job_title": ("job_title",),
    "clean_location": ("location",),
}


def _qualified(table_name: str) -> str:
    """Return ``gold.<table_name>``."""
    return f"{GOLD_SCHEMA}.{table_name}"


def _unwrap_optional(python_type: type) -> type:
    origin = get_origin(python_type)
    if origin is not Union:
        return python_type

    non_none_types = [arg for arg in get_args(python_type) if arg is not NONE_TYPE]
    return non_none_types[0] if non_none_types else python_type


def _duckdb_type(python_type: type) -> str:
    unwrapped_type = _unwrap_optional(python_type)
    origin = get_origin(unwrapped_type)

    if origin is list:
        inner_types = get_args(unwrapped_type)
        inner_type = inner_types[0] if inner_types else str
        return f"{_duckdb_type(inner_type)}[]"

    return DUCKDB_TYPE_BY_PYTHON_TYPE.get(unwrapped_type, "VARCHAR")


GOLD_COLUMN_TYPES = {
    field.name: _duckdb_type(field.type)
    for field in fields(GoldJobItem)
}
GOLD_COLUMN_TYPES.update({column: "VARCHAR" for column in GOLD_DATE_COLUMNS})


def _fallback_expression(column_name: str, available_columns: set[str]) -> str | None:
    fallback_columns = [
        fallback_column
        for fallback_column in LEGACY_COLUMN_FALLBACKS.get(column_name, ())
        if fallback_column in available_columns
    ]
    if not fallback_columns:
        return None

    return f"COALESCE({', '.join(fallback_columns)})"


def _select_expression(column_name: str, available_columns: set[str]) -> str:
    column_type = GOLD_COLUMN_TYPES.get(column_name, "VARCHAR")

    if column_name in available_columns:
        return column_name

    fallback_expression = _fallback_expression(column_name, available_columns)
    if fallback_expression:
        return f"CAST({fallback_expression} AS {column_type}) AS {column_name}"

    return f"CAST(NULL AS {column_type}) AS {column_name}"


def _read_silver_parquet_sql() -> str:
    return f"""
    read_parquet(
        '{SILVER_PARQUET_GLOB}',
        hive_partitioning = true,
        filename = true,
        union_by_name = true
    )
    """


def get_silver_columns(client: MotherDuckClient) -> set[str]:
    rows = client.con.sql(
        f"""
        DESCRIBE SELECT *
        FROM {_read_silver_parquet_sql()}
        """
    ).fetchall()
    return {row[0] for row in rows}


def build_all_gold_sql(silver_columns: set[str]) -> list[tuple[str, str]]:
    """Return ordered list of ``(description, sql)`` to rebuild all Gold tables.

    The list is designed to be executed sequentially:
    1. ``gold._staging`` — deduped snapshot (one row per ``job_url``) of all
       needed Silver columns.
    2. ``gold.dim_date`` — contiguous calendar from 2023-01-01 to 2026-05-01.
    3. ``gold.jobs`` — fact: scalars + ``source_site`` + derived ``date_key`` FK.
    5. ``gold.job_industries`` — unnested industries ``(job_url, industry)``.
    6. ``gold.job_benefits`` — unnested benefits ``(job_url, benefit)``.
    7. ``gold.job_requirements`` — unnested requirements/skills/keywords
       ``(job_url, requirement_type, value)``.
    8. ``DROP gold._staging``.
    """
    # Staging carries every column the downstream tables need: scalars
    # (GOLD_JOBS_COLUMNS already includes job_url and source_site), the list
    # columns to unnest, and the date columns used to build dim_date + date_key.
    list_col_names = list(LIST_FIELD_TO_CHILD.keys())
    all_cols = ",\n            ".join(
        _select_expression(column_name, silver_columns)
        for column_name in GOLD_JOBS_COLUMNS + list_col_names + GOLD_DATE_COLUMNS
    )
    # The fact keeps job_url + all scalars (including the source_site FK).
    fact_cols = ",\n            ".join(GOLD_JOBS_COLUMNS)

    staging = _qualified("_staging")

    statements: list[tuple[str, str]] = []

    # ------------------------------------------------------------------
    # 1. Staging table — read Silver once, deduplicate to one row per job_url
    # ------------------------------------------------------------------
    staging_sql = f"""
    CREATE OR REPLACE TABLE {staging} AS
    SELECT
        {all_cols}
    FROM {_read_silver_parquet_sql()}
    QUALIFY row_number() OVER (
        PARTITION BY job_url
        ORDER BY year DESC, month DESC, day DESC, filename DESC
    ) = 1
    """
    statements.append((f"Staging table {staging}", staging_sql))

    # ------------------------------------------------------------------
    # 2. gold.dim_date — contiguous calendar from 2023-01-01 to 2026-05-01
    #    Power BI time intelligence expects a gap-free date table.
    # ------------------------------------------------------------------
    dim_date_table = _qualified(GOLD_DIM_DATE_TABLE)
    statements.append((
        f"Date dimension {dim_date_table}",
        f"""
    CREATE OR REPLACE TABLE {dim_date_table} AS
    WITH calendar AS (
        SELECT CAST(d AS DATE) AS full_date
        FROM generate_series(DATE '2023-01-01', DATE '2026-05-01', INTERVAL 1 DAY) AS g(d)
    )
    SELECT
        CAST(strftime(full_date, '%Y%m%d') AS INTEGER) AS date_key,
        full_date,
        year(full_date)          AS year,
        quarter(full_date)       AS quarter,
        month(full_date)         AS month,
        monthname(full_date)     AS month_name,
        day(full_date)           AS day,
        isodow(full_date)        AS day_of_week,
        dayname(full_date)       AS day_name,
        week(full_date)          AS week_of_year,
        (isodow(full_date) >= 6) AS is_weekend
    FROM calendar
    ORDER BY full_date
    """,
    ))

    # ------------------------------------------------------------------
    # 3. gold.jobs — fact: scalars + source_site + derived date_key FK
    # ------------------------------------------------------------------
    jobs_table = _qualified(GOLD_JOBS_TABLE)
    statements.append((
        f"Fact table {jobs_table}",
        f"""
    CREATE OR REPLACE TABLE {jobs_table} AS
    SELECT
        {fact_cols},
        CAST(year AS INT) * 10000 + CAST(month AS INT) * 100 + CAST(day AS INT) AS date_key
    FROM {staging}
    """,
    ))

    # ------------------------------------------------------------------
    # 5. gold.job_industries
    # ------------------------------------------------------------------
    industries_table = _qualified(GOLD_INDUSTRIES_TABLE)
    statements.append((
        f"Industries table {industries_table}",
        f"""
    CREATE OR REPLACE TABLE {industries_table} AS
    SELECT
        job_url,
        UNNEST(job_industry_clean) AS industry
    FROM {staging}
    WHERE job_industry_clean IS NOT NULL
      AND len(job_industry_clean) > 0
    """,
    ))

    # ------------------------------------------------------------------
    # 6. gold.job_benefits
    # ------------------------------------------------------------------
    benefits_table = _qualified(GOLD_BENEFITS_TABLE)
    statements.append((
        f"Benefits table {benefits_table}",
        f"""
    CREATE OR REPLACE TABLE {benefits_table} AS
    SELECT
        job_url,
        UNNEST(benefits_categories_vi) AS benefit
    FROM {staging}
    WHERE benefits_categories_vi IS NOT NULL
      AND len(benefits_categories_vi) > 0
    """,
    ))

    # ------------------------------------------------------------------
    # 7. gold.job_requirements — union of all require_* + keywords
    # ------------------------------------------------------------------
    requirements_table = _qualified(GOLD_REQUIREMENTS_TABLE)

    branches: list[str] = []
    for field_name, (child_table, discriminator) in LIST_FIELD_TO_CHILD.items():
        if child_table != GOLD_REQUIREMENTS_TABLE:
            continue
        branches.append(f"""    SELECT
        job_url,
        '{discriminator}' AS requirement_type,
        UNNEST({field_name}) AS value
    FROM {staging}
    WHERE {field_name} IS NOT NULL
      AND len({field_name}) > 0""")

    union_body = "\n    UNION ALL\n".join(branches)
    statements.append((
        f"Requirements table {requirements_table}",
        f"""
    CREATE OR REPLACE TABLE {requirements_table} AS
{union_body}
    """,
    ))

    # ------------------------------------------------------------------
    # 8. Drop staging
    # ------------------------------------------------------------------
    statements.append((
        f"Drop staging {staging}",
        f"DROP TABLE IF EXISTS {staging}",
    ))

    return statements


def main() -> None:
    client = MotherDuckClient()
    client.setup_s3_credentials()

    client.execute_statement(f'CREATE DATABASE IF NOT EXISTS "{MOTHERDUCK_DATABASE}"')
    client.execute_statement(f'USE "{MOTHERDUCK_DATABASE}"')
    client.execute_statement(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

    silver_columns = get_silver_columns(client)
    statements = build_all_gold_sql(silver_columns)

    for description, sql in statements:
        logger.info("Executing: %s", description)
        client.execute_statement(sql)

    # Print final row counts for all gold tables.
    for table_name in (
        GOLD_JOBS_TABLE,
        GOLD_DIM_DATE_TABLE,
        GOLD_INDUSTRIES_TABLE,
        GOLD_BENEFITS_TABLE,
        GOLD_REQUIREMENTS_TABLE,
    ):
        qualified = _qualified(table_name)
        row_count = client.con.sql(
            f"SELECT count(*) FROM {qualified}"
        ).fetchone()[0]
        logger.info(
            "%s.%s.%s: %d rows",
            MOTHERDUCK_DATABASE,
            GOLD_SCHEMA,
            table_name,
            row_count,
        )


if __name__ == "__main__":
    main()
