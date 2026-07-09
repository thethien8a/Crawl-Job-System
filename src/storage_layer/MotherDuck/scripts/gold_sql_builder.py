from src.storage_layer.MotherDuck.config import (
    GOLD_BENEFITS_TABLE,
    GOLD_DATE_COLUMNS,
    GOLD_DIM_DATE_TABLE,
    GOLD_INDUSTRIES_TABLE,
    GOLD_JOBS_COLUMNS,
    GOLD_JOBS_TABLE,
    GOLD_REQUIREMENTS_TABLE,
    LIST_FIELD_TO_CHILD,
)
from src.storage_layer.MotherDuck.scripts.gold_sql_expressions import (
    JOB_ID_COLUMN,
    build_unique_url_expression,
    build_select_list,
    qualified,
    read_silver_parquet_sql,
)

GoldStatement = tuple[str, str]
GOLD_DEDUP_KEY = "dedup_url"


def build_all_gold_sql(silver_columns: set[str]) -> list[GoldStatement]:
    staging_table = qualified("_staging")
    dim_date_table = qualified(GOLD_DIM_DATE_TABLE)
    jobs_table = qualified(GOLD_JOBS_TABLE)
    industries_table = qualified(GOLD_INDUSTRIES_TABLE)
    benefits_table = qualified(GOLD_BENEFITS_TABLE)
    requirements_table = qualified(GOLD_REQUIREMENTS_TABLE)

    return [
        (
            f"Staging table {staging_table}",
            build_staging_sql(silver_columns, staging_table),
        ),
        (f"Date dimension {dim_date_table}", build_dim_date_sql(dim_date_table)),
        (f"Fact table {jobs_table}", build_jobs_sql(staging_table, jobs_table)),
        (
            f"Industries table {industries_table}",
            build_industries_sql(staging_table, industries_table),
        ),
        (
            f"Benefits table {benefits_table}",
            build_benefits_sql(staging_table, benefits_table),
        ),
        (
            f"Requirements table {requirements_table}",
            build_requirements_sql(staging_table, requirements_table),
        ),
        (f"Drop staging {staging_table}", f"DROP TABLE IF EXISTS {staging_table}"),
    ]


def build_staging_sql(silver_columns: set[str], staging_table: str) -> str:
    source_columns = GOLD_JOBS_COLUMNS + list(LIST_FIELD_TO_CHILD) + GOLD_DATE_COLUMNS
    selected_columns = build_select_list(source_columns, silver_columns)
    dedup_url_column = build_unique_url_expression(silver_columns, alias=GOLD_DEDUP_KEY)

    return f"""
    CREATE OR REPLACE TABLE {staging_table} AS
    WITH source_rows AS (
        SELECT
            {selected_columns},
            {dedup_url_column},
            filename
        FROM {read_silver_parquet_sql()}
    ),
    ranked AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY COALESCE({GOLD_DEDUP_KEY}, job_url)
                ORDER BY
                    TRY_CAST(year AS INT) DESC,
                    TRY_CAST(month AS INT) DESC,
                    TRY_CAST(day AS INT) DESC,
                    filename DESC
            ) AS dedup_rank
        FROM source_rows
    ),
    deduped AS (
        SELECT
            * EXCLUDE (filename, dedup_rank)
        FROM ranked
        WHERE dedup_rank = 1
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY COALESCE({GOLD_DEDUP_KEY}, job_url)) AS {JOB_ID_COLUMN},
        * EXCLUDE ({GOLD_DEDUP_KEY})
    FROM deduped
    """


def build_dim_date_sql(dim_date_table: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE {dim_date_table} AS
    WITH calendar AS (
        SELECT CAST(d AS DATE) AS full_date
        FROM generate_series(DATE '2023-01-01', current_date + INTERVAL 1 YEAR, INTERVAL 1 DAY) AS g(d)
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
    """


def build_jobs_sql(staging_table: str, jobs_table: str) -> str:
    fact_columns = ",\n            ".join([JOB_ID_COLUMN, *GOLD_JOBS_COLUMNS])
    return f"""
    CREATE OR REPLACE TABLE {jobs_table} AS
    SELECT
        {fact_columns},
        CAST(year AS INT) * 10000 + CAST(month AS INT) * 100 + CAST(day AS INT) AS date_key
    FROM {staging_table}
    """


def build_industries_sql(staging_table: str, industries_table: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE {industries_table} AS
    SELECT
        {JOB_ID_COLUMN},
        UNNEST(job_industry_clean) AS industry
    FROM {staging_table}
    WHERE job_industry_clean IS NOT NULL
      AND len(job_industry_clean) > 0
    """


def build_benefits_sql(staging_table: str, benefits_table: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE {benefits_table} AS
    SELECT
        {JOB_ID_COLUMN},
        UNNEST(benefits_categories_vi) AS benefit
    FROM {staging_table}
    WHERE benefits_categories_vi IS NOT NULL
      AND len(benefits_categories_vi) > 0
    """


def build_requirements_sql(staging_table: str, requirements_table: str) -> str:
    branches = [
        build_requirement_branch(field_name, discriminator, staging_table)
        for field_name, (child_table, discriminator) in LIST_FIELD_TO_CHILD.items()
        if child_table == GOLD_REQUIREMENTS_TABLE and discriminator is not None
    ]
    union_body = "\n    UNION ALL\n".join(branches)

    return f"""
    CREATE OR REPLACE TABLE {requirements_table} AS
{union_body}
    """


def build_requirement_branch(
    field_name: str,
    discriminator: str,
    staging_table: str,
) -> str:
    return f"""    SELECT
        {JOB_ID_COLUMN},
        '{discriminator}' AS requirement_type,
        UNNEST({field_name}) AS value
    FROM {staging_table}
    WHERE {field_name} IS NOT NULL
      AND len({field_name}) > 0"""
