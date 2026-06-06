"""Build the Gold layer in MotherDuck from Silver Parquet on S3.

Gold is a curated projection of Silver for BI (Power BI via MotherDuck),
modeled as a star schema: a ``gold.jobs`` fact (one row per ``job_url``) with
``dim_date`` / ``dim_source`` dimensions and multi-valued bridge tables.

The Silver cleaning pipeline can run more than once a day (each run appends a
new ``clean_bronze_<timestamp>.parquet`` under the same day partition) and a job
is re-crawled across days, so the same job appears in several files. A single
window function keeps only the newest snapshot per job:

    newest day wins, and within a day the newest file (``filename``) wins.

MotherDuck reads Silver straight from S3 via the persistent secret created by
``MotherDuckClient.setup_s3_credentials`` -- no data flows through this process.

Data is currently small, so every run does a full refresh with
``CREATE OR REPLACE TABLE`` (fully idempotent, no stale-row bookkeeping).

**Table layout** — a star schema for Power BI.

Fact:
- ``gold.jobs`` — one row per ``job_url`` (degenerate key); scalar columns plus
  the ``source_site`` and ``date_key`` foreign keys.

Dimensions:
- ``gold.dim_date`` — one row per calendar date (contiguous): ``date_key``
  (yyyymmdd) + ``full_date`` and calendar attributes for time intelligence.

Multi-valued bridges (join to ``gold.jobs`` on ``job_url``):
- ``gold.job_industries`` — unnested ``job_industry_clean`` ``(job_url, industry)``.
- ``gold.job_benefits`` — unnested ``benefits_categories_vi`` ``(job_url, benefit)``.
- ``gold.job_requirements`` — unnested ``require_*`` + ``job_title_special_keywords``
  ``(job_url, requirement_type, value)``.

A temporary ``gold._staging`` table is created first so S3 is scanned exactly
once; it is dropped after all target tables are rebuilt.

Run:
    python -m src.storage_layer.MotherDuck.scripts.load_silver_to_gold
"""

import logging

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _qualified(table_name: str) -> str:
    """Return ``gold.<table_name>``."""
    return f"{GOLD_SCHEMA}.{table_name}"


def build_all_gold_sql() -> list[tuple[str, str]]:
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
        GOLD_JOBS_COLUMNS + list_col_names + GOLD_DATE_COLUMNS
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
    FROM read_parquet(
        '{SILVER_PARQUET_GLOB}',
        hive_partitioning = true,
        filename = true
    )
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

    statements = build_all_gold_sql()

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
