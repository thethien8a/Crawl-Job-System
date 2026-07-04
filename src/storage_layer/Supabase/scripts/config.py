import os
from dotenv import load_dotenv
from dataclasses import fields
from typing import get_origin, get_args
from src.storage_layer.MinIO_S3.config.path import DEFAULT_ENTITY_NAME
from src.storage_layer.Supabase.schema.data_class import JobData

load_dotenv()

# Sites to read from MinIO/S3 Silver layer
SITES = [
    'topcv',
    'itviec',
    'vietnamworks'
]

# Supabase configuration
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_DATABASE = os.getenv("SUPABASE_DATABASE")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")

# Silver layer entity name; matches the MinIO Silver path convention "jobs/source_site=.../..."
SILVER_ENTITY_NAME = DEFAULT_ENTITY_NAME

# Target Supabase table and the column used as the UPSERT conflict key
TARGET_TABLE = "ready_jobs"
CONFLICT_KEY = "unique_url"

# Rows per bulk upsert; large enough to amortize roundtrip latency without blowing the wire protocol
BATCH_SIZE = 100


# Python type annotation → PostgreSQL type mapping
_TYPE_MAP = {
    str: "TEXT",
    int: "INTEGER",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
}


def _pg_type(python_type) -> str:
    """Map a Python type annotation to its PostgreSQL column type.

    Generic aliases like ``list[str]`` are resolved via
    ``typing.get_origin`` / ``typing.get_args`` so that ``list[str]``
    becomes ``TEXT[]``, ``list[int]`` becomes ``INTEGER[]``, etc.
    """
    origin = get_origin(python_type)
    if origin is list:
        args = get_args(python_type)
        if args:
            inner = _TYPE_MAP.get(args[0], "TEXT")
            return f"{inner}[]"
        return "TEXT[]"
    return _TYPE_MAP.get(python_type, "TEXT")


JOB_DATA_COLUMNS = [f.name for f in fields(JobData)]
_UPDATE_COLUMNS = [c for c in JOB_DATA_COLUMNS if c != CONFLICT_KEY]

# Build column definitions with proper PostgreSQL types derived from JobData
_JOB_DATA_PG_COLUMNS = [f"{f.name} {_pg_type(f.type)}" for f in fields(JobData)]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    {', '.join(_JOB_DATA_PG_COLUMNS)}
);
"""

SCHEMA_MIGRATION_SQLS = (
    f"ALTER TABLE {TARGET_TABLE} ADD COLUMN IF NOT EXISTS {CONFLICT_KEY} TEXT;",
    f"""
    WITH backfilled AS (
        SELECT
            ctid,
            CASE
                WHEN lower(trim(source_site)) = 'itviec'
                    THEN regexp_replace(regexp_replace(job_url, '[?#].*$', ''), '-[0-9]+/?$', '')
                ELSE regexp_replace(job_url, '[?#].*$', '')
            END AS backfilled_unique_url
        FROM {TARGET_TABLE}
        WHERE {CONFLICT_KEY} IS NULL
          AND job_url IS NOT NULL
    ),
    duplicate_null_rows AS (
        SELECT ctid
        FROM (
            SELECT
                ctid,
                row_number() OVER (
                    PARTITION BY backfilled_unique_url
                    ORDER BY ctid DESC
                ) AS duplicate_rank
            FROM backfilled
            WHERE backfilled_unique_url IS NOT NULL
              AND btrim(backfilled_unique_url) <> ''
        ) ranked
        WHERE duplicate_rank > 1
    ),
    rows_conflicting_with_existing_key AS (
        SELECT backfilled.ctid
        FROM backfilled
        WHERE backfilled.backfilled_unique_url IS NOT NULL
          AND btrim(backfilled.backfilled_unique_url) <> ''
          AND EXISTS (
              SELECT 1
              FROM {TARGET_TABLE} existing
              WHERE existing.{CONFLICT_KEY} = backfilled.backfilled_unique_url
          )
    ),
    rows_to_delete AS (
        SELECT ctid FROM duplicate_null_rows
        UNION
        SELECT ctid FROM rows_conflicting_with_existing_key
    )
    DELETE FROM {TARGET_TABLE} target
    USING rows_to_delete
    WHERE target.ctid = rows_to_delete.ctid;
    """,
    f"""
    UPDATE {TARGET_TABLE}
    SET {CONFLICT_KEY} = CASE
        WHEN lower(trim(source_site)) = 'itviec'
            THEN regexp_replace(regexp_replace(job_url, '[?#].*$', ''), '-[0-9]+/?$', '')
        ELSE regexp_replace(job_url, '[?#].*$', '')
    END
    WHERE {CONFLICT_KEY} IS NULL
      AND job_url IS NOT NULL;
    """,
    f"""
    DELETE FROM {TARGET_TABLE} stale
    USING {TARGET_TABLE} kept
    WHERE stale.ctid < kept.ctid
      AND stale.{CONFLICT_KEY} IS NOT NULL
      AND stale.{CONFLICT_KEY} = kept.{CONFLICT_KEY};
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS {TARGET_TABLE}_{CONFLICT_KEY}_uidx
    ON {TARGET_TABLE} ({CONFLICT_KEY});
    """,
)

UPSERT_SQL = (
    f"INSERT INTO {TARGET_TABLE} ({', '.join(JOB_DATA_COLUMNS)}) "
    "VALUES %s "
    f"ON CONFLICT ({CONFLICT_KEY}) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATE_COLUMNS)
)
