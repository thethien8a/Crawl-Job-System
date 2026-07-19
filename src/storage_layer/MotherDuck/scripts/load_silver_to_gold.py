import logging

from src.storage_layer.MotherDuck.client import MotherDuckClient
from src.storage_layer.MotherDuck.config import (
    GOLD_BENEFITS_TABLE,
    GOLD_DIM_DATE_TABLE,
    GOLD_INDUSTRIES_TABLE,
    GOLD_JOBS_TABLE,
    GOLD_REQUIREMENTS_TABLE,
    GOLD_SCHEMA,
    MOTHERDUCK_DATABASE,
)
from src.storage_layer.MotherDuck.scripts.gold_sql_builder import (
    GoldStatement,
    build_all_gold_sql,
)
from src.storage_layer.MotherDuck.scripts.gold_sql_expressions import (
    qualified,
    read_silver_parquet_sql,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GOLD_TABLES = (
    GOLD_JOBS_TABLE,
    GOLD_DIM_DATE_TABLE,
    GOLD_INDUSTRIES_TABLE,
    GOLD_BENEFITS_TABLE,
    GOLD_REQUIREMENTS_TABLE,
)


def get_silver_columns(client: MotherDuckClient) -> set[str]:
    rows = client.con.sql(
        f"""
        DESCRIBE SELECT *
        FROM {read_silver_parquet_sql()}
        """
    ).fetchall()
    return {row[0] for row in rows}


def ensure_gold_database(client: MotherDuckClient) -> None:
    client.execute_statement(f'CREATE DATABASE IF NOT EXISTS "{MOTHERDUCK_DATABASE}"')
    client.execute_statement(f'USE "{MOTHERDUCK_DATABASE}"')
    client.execute_statement(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")


def execute_gold_statements(
    client: MotherDuckClient,
    statements: list[GoldStatement],
) -> None:
    client.execute_statement("BEGIN TRANSACTION")
    try:
        for description, sql in statements:
            logger.info("Executing: %s", description)
            client.execute_statement(sql)
        client.execute_statement("COMMIT")
    except Exception:
        try:
            client.execute_statement("ROLLBACK")
        except Exception:
            logger.exception("Failed to roll back Gold refresh transaction")
        raise


def log_gold_row_counts(client: MotherDuckClient) -> None:
    for table_name in GOLD_TABLES:
        row_count = client.con.sql(
            f"SELECT count(*) FROM {qualified(table_name)}"
        ).fetchone()[0]
        logger.info(
            "%s.%s.%s: %d rows",
            MOTHERDUCK_DATABASE,
            GOLD_SCHEMA,
            table_name,
            row_count,
        )


def main() -> None:
    client = MotherDuckClient()
    client.setup_s3_credentials()
    ensure_gold_database(client)

    silver_columns = get_silver_columns(client)
    statements = build_all_gold_sql(silver_columns)

    execute_gold_statements(client, statements)
    log_gold_row_counts(client)


if __name__ == "__main__":
    main()
