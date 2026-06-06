"""Load taxonomy CSVs from the Silver layer seeds to MotherDuck Gold layer.

This script reads all `*_taxonomy.csv` files from the Silver seeds directory
and creates dimension tables in the Gold schema (e.g., `gold.dim_industry_taxonomy`).
Mapping files (`*_mapping.csv`) are explicitly ignored.

Run:
    python -m src.storage_layer.MotherDuck.scripts.load_taxonomy_to_gold
"""

import logging
from pathlib import Path

from src.storage_layer.MotherDuck.client import MotherDuckClient
from src.storage_layer.MotherDuck.config import GOLD_SCHEMA, MOTHERDUCK_DATABASE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    client = MotherDuckClient()

    client.execute_statement(f'CREATE DATABASE IF NOT EXISTS "{MOTHERDUCK_DATABASE}"')
    client.execute_statement(f'USE "{MOTHERDUCK_DATABASE}"')
    client.execute_statement(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

    # Resolve the path to the seeds directory
    seeds_dir = (
        Path(__file__).resolve().parents[2] / "MinIO_S3" / "layer" / "silver" / "seeds"
    )

    if not seeds_dir.exists():
        logger.error("Seeds directory not found at: %s", seeds_dir)
        return

    # Find all taxonomy files, ignoring anything else (like _mapping.csv)
    csv_files = list(seeds_dir.glob("*_taxonomy.csv"))

    if not csv_files:
        logger.info("No taxonomy CSVs found in %s", seeds_dir)
        return

    for csv_file in csv_files:
        table_base_name = csv_file.stem  # e.g., "industry_taxonomy"
        table_name = f"dim_{table_base_name}"
        qualified_table = f"{GOLD_SCHEMA}.{table_name}"

        logger.info("Loading %s into %s", csv_file.name, qualified_table)

        # Ensure forward slashes for DuckDB's read_csv
        file_path_str = str(csv_file).replace("\\", "/")

        sql = f"""
        CREATE OR REPLACE TABLE {qualified_table} AS
        SELECT * FROM read_csv_auto('{file_path_str}', header=True)
        """

        client.execute_statement(sql)

        # Print final row counts for verification
        row_count = client.con.sql(f"SELECT count(*) FROM {qualified_table}").fetchone()[
            0
        ]
        logger.info(
            "%s.%s: %d rows", MOTHERDUCK_DATABASE, qualified_table, row_count
        )


if __name__ == "__main__":
    main()
