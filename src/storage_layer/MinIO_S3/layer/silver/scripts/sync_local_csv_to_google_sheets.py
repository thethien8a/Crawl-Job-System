import argparse
import csv
import logging
from pathlib import Path

from src.storage_layer.MinIO_S3.layer.silver.utils.google_sheets import (
    get_or_create_worksheet,
    open_spreadsheet,
    replace_worksheet_values,
    worksheet_title_for_csv,
)

logger = logging.getLogger(__name__)

SILVER_DIR = Path(__file__).resolve().parent.parent
SEEDS_DIR = SILVER_DIR / "seeds"
CLUSTERS_REVIEW_PATH = SILVER_DIR / "utils" / "clusters_review.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="First-sync local Silver seed CSVs and clusters_review.csv to Google Sheets."
    )
    parser.add_argument(
        "--skip-clusters-review",
        action="store_true",
        help="Only sync files from the seeds folder.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    args = parse_args()
    spreadsheet = open_spreadsheet()

    for csv_path in discover_csv_paths(include_clusters_review=not args.skip_clusters_review):
        values = read_csv_values(csv_path)
        title = worksheet_title_for_csv(csv_path)
        worksheet = get_or_create_worksheet(
            spreadsheet,
            title=title,
            rows=len(values),
            cols=max((len(row) for row in values), default=1),
        )
        replace_worksheet_values(worksheet, values)
        logger.info("Synced %s -> worksheet '%s' (%d rows)", csv_path, title, max(len(values) - 1, 0))


def discover_csv_paths(include_clusters_review: bool) -> list[Path]:
    csv_paths = sorted(SEEDS_DIR.glob("*.csv"))
    if include_clusters_review and CLUSTERS_REVIEW_PATH.exists():
        csv_paths.append(CLUSTERS_REVIEW_PATH)
    return csv_paths


def read_csv_values(csv_path: Path) -> list[list[str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        values = list(csv.reader(file))
    return _rectangular_values(values)


def _rectangular_values(values: list[list[str]]) -> list[list[str]]:
    if not values:
        return []
    width = max(len(row) for row in values)
    return [row + [""] * (width - len(row)) for row in values]


if __name__ == "__main__":
    main()
