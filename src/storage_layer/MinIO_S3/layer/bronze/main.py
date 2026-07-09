"""Upload local temp JSONL to MinIO Bronze layer.

Usage:
    python -m src.storage_layer.MinIO_S3.layer.bronze.main              # all sites
    python -m src.storage_layer.MinIO_S3.layer.bronze.main --source topcv  # one site
"""

import argparse

from src.storage_layer.MinIO_S3.config.path import get_bronze_bucket_name
from src.crawl_layer.utils.loader import load_to_bronze

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        default=None,
        choices={"topcv", "itviec", "vietnamworks", None},
        help="Upload only the given source; omit to upload all.",
    )
    args = parser.parse_args()

    bronze_bucket_name = get_bronze_bucket_name()
    load_to_bronze(bronze_bucket_name, source_filter=args.source)