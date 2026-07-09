"""Index Silver-layer job data into Qdrant for CV recommendations.

Run:
    python -m src.storage_layer.Qdrant.scripts.index_silver_to_qdrant \
        --from_date 2026-06-01 --to_date 2026-07-04
"""

from __future__ import annotations

import argparse
import logging

from src.storage_layer.Qdrant.client import get_qdrant_client
from src.storage_layer.Qdrant.collection import delete_expired_points, ensure_collection
from src.storage_layer.Qdrant.config import (
    EMBEDDING_DIM,
    EMBEDDING_MODEL,
    QDRANT_BATCH_SIZE,
    QDRANT_COLLECTION,
    QDRANT_RETENTION_DAYS,
    SITES,
)
from src.storage_layer.Qdrant.embedder import embed_documents, get_embedding_client
from src.storage_layer.Qdrant.payload import build_points, current_utc_timestamp, document_texts
from src.storage_layer.Qdrant.schema import DateRange, IndexContext, IndexSettings
from src.storage_layer.Qdrant.silver import load_latest_silver_jobs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from_date", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--to_date", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument(
        "--retention_days",
        type=int,
        default=QDRANT_RETENTION_DAYS,
        help="Keep only Qdrant points from the most recent N Silver days",
    )
    args = parser.parse_args()

    if args.retention_days <= 0:
        raise ValueError("--retention_days must be greater than 0")

    settings = IndexSettings(
        collection_name=QDRANT_COLLECTION,
        embedding_model=EMBEDDING_MODEL,
        embedding_dim=EMBEDDING_DIM,
        batch_size=QDRANT_BATCH_SIZE,
        retention_days=args.retention_days,
    )
    context = IndexContext(
        qdrant_client=get_qdrant_client(),
        embedding_client=get_embedding_client(),
        settings=settings,
    )

    ensure_collection(context.qdrant_client, context.settings)

    date_range = DateRange(args.from_date, args.to_date)
    total_upserted = 0
    for site in SITES:
        try:
            total_upserted += _index_site(context, site, date_range)
        except Exception:
            logger.exception("Failed indexing site %s; skipping", site)

    deleted = delete_expired_points(context.qdrant_client, context.settings)
    logger.info(
        "Qdrant indexing finished: upserted=%d, expired_delete_operation=%s",
        total_upserted,
        deleted,
    )


def _index_site(context: IndexContext, site: str, date_range: DateRange) -> int:
    df = load_latest_silver_jobs(site, date_range)
    if df is None:
        return 0

    if df.is_empty():
        logger.info("Silver returned 0 rows for %s after unique_url dedup", site)
        return 0

    upserted = 0
    for chunk in df.iter_slices(context.settings.batch_size):
        rows = chunk.to_dicts()
        vectors = embed_documents(
            context.embedding_client,
            context.settings,
            document_texts(rows),
        )
        points = build_points(rows, vectors, current_utc_timestamp())
        if not points:
            continue

        context.qdrant_client.upsert(
            collection_name=context.settings.collection_name,
            points=points,
            wait=True,
        )
        upserted += len(points)
        logger.debug("Currently upserted %d Qdrant points for %s", upserted, site)

    logger.info("Successfully upserted %d Qdrant points for %s", upserted, site)
    return upserted


if __name__ == "__main__":
    main()
