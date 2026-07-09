from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from qdrant_client import QdrantClient, models

from src.storage_layer.Qdrant.payload import timestamp_from_date
from src.storage_layer.Qdrant.schema import (
    INDEXED_AT_TS_FIELD,
    PAYLOAD_INDEXES,
    SILVER_DATE_TS_FIELD,
    IndexSettings,
)

logger = logging.getLogger(__name__)


def ensure_collection(client: QdrantClient, settings: IndexSettings) -> None:
    if not client.collection_exists(settings.collection_name):
        client.create_collection(
            collection_name=settings.collection_name,
            vectors_config=models.VectorParams(
                size=settings.embedding_dim,
                distance=models.Distance.COSINE,
            ),
        )
        logger.info("Created Qdrant collection %s", settings.collection_name)

    existing_indexes = _existing_payload_indexes(client, settings)
    for field_name, field_schema in PAYLOAD_INDEXES.items():
        if field_name in existing_indexes:
            continue

        client.create_payload_index(
            collection_name=settings.collection_name,
            field_name=field_name,
            field_schema=field_schema,
        )


def delete_expired_points(client: QdrantClient, settings: IndexSettings):
    cutoff_date = datetime.now(timezone.utc).date() - timedelta(days=settings.retention_days)
    cutoff_ts = timestamp_from_date(cutoff_date)
    if cutoff_ts is None:
        return None

    return client.delete(
        collection_name=settings.collection_name,
        points_selector=models.FilterSelector(
            filter=models.Filter(
                should=[
                    models.FieldCondition(
                        key=SILVER_DATE_TS_FIELD,
                        range=models.Range(lt=cutoff_ts),
                    ),
                    models.FieldCondition(
                        key=INDEXED_AT_TS_FIELD,
                        range=models.Range(lt=cutoff_ts),
                    ),
                ],
            ),
        ),
        wait=True,
    )


def _existing_payload_indexes(client: QdrantClient, settings: IndexSettings) -> set[str]:
    collection = client.get_collection(settings.collection_name)
    payload_schema = getattr(collection, "payload_schema", None) or {}
    return set(payload_schema)
