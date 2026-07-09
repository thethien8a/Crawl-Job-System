from qdrant_client import QdrantClient

from src.storage_layer.Qdrant.config import QDRANT_API_KEY, QDRANT_TIMEOUT, QDRANT_URL


def get_qdrant_client() -> QdrantClient:
    if not QDRANT_URL:
        raise RuntimeError("QDRANT_URL is required to index Silver jobs into Qdrant")

    return QdrantClient(
        url=QDRANT_URL,
        api_key=QDRANT_API_KEY or None,
        timeout=QDRANT_TIMEOUT,
    )
