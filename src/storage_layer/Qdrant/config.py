import os

from dotenv import load_dotenv

from src.storage_layer.MinIO_S3.config.path import DEFAULT_ENTITY_NAME

load_dotenv()

SITES = [
    "topcv",
    "itviec",
    "vietnamworks",
]

SILVER_ENTITY_NAME = DEFAULT_ENTITY_NAME

QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "jobs_v2")
# Default qdrant-client write timeout (5s) is too short for upserting
# large batches of high-dimensional vectors over the network.
QDRANT_TIMEOUT = int(os.getenv("QDRANT_TIMEOUT", "60"))

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "openai/text-embedding-3-small")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "768"))
DOCUMENT_EMBEDDING_INPUT_TYPE = os.getenv("DOCUMENT_EMBEDDING_INPUT_TYPE", "search_document")

QDRANT_BATCH_SIZE = int(os.getenv("QDRANT_BATCH_SIZE", "64"))
QDRANT_RETENTION_DAYS = 30
QDRANT_VIETNAMWORKS_RETENTION_DAYS = 3
