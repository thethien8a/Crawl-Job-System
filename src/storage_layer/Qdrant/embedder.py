from __future__ import annotations

import math
from typing import Any

from openrouter import OpenRouter

from src.storage_layer.Qdrant.config import (
    DOCUMENT_EMBEDDING_INPUT_TYPE,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
)
from src.storage_layer.Qdrant.schema import IndexSettings


OPENROUTER_TIMEOUT_MS = 120_000
EMBEDDING_ENCODING_FORMAT = "float"


class OpenRouterEmbeddingClient:
    def __init__(
        self,
        api_key: str,
        base_url: str,
    ) -> None:
        self.client = OpenRouter(
            api_key=api_key,
            server_url=base_url.rstrip("/"),
            timeout_ms=OPENROUTER_TIMEOUT_MS,
        )

    def embed_documents(
        self,
        settings: IndexSettings,
        texts: list[str],
    ) -> list[list[float]]:
        if not texts:
            return []

        response = self.client.embeddings.generate(
            model=settings.embedding_model,
            input=texts,
            dimensions=settings.embedding_dim,
            encoding_format=EMBEDDING_ENCODING_FORMAT,
            input_type=DOCUMENT_EMBEDDING_INPUT_TYPE,
        )
        vectors = _vectors_from_response(response, expected_count=len(texts))
        return [_normalize_vector(vector) for vector in vectors]


def get_embedding_client() -> OpenRouterEmbeddingClient:
    if not OPENROUTER_API_KEY:
        raise RuntimeError("OPENROUTER_API_KEY is required to embed Silver jobs")

    return OpenRouterEmbeddingClient(
        api_key=OPENROUTER_API_KEY,
        base_url=OPENROUTER_BASE_URL,
    )


def embed_documents(
    client: OpenRouterEmbeddingClient,
    settings: IndexSettings,
    texts: list[str],
) -> list[list[float]]:
    return client.embed_documents(settings, texts)


def _vectors_from_response(response: Any, expected_count: int) -> list[list[float]]:
    items = getattr(response, "data", [])
    vectors: list[list[float]] = []
    for item in sorted(items, key=lambda item: item.index or 0):
        embedding = item.embedding
        if isinstance(embedding, str):
            raise RuntimeError("OpenRouter returned base64 embeddings; expected float embeddings")
        vectors.append(list(embedding))

    if len(vectors) != expected_count:
        raise RuntimeError(
            "OpenRouter returned an unexpected embedding count: "
            f"expected={expected_count}, actual={len(vectors)}"
        )

    return vectors


def _normalize_vector(values: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in values))
    if norm == 0:
        return list(values)

    return [value / norm for value in values]
