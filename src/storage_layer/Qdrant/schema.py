from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from qdrant_client import QdrantClient, models

UNIQUE_URL_FIELD = "unique_url"
JOB_URL_FIELD = "job_url"
SILVER_DATE_FIELD = "silver_date"
SILVER_DATE_TS_FIELD = "silver_date_ts"
INDEXED_AT_TS_FIELD = "indexed_at_ts"
MAX_DOCUMENT_TEXT_CHARS = 6000

SKILL_FIELDS = (
    "require_programming_languages",
    "require_frameworks",
    "require_tools",
    "require_cloud_skills",
    "require_knowledge",
    "require_domain_knowledge",
    "require_foreign_languages",
    "require_domain_university",
)

DOCUMENT_TEXT_FIELDS = (
    "requirements_cleaned",
    "job_description_cleaned",
)

LIST_PAYLOAD_FIELDS = (
    "job_title_special_keywords",
    "job_industry_clean",
)

SCALAR_PAYLOAD_FIELDS = (
    "job_type",
    "job_position",
    "salary",
    "min_monthly_salary",
    "max_monthly_salary",
    "is_vietnam",
)

PAYLOAD_INDEXES = {
    UNIQUE_URL_FIELD: models.PayloadSchemaType.KEYWORD,
    JOB_URL_FIELD: models.PayloadSchemaType.KEYWORD,
    "source_site": models.PayloadSchemaType.KEYWORD,
    "clean_location": models.PayloadSchemaType.KEYWORD,
    "deadline_ts": models.PayloadSchemaType.INTEGER,
    SILVER_DATE_TS_FIELD: models.PayloadSchemaType.INTEGER,
    INDEXED_AT_TS_FIELD: models.PayloadSchemaType.INTEGER,
    "min_exp_level": models.PayloadSchemaType.FLOAT,
    "max_exp_level": models.PayloadSchemaType.FLOAT,
    "min_monthly_salary": models.PayloadSchemaType.FLOAT,
    "max_monthly_salary": models.PayloadSchemaType.FLOAT,
    "is_vietnam": models.PayloadSchemaType.KEYWORD,
}


@dataclass(frozen=True)
class DateRange:
    from_date: str
    to_date: str


@dataclass(frozen=True)
class IndexSettings:
    collection_name: str
    embedding_model: str
    embedding_dim: int
    batch_size: int
    retention_days: int


class EmbeddingClient(Protocol):
    def embed_documents(
        self,
        settings: IndexSettings,
        texts: list[str],
    ) -> list[list[float]]:
        ...


@dataclass(frozen=True)
class IndexContext:
    qdrant_client: QdrantClient
    embedding_client: EmbeddingClient
    settings: IndexSettings
