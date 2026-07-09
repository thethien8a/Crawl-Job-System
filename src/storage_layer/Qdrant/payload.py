from __future__ import annotations

from datetime import date, datetime, time, timezone
import math
import uuid

from qdrant_client import models

from src.storage_layer.Qdrant.schema import (
    DOCUMENT_TEXT_FIELDS,
    INDEXED_AT_TS_FIELD,
    JOB_URL_FIELD,
    LIST_PAYLOAD_FIELDS,
    MAX_DOCUMENT_TEXT_CHARS,
    SCALAR_PAYLOAD_FIELDS,
    SILVER_DATE_FIELD,
    SILVER_DATE_TS_FIELD,
    SKILL_FIELDS,
    UNIQUE_URL_FIELD,
)


def current_utc_timestamp() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def document_texts(rows: list[dict]) -> list[str]:
    return [_document_text(row) for row in rows]


def build_points(
    rows: list[dict],
    vectors: list[list[float]],
    indexed_at_ts: int,
) -> list[models.PointStruct]:
    return [
        models.PointStruct(
            id=_point_id(row[UNIQUE_URL_FIELD]),
            vector=vector,
            payload=_payload(row, indexed_at_ts),
        )
        for row, vector in zip(rows, vectors, strict=True)
    ]


def timestamp_from_date(value) -> int | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return int(value.astimezone(timezone.utc).timestamp())

    if isinstance(value, date):
        dt = datetime.combine(value, time.min, tzinfo=timezone.utc)
        return int(dt.timestamp())

    if isinstance(value, str):
        return _timestamp_from_date_string(value)

    return None


def _point_id(unique_url: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, unique_url))


def _payload(row: dict, indexed_at_ts: int) -> dict:
    payload = {
        UNIQUE_URL_FIELD: _clean_scalar(row.get(UNIQUE_URL_FIELD)),
        JOB_URL_FIELD: _clean_scalar(row.get(JOB_URL_FIELD)),
        "job_title": _clean_scalar(row.get("job_title")),
        "company_name": _clean_scalar(row.get("company_name")),
        "location": _clean_scalar(row.get("location")),
        "clean_location": _clean_scalar(row.get("clean_location")),
        "job_deadline": _clean_scalar(row.get("job_deadline")),
        "deadline_ts": timestamp_from_date(row.get("job_deadline")),
        "min_exp_level": _clean_scalar(row.get("min_exp_level")),
        "max_exp_level": _clean_scalar(row.get("max_exp_level")),
        "source_site": _clean_scalar(row.get("source_site")),
        SILVER_DATE_FIELD: _clean_scalar(row.get(SILVER_DATE_FIELD)),
        SILVER_DATE_TS_FIELD: timestamp_from_date(row.get(SILVER_DATE_FIELD)),
        INDEXED_AT_TS_FIELD: indexed_at_ts,
    }
    payload.update({field: _clean_list(row.get(field)) for field in SKILL_FIELDS})
    payload.update({field: _clean_list(row.get(field)) for field in LIST_PAYLOAD_FIELDS})
    payload.update({field: _clean_scalar(row.get(field)) for field in SCALAR_PAYLOAD_FIELDS})
    payload["title"] = payload.get("job_title")
    payload["company"] = payload.get("company_name")

    return {key: value for key, value in payload.items() if value is not None}


def _document_text(row: dict) -> str:
    sections = [
        _clean_scalar(row.get("job_title")),
    ]

    sections.extend(_skill_text(field, row.get(field)) for field in SKILL_FIELDS)
    sections.extend(_prefixed_text(field, row.get(field)) for field in DOCUMENT_TEXT_FIELDS)

    text = "\n".join(section for section in sections if section)
    return (text or str(row.get(UNIQUE_URL_FIELD)))[:MAX_DOCUMENT_TEXT_CHARS]


def _prefixed_text(label: str, value) -> str | None:
    cleaned = _clean_scalar(value)
    if cleaned is None:
        return None

    return f"{label}: {cleaned}"


def _skill_text(field_name: str, value) -> str | None:
    values = _clean_list(value)
    if not values:
        return None

    return f"{field_name}: {', '.join(values)}"


def _clean_list(value) -> list[str]:
    if value is None:
        return []

    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if item is not None and str(item).strip()]

    cleaned = str(value).strip()
    return [cleaned] if cleaned else []


def _clean_scalar(value):
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None

    return value


def _timestamp_from_date_string(value: str) -> int | None:
    stripped = value.strip()
    if not stripped:
        return None

    for date_format in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            parsed = datetime.strptime(stripped, date_format).date()
            return timestamp_from_date(parsed)
        except ValueError:
            continue

    return None
