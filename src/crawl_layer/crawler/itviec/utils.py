"""Small pure helpers for the ITviec crawler.

Kept dependency-free so they can be imported from any layer (parser,
browser, crawler) without creating an import cycle.
"""

from __future__ import annotations

from typing import Iterable
from urllib.parse import urljoin

from .config import BASE_URL


def encode_keyword(keyword: str) -> str:
    """Turn a free-text keyword into the URL slug ITviec expects.

    "Data Analyst" -> "data-analyst"
    """
    parts = [w.lower() for w in (keyword or "").split() if w]
    return "-".join(parts)


def join_clean(parts: Iterable[str]) -> str | None:
    """Join element text fragments, trim whitespace, return None if empty."""
    cleaned = " ".join(p.strip() for p in parts if p and p.strip())
    return cleaned or None


def clean_text(text: str | None) -> str | None:
    """Strip surrounding whitespace; collapse empty results to None."""
    if text is None:
        return None
    stripped = text.strip()
    return stripped or None


def absolute_url(href: str | None) -> str | None:
    """Resolve a possibly-relative ITviec URL against BASE_URL."""
    if not href:
        return None
    return urljoin(BASE_URL + "/", href)
