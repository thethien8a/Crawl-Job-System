from __future__ import annotations
from typing import Iterable


def encode_input(search_word: str) -> str:
    parts = [w.lower() for w in (search_word or "").split() if w]
    return "-".join(parts)


def join_clean(parts: Iterable[str]) -> str | None:
    cleaned = " ".join(p.strip() for p in parts if p and p.strip())
    return cleaned or None


def sanitize_title(title: str | None, unwanted_fragments: Iterable[str]) -> str | None:
    if not title:
        return title
    out = title
    for chunk in unwanted_fragments:
        if chunk in out:
            out = out.replace(chunk, "").strip()
    return out
