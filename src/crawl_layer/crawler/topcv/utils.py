"""Pure text helpers used across the TopCV crawler.

No I/O, no Selector dependency — anything here must stay trivially testable
with plain string inputs.
"""

from __future__ import annotations

from typing import Iterable


def encode_input(search_word: str) -> str:
    # TopCV expects search slugs as lowercase, hyphen-joined words. Mirrors
    # the original CrawlJob/utils.encode_input so URLs match the spider.
    parts = [w.lower() for w in (search_word or "").split() if w]
    return "-".join(parts)


def join_clean(parts: Iterable[str]) -> str | None:
    # Selectors return lists of text nodes that are usually fragmented by
    # whitespace and inline tags; collapse them and drop empties so callers
    # never have to repeat the same boilerplate.
    cleaned = " ".join(p.strip() for p in parts if p and p.strip())
    return cleaned or None


def sanitize_title(title: str | None, unwanted_fragments: Iterable[str]) -> str | None:
    # TopCV's H1 sometimes inherits sidebar text. Strip known noise rather
    # than guessing with regex — fragments are stable copy from the page.
    if not title:
        return title
    out = title
    for chunk in unwanted_fragments:
        if chunk in out:
            out = out.replace(chunk, "").strip()
    return out
