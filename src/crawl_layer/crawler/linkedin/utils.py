"""Small pure helpers for the LinkedIn crawler.

Kept dependency-free so they can be imported from any layer (parser,
browser, crawler) without creating an import cycle.
"""

from __future__ import annotations

import asyncio
import random
import re
from typing import Iterable
from urllib.parse import urljoin

from .config import BASE_URL


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
    """Resolve a possibly-relative LinkedIn URL against BASE_URL."""
    if not href:
        return None
    return urljoin(BASE_URL + "/", href)


def split_info_parts(text: str) -> list[str]:
    """Split a LinkedIn info bar into trimmed, non-empty fragments.

    LinkedIn separates "Industry · 1,001-5,000 employees · 12K followers"
    style strings with bullets, dots, pipes, and stray whitespace.
    """
    return [p.strip() for p in re.split(r"[·\n\r\t●•|]", text) if p.strip()]


async def human_like_typing(
    element,
    text: str,
    delay_range: tuple[float, float],
) -> None:
    """Send keys character-by-character with a small random pause.

    LinkedIn's bot detection tracks typing cadence; bursting the whole
    string in one call is a clear automation signal.
    """
    for char in text:
        await element.send_keys(char)
        await asyncio.sleep(random.uniform(*delay_range))


async def type_into_focused(
    tab,
    text: str,
    delay_range: tuple[float, float],
) -> None:
    """Type text into the currently-focused element via raw CDP key events.

    Unlike ``Element.send_keys`` (which re-runs ``elem.focus()`` per char
    via JavaScript), this only dispatches the char event. Pair it with an
    explicit ``DOM.focus`` call so LinkedIn's React focus handlers cannot
    yank focus back to the email field between keystrokes.
    """
    import nodriver as uc

    for char in text:
        await tab.send(uc.cdp.input_.dispatch_key_event("char", text=char))
        await asyncio.sleep(random.uniform(*delay_range))
