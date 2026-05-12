"""Small pure helpers for the LinkedIn crawler.

Kept dependency-free so they can be imported from any layer (parser,
browser, crawler) without creating an import cycle.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from typing import Iterable
from urllib.parse import urljoin

from .config import BASE_URL

logger = logging.getLogger(__name__)


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


async def _press_special_key(tab, key: str, code: str, vk_code: int) -> None:
    """Dispatch a non-character key (Tab/Enter/Delete) via raw CDP key events.

    LinkedIn's React layer intercepts JS focus calls; CDP-level key events
    are routed by the browser to the currently-focused element directly,
    bypassing React's onKeyDown synthetic-event interceptor for focus moves.
    """
    import nodriver as uc

    for event_type in ("keyDown", "keyUp"):
        await tab.send(uc.cdp.input_.dispatch_key_event(
            event_type,
            key=key,
            code=code,
            windows_virtual_key_code=vk_code,
            native_virtual_key_code=vk_code,
        ))


async def _select_all_focused(tab) -> None:
    """Send Ctrl+A so any pre-existing value gets overwritten by the next keystrokes."""
    import nodriver as uc

    for event_type in ("keyDown", "keyUp"):
        await tab.send(uc.cdp.input_.dispatch_key_event(
            event_type,
            key="a",
            code="KeyA",
            modifiers=2,  # Ctrl
            windows_virtual_key_code=65,
            native_virtual_key_code=65,
        ))


async def _get_center_via_js(element) -> tuple[float, float] | None:
    """Get the element's viewport-center coordinates via getBoundingClientRect.

    Used as a fallback when CDP ``get_content_quads`` returns nothing —
    for React 18 inputs with synthetic ids (e.g. ``:r4:``) inside
    heavily-styled containers, CDP's layout tree sometimes can't see
    quads even though the element is painted and interactive.
    """
    import json

    raw = await element.apply("""
        (el) => {
            const r = el.getBoundingClientRect();
            return JSON.stringify({
                x: r.left + r.width / 2,
                y: r.top + r.height / 2,
                w: r.width,
                h: r.height,
            });
        }
    """)
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return None
    if not data or data.get("w", 0) <= 0 or data.get("h", 0) <= 0:
        return None
    return (float(data["x"]), float(data["y"]))


async def _wait_for_clickable(element, timeout: float = 5.0) -> tuple[float, float]:
    """Poll until the element exposes valid hit-testable coordinates.

    Tries CDP ``get_position()`` first (which uses ``DOM.getContentQuads``);
    if that returns nothing, falls back to JS ``getBoundingClientRect``.
    Raises if neither yields coordinates within ``timeout`` seconds, so
    the caller never silently no-ops on a missed click.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    last_quads = None
    last_rect = None
    while asyncio.get_event_loop().time() < deadline:
        try:
            pos = await element.get_position()
        except Exception:
            pos = None
        if pos is not None:
            center = pos.center
            if center and center[0] > 0 and center[1] > 0:
                return center
            last_quads = pos

        rect_center = await _get_center_via_js(element)
        if rect_center is not None and rect_center[0] > 0 and rect_center[1] > 0:
            return rect_center
        last_rect = rect_center

        await asyncio.sleep(0.15)
    raise RuntimeError(
        f"Element never became clickable within {timeout}s "
        f"(last quads: {last_quads}, last rect: {last_rect})"
    )


async def _cdp_mouse_click(tab, x: float, y: float) -> None:
    """Dispatch a real CDP mouse click at absolute (x, y) viewport coordinates.

    Going through ``Input.dispatchMouseEvent`` directly (rather than
    ``element.mouse_click()``) lets us control coordinates explicitly and
    add a ``mouseMoved`` step so LinkedIn's hover-tracking heuristics
    don't see a teleporting cursor.
    """
    import nodriver as uc

    await tab.send(uc.cdp.input_.dispatch_mouse_event(
        type_="mouseMoved",
        x=x,
        y=y,
    ))
    await asyncio.sleep(0.05)
    await tab.send(uc.cdp.input_.dispatch_mouse_event(
        type_="mousePressed",
        x=x,
        y=y,
        button=uc.cdp.input_.MouseButton("left"),
        buttons=1,
        click_count=1,
    ))
    await asyncio.sleep(0.05)
    await tab.send(uc.cdp.input_.dispatch_mouse_event(
        type_="mouseReleased",
        x=x,
        y=y,
        button=uc.cdp.input_.MouseButton("left"),
        buttons=1,
        click_count=1,
    ))


async def _is_focused(element) -> bool:
    """True if ``element`` is the document's currently-focused element."""
    try:
        result = await element.apply("(el) => el === document.activeElement")
    except Exception:
        return False
    if isinstance(result, bool):
        return result
    if isinstance(result, str):
        return result.lower() == "true"
    return bool(result)


async def _force_focus(tab, element, center: tuple[float, float]) -> bool:
    """Focus an element through both CDP mouse click and CDP DOM.focus.

    Order matters:
      1. CDP mouse click satisfies LinkedIn's "real interaction" anti-bot
         heuristic and triggers React's onClick / onMouseDown handlers in
         a way it considers legitimate.
      2. CDP ``DOM.focus`` then directly tells the browser's input
         subsystem which element should receive keystrokes — this is
         OS-level and React's synthetic event layer cannot intercept it.

    Returns True if ``document.activeElement`` ended up being our element.
    """
    import nodriver as uc

    await _cdp_mouse_click(tab, center[0], center[1])
    await asyncio.sleep(0.2)

    try:
        await tab.send(uc.cdp.dom.focus(backend_node_id=element.backend_node_id))
    except Exception as e:
        logger.debug("CDP DOM.focus failed: %s", e)

    await asyncio.sleep(0.2)
    return await _is_focused(element)


async def human_like_typing(
    element,
    text: str,
    delay_range: tuple[float, float],
) -> None:
    """Focus an input via CDP, verify focus landed, then type via CDP key events.

    Why this layered approach:
      * ``element.click()`` / ``element.focus()`` / ``element.send_keys()``
        all run in JS — React intercepts them and can yank focus back to
        whatever its state machine thinks is "active".
      * CDP ``Input.dispatchMouseEvent`` is a real OS-level click that
        reaches the input subsystem before React's listeners fire.
      * CDP ``DOM.focus`` is a browser-level focus that React cannot
        intercept; we run it after the click so the browser knows for
        certain which element should receive subsequent key events.
      * We then *verify* focus actually landed via
        ``document.activeElement`` and retry once before typing — this
        catches the race where React re-renders between click and focus.

    Ctrl+A then clears any leftover value so new chars overwrite instead
    of appending.
    """
    import nodriver as uc

    tab = element.tab

    # Cuộn vào tầm nhìn (CDP, không kích hoạt JS handler nào).
    await element.scroll_into_view()
    await asyncio.sleep(0.25)

    # Lấy toạ độ hit-testable: thử CDP quads trước, fallback JS getBoundingClientRect.
    center = await _wait_for_clickable(element, timeout=5.0)
    logger.info("Typing into element at viewport coords (%.1f, %.1f)", *center)

    # Thử focus, nếu chưa được thì retry — chống race với React re-render.
    focused = await _force_focus(tab, element, center)
    if not focused:
        logger.warning(
            "Focus did not land after first attempt — retrying CDP click + DOM.focus"
        )
        await asyncio.sleep(0.3)
        focused = await _force_focus(tab, element, center)
    if not focused:
        raise RuntimeError(
            "Could not focus target input — React keeps yanking focus away. "
            "Check that the selector points to the visible input (not a hidden autofill)."
        )

    # Bôi đen text cũ (nếu có) để các ký tự mới ghi đè thay vì append.
    await _select_all_focused(tab)
    await asyncio.sleep(0.1)

    for char in text:
        await tab.send(uc.cdp.input_.dispatch_key_event("char", text=char))
        await asyncio.sleep(random.uniform(*delay_range))


async def press_tab(tab) -> None:
    """Move focus to the next form field the way a real user would.

    React's state machine respects native Tab navigation, so this is the
    most reliable way to hop from the username field to the password
    field on LinkedIn's login form.
    """
    await _press_special_key(tab, key="Tab", code="Tab", vk_code=9)


async def type_into_focused(
    tab,
    text: str,
    delay_range: tuple[float, float],
) -> None:
    """Type text into the currently-focused element via raw CDP key events.

    Use this after :func:`press_tab` (or any other focus move that
    happened outside JS) so the browser already has the right element
    focused at the input-subsystem level.
    """
    import nodriver as uc

    for char in text:
        await tab.send(uc.cdp.input_.dispatch_key_event("char", text=char))
        await asyncio.sleep(random.uniform(*delay_range))
