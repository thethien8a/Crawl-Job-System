"""Browser layer for collecting TopCV listing URLs with nodriver."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random

import nodriver as uc

from .config import (
    BROWSER_ARGS,
    JOB_URL_SELECTOR,
    NEXT_PAGE_SELECTOR,
    PAGE_LOAD_DELAY,
    PAGINATION_DELAY_RANGE,
    SCROLL_INCREMENT,
    SEARCH_URL_TEMPLATE,
)
from .utils import absolute_topcv_url, encode_input

logger = logging.getLogger(__name__)


class TopcvBrowser:
    """Owns the nodriver browser used only for TopCV search pages."""

    def __init__(self, headless: bool = True) -> None:
        self.headless = headless
        self._browser: uc.Browser | None = None
        self._tab: uc.Tab | None = None

    async def __aenter__(self) -> "TopcvBrowser":
        kwargs = {
            "headless": self.headless,
            "sandbox": False,
            "browser_args": list(BROWSER_ARGS),
        }

        chrome_bin = os.getenv("CHROME_BIN")
        if chrome_bin:
            kwargs["browser_executable_path"] = chrome_bin

        self._browser = await uc.start(**kwargs)
        self._tab = self._browser.main_tab
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._browser is None:
            return

        try:
            self._browser.stop()
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.warning("Browser stop raised: %s", e)
        finally:
            self._browser = None
            self._tab = None

    @property
    def tab(self) -> uc.Tab:
        assert self._tab is not None, "TopcvBrowser must be used as `async with`"
        return self._tab

    async def open_search(self, keyword: str) -> None:
        """Navigate to the first TopCV search results page."""
        slug = encode_input(keyword)
        await self.tab.get(SEARCH_URL_TEMPLATE.format(slug=slug))
        await asyncio.sleep(PAGE_LOAD_DELAY)
        logger.info("TopCV search page loaded for keyword: %s", keyword)

    async def get_job_urls_on_page(self) -> list[str]:
        """Scroll the current search page and return absolute detail URLs."""
        await self._scroll_to_bottom()

        links = await self.tab.select_all(JOB_URL_SELECTOR)
        urls: list[str] = []
        for link in links:
            href = link.attrs.get("href")
            absolute_url = absolute_topcv_url(href)
            if absolute_url:
                urls.append(absolute_url)

        logger.info("Found %d TopCV job URLs on current page", len(urls))
        return urls

    async def go_to_next_page(self) -> bool:
        """Follow TopCV's next-page data URL when it exists."""
        try:
            next_url = await self._next_page_url()
            if not next_url:
                logger.info("No TopCV next page URL found")
                return False

            await self.tab.get(next_url)
            await asyncio.sleep(random.uniform(*PAGINATION_DELAY_RANGE))
            return True
        except Exception as e:
            logger.warning("Failed to navigate to next TopCV page: %s", e)
            return False

    async def _next_page_url(self) -> str | None:
        script = f"""
        (() => {{
            const links = Array.from(document.querySelectorAll({json.dumps(NEXT_PAGE_SELECTOR)}));
            for (const link of links) {{
                const rawUrl = link.getAttribute("data-href") || link.getAttribute("href");
                if (rawUrl) {{
                    return new URL(rawUrl, window.location.href).href;
                }}
            }}
            return null;
        }})()
        """
        next_url = await self.tab.evaluate(script, return_by_value=True)
        return next_url if isinstance(next_url, str) else None

    async def _scroll_to_bottom(self) -> None:
        try:
            last_height = await self._wait_for_body_height()
            if last_height is None:
                return

            current_position = 0
            stable_rounds = 0
            while stable_rounds < 2:
                current_position = min(
                    current_position + SCROLL_INCREMENT,
                    last_height + SCROLL_INCREMENT,
                )
                await self.tab.evaluate(f"window.scrollTo(0, {current_position});")
                await asyncio.sleep(random.uniform(0.8, 1.5))

                new_height = await self._body_height()
                if new_height is not None and new_height > last_height:
                    last_height = new_height
                    stable_rounds = 0
                elif current_position >= last_height:
                    stable_rounds += 1
                    await asyncio.sleep(1.0)
        except Exception as e:
            logger.exception("Error during TopCV search-page scrolling: %s", e)

    async def _wait_for_body_height(self) -> int | None:
        for _ in range(20):
            height = await self._body_height()
            if height is not None and height > 200:
                return height
            await asyncio.sleep(0.5)

        logger.warning("TopCV search page body never grew; aborting scroll")
        return None

    async def _body_height(self) -> int | None:
        height = await self.tab.evaluate(
            "document.body.scrollHeight",
            return_by_value=True,
        )
        return height if isinstance(height, int) else None
