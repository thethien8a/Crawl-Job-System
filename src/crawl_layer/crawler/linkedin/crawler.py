"""LinkedIn crawler — top-level orchestration.

Wires together:
  * `LinkedinBrowser` — owns the nodriver session, handles login + navigation.
  * `LinkedinParser`  — turns detail-panel HTML into LinkedinJobItem instances.

Keeps only the high-level flow here: log in, walk search-result pages,
dedupe URLs, harvest items.
"""

from __future__ import annotations

import logging
from dataclasses import asdict

from src.crawl_layer.data_model.data_class import LinkedinJobItem
from src.crawl_layer.utils.loader import save_to_temp

from .browser import LinkedinBrowser, LinkedinLoginError
from .config import DEFAULT_KEYWORD, DEFAULT_LOCATION, DEFAULT_MAX_PAGES
from .parser import LinkedinParser

logger = logging.getLogger(__name__)

SOURCE_NAME = "linkedin"
ENTITY_NAME = "jobs"


class LinkedinCrawler:
    """Async LinkedIn crawler.

    Strategy:
        * Single browser session (LinkedIn ties anti-bot heuristics to a
          stable cookie + UA, so concurrency would defeat the point).
        * Per-card click + side-panel snapshot, parsed with parsel.
        * Pagination capped by `max_pages` to mirror the original spider's
          polite default.
    """

    def __init__(
        self,
        keyword: str = DEFAULT_KEYWORD,
        location: str = DEFAULT_LOCATION,
        max_pages: int = DEFAULT_MAX_PAGES,
        headless: bool = True,
    ) -> None:
        self.keyword = keyword
        self.location = location
        self.max_pages = max_pages
        self.headless = headless

        self.browser = LinkedinBrowser(headless=headless)
        self.parser = LinkedinParser()

        self._seen_urls: set[str] = set()

    # -- public entry point -------------------------------------------------
    async def crawl(self) -> list[LinkedinJobItem]:
        items: list[LinkedinJobItem] = []

        async with self.browser:
            # Attempt to login up to 3 times
            for i in range(3):
                try:
                    await self.browser.login()
                    break
                except LinkedinLoginError as e:
                    logger.error("Login attempt %d failed: %s", i + 1, e)
                    if i == 2:
                        logger.error("All login attempts failed — aborting crawl")
                        return items
                        
            logger.info("Login successful")
            await self.browser.open_search(self.keyword, self.location)

            pages_done = 0
            while pages_done < self.max_pages:
                temp_items: list[LinkedinJobItem] = []
                pages_done += 1
                logger.info("Scraping page %d/%d", pages_done, self.max_pages)
                page_items = await self._scrape_current_page()
                # Append total jobs scrape
                items.extend(page_items)
                # Current page jobs
                temp_items.extend(page_items)
                self._flush_batch(temp_items, pages_done)

                if pages_done >= self.max_pages:
                    break
                if not await self.browser.go_to_next_page():
                    break

        logger.info("Crawl finished — collected %d items", len(items))
        return items

    # -- per-page scrape ----------------------------------------------------
    async def _scrape_current_page(self) -> list[LinkedinJobItem]:
        page_items: list[LinkedinJobItem] = []
        async for panel_html in self.browser.iter_job_panels():
            try:
                item = self.parser.parse_detail_panel(
                    panel_html, self.keyword
                )
            except Exception as e:
                logger.warning("Parse failed: %s", e)
                continue

            if item.job_title is None:
                continue
            page_items.append(item)
        return page_items

    # -- incremental batch save --------------------------------------------
    def _flush_batch(self, page_items: list[LinkedinJobItem], page_num: int) -> None:
        """Persist one page's worth of items to the local temp file.

        Streaming page-by-page avoids losing all progress if a later page
        crashes mid-crawl, and keeps memory bounded for long runs.
        """
        if not page_items:
            return
        save_to_temp(
            [asdict(item) for item in page_items], SOURCE_NAME, ENTITY_NAME
        )
        logger.info(
            "Flushed %d items from page %d to temp", len(page_items), page_num
        )
