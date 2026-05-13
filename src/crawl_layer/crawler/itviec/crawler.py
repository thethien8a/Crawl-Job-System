"""ITviec crawler — top-level orchestration.

Wires together:
  * `ItviecBrowser` — owns the nodriver session, handles login + navigation.
  * `ItviecParser`  — turns preview-panel HTML into ITViecJobItem instances.

Keeps only the high-level flow here: log in, walk search-result pages,
dedupe URLs, harvest items.
"""

from __future__ import annotations

import logging
from dataclasses import asdict

from src.crawl_layer.data_model.data_class import ITViecJobItem
from src.crawl_layer.utils.loader import save_to_temp
from src.crawl_layer.crawler.itviec.config import SOURCE_NAME, ENTITY_NAME

from .browser import ItviecBrowser, ItviecLoginError
from .parser import ItviecParser

logger = logging.getLogger(__name__)


class ItviecCrawler:
    """Async ITviec crawler.

    Strategy:
        * Single browser session (ITviec ties Cloudflare clearance to UA + IP
          + a logged-in cookie, so concurrency would defeat the point).
        * Per-card click + side-panel snapshot, parsed with parsel.
        * Pagination capped by `max_pages` to mirror the original spider's
          polite default.
    """

    def __init__(
        self,
        keyword: str = "data analyst",
        max_pages: int = 3,
        headless: bool = True,
    ) -> None:
        self.keyword = keyword
        self.max_pages = max_pages
        self.headless = headless

        self.browser = ItviecBrowser(headless=headless)
        self.parser = ItviecParser()

        self._seen_urls: set[str] = set()

    # -- public entry point -------------------------------------------------
    async def crawl(self) -> list[ITViecJobItem]:
        items: list[ITViecJobItem] = []

        async with self.browser:
            try:
                await self.browser.login()
            except ItviecLoginError as e:
                logger.error("Login failed — aborting crawl: %s", e)
                return items

            await self.browser.open_search(self.keyword)

            pages_done = 0
            while pages_done < self.max_pages:
                temp_items: list[ITViecJobItem] = []
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
    async def _scrape_current_page(self) -> list[ITViecJobItem]:
        page_items: list[ITViecJobItem] = []
        async for job_url, panel_html in self.browser.iter_job_panels():
            if job_url in self._seen_urls:
                continue
            self._seen_urls.add(job_url)

            try:
                item = self.parser.parse_preview_panel(
                    panel_html, job_url, self.keyword
                )
            except Exception as e:
                logger.warning("Parse failed for %s: %s", job_url, e)
                continue

            page_items.append(item)
        return page_items

    # -- incremental batch save --------------------------------------------
    def _flush_batch(self, page_items: list[ITViecJobItem], page_num: int) -> None:
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
