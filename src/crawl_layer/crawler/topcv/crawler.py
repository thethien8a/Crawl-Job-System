"""TopCV crawler — top-level orchestration.

Wires together:
  * `TopcvBrowser`    — owns the nodriver session for search-page URL collection.
  * `TopcvHttpClient` — owns the curl_cffi session and nodriver warm-up.
  * `TopcvParser`     — turns HTML into TopCVJobItem instances.

Keeps only the high-level flow here: paginate rendered search results, dedupe
URLs, fan out detail-page fetches, gather results.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict

from src.crawl_layer.data_model.data_class import TopCVJobItem
from src.crawl_layer.utils.loader import save_to_temp
from src.storage_layer.MinIO_S3.config.path import DEFAULT_ENTITY_NAME

from .config import BASE_URL
from .browser import TopcvBrowser
from .http_client import TopcvHttpClient
from .parser import TopcvParser

logger = logging.getLogger(__name__)

SOURCE_NAME = "topcv"
ENTITY_NAME = DEFAULT_ENTITY_NAME


class TopcvCrawler:
    """Async TopCV crawler.

    Strategy:
        * Render search-result pages with nodriver, collect unique job URLs.
        * Concurrently fetch each detail page (bounded by a semaphore).
        * Per-host throttling and exponential backoff replace the role of
          Scrapy's RetryMiddleware + AutoThrottle so we stay well clear of 429s.
    """

    def __init__(
        self,
        keyword: str = "data analyst",
        max_pages: int = 5,
        concurrency: int = 5,
        request_delay: tuple[float, float] = (4.0, 6.0),
        max_retries: int = 5,
        timeout: float = 30.0,
        headless: bool = True,
    ) -> None:
        self.keyword = keyword
        self.max_pages = max_pages

        self.browser = TopcvBrowser(headless=headless)
        self.http = TopcvHttpClient(
            concurrency=concurrency,
            request_delay=request_delay,
            max_retries=max_retries,
            timeout=timeout,
        )
        self.parser = TopcvParser()

        self._seen_urls: set[str] = set()

    # -- public entry point -------------------------------------------------
    async def crawl(self) -> list[TopCVJobItem]:
        """Walk search pages one at a time, fetching + saving details per page.

        Per-page streaming means each search page's detail items are flushed
        to the temp file before we move on — so a crash on page 5 still
        leaves pages 1..4 safely on disk.
        """
        items: list[TopCVJobItem] = []

        async with self.browser, self.http:
            await self.browser.open_search(self.keyword)

            for page_num in range(1, self.max_pages + 1):
                temp_items: list[TopCVJobItem] = []
                logger.info("Scanning TopCV URLs on page %d/%d", page_num, self.max_pages)

                page_urls = await self._collect_current_page_urls()
                if not page_urls:
                    if not await self.browser.go_to_next_page():
                        break
                    continue

                page_items = await self._scrape_details_batch(page_urls)
                
                # Append total jobs scrape
                items.extend(page_items)

                # Current page jobs
                temp_items.extend(page_items)
                self._flush_batch(temp_items, page_num)

                if page_num >= self.max_pages:
                    break
                if not await self.browser.go_to_next_page():
                    break

        return items

    # -- search pagination (single page) -----------------------------------
    async def _collect_current_page_urls(self) -> list[str]:
        """Return new job URLs from the currently rendered search page."""
        page_urls = await self.browser.get_job_urls_on_page()
        new_urls: list[str] = []
        for url in page_urls:
            if url and url not in self._seen_urls:
                self._seen_urls.add(url)
                new_urls.append(url)
        return new_urls

    # -- batch detail fetch -------------------------------------------------
    async def _scrape_details_batch(self, urls: list[str]) -> list[TopCVJobItem]:
        """Concurrently fetch + parse a batch of detail URLs from one search page."""
        tasks = [self._scrape_detail(u) for u in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        page_items: list[TopCVJobItem] = []
        for url, result in zip(urls, results):
            if isinstance(result, Exception):
                logger.warning("Detail crawl failed for %s: %s", url, result)
            elif result is not None:
                page_items.append(result)
        return page_items

    # -- detail page --------------------------------------------------------
    async def _scrape_detail(self, url: str) -> TopCVJobItem | None:
        html = await self.http.fetch(url, referer=BASE_URL)
        if not html:
            return None
        return self.parser.parse_job_detail(html, url, self.keyword)

    # -- incremental batch save --------------------------------------------
    def _flush_batch(self, page_items: list[TopCVJobItem], page_num: int) -> None:
        """Persist one search page's detail items to the local temp file."""
        if not page_items:
            return
        save_to_temp(
            [asdict(item) for item in page_items], SOURCE_NAME, ENTITY_NAME
        )
        logger.info(
            "Flushed %d items from page %d to temp", len(page_items), page_num
        )
