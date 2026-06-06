"""TopCV crawler — top-level orchestration.

Wires together:
  * `TopcvHttpClient` — owns the curl_cffi session and nodriver warm-up.
  * `TopcvParser`     — turns HTML into TopCVJobItem instances.

Keeps only the high-level flow here: paginate search results, dedupe URLs,
fan out detail-page fetches, gather results.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict

from src.crawl_layer.data_model.data_class import TopCVJobItem
from src.crawl_layer.utils.loader import save_to_temp
from src.storage_layer.MinIO_S3.config.path import DEFAULT_ENTITY_NAME

from .config import BASE_URL
from .http_client import TopcvHttpClient
from .parser import TopcvParser
from .utils import encode_input

logger = logging.getLogger(__name__)

SOURCE_NAME = "topcv"
ENTITY_NAME = DEFAULT_ENTITY_NAME


class TopcvCrawler:
    """Async TopCV crawler.

    Strategy:
        * Fetch search-result pages, collect unique job URLs.
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
    ) -> None:
        self.keyword = keyword
        self.max_pages = max_pages

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
        slug = encode_input(self.keyword)
        url: str | None = f"{BASE_URL}-{slug}"

        async with self.http:
            for page_num in range(1, self.max_pages + 1):
                temp_items: list[TopCVJobItem] = []
                if not url:
                    break

                page_urls, next_url = await self._collect_page_urls(url)
                if not page_urls:
                    url = next_url
                    continue

                page_items = await self._scrape_details_batch(page_urls)
                
                # Append total jobs scrape
                items.extend(page_items)

                # Current page jobs
                temp_items.extend(page_items)
                self._flush_batch(temp_items, page_num)

                url = next_url

        return items

    # -- search pagination (single page) -----------------------------------
    async def _collect_page_urls(self, url: str) -> tuple[list[str], str | None]:
        """Fetch one search page and return its new (deduped) job URLs + next page URL."""
        html = await self.http.fetch(url, referer=None)
        if not html:
            return [], None

        page_urls, next_url = self.parser.parse_search_page(html)
        new_urls: list[str] = []
        for u in page_urls:
            if u and u not in self._seen_urls:
                self._seen_urls.add(u)
                new_urls.append(u)
        return new_urls, next_url

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
