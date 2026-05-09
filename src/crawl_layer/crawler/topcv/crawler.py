"""TopCV crawler — top-level orchestration.

Wires together:
  * `TopcvHttpClient` — owns the curl_cffi session and nodriver warm-up.
  * `TopcvParser`     — turns HTML into JobItem instances.

Keeps only the high-level flow here: paginate search results, dedupe URLs,
fan out detail-page fetches, gather results.
"""

from __future__ import annotations

import asyncio
import logging

from src.crawl_layer.data_model.data_class import JobItem

from .config import BASE_URL
from .http_client import TopcvHttpClient
from .parser import TopcvParser
from .utils import encode_input

logger = logging.getLogger(__name__)


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
        concurrency: int = 1,
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
    async def crawl(self) -> list[JobItem]:
        async with self.http:
            job_urls = await self._collect_job_urls()
            logger.info("Collected %d unique job URLs", len(job_urls))

            tasks = [self._scrape_detail(url) for url in job_urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        items: list[JobItem] = []
        for url, result in zip(job_urls, results):
            if isinstance(result, Exception):
                logger.warning("Detail crawl failed for %s: %s", url, result)
            elif result is not None:
                items.append(result)
        return items

    # -- search pagination --------------------------------------------------
    async def _collect_job_urls(self) -> list[str]:
        slug = encode_input(self.keyword)
        url: str | None = f"{BASE_URL}-{slug}"
        urls: list[str] = []

        for _ in range(self.max_pages):
            if not url:
                break
            html = await self.http.fetch(url, referer=None)
            if not html:
                break

            page_urls, next_url = self.parser.parse_search_page(html)
            for u in page_urls:
                if u and u not in self._seen_urls:
                    self._seen_urls.add(u)
                    urls.append(u)
            url = next_url

        return urls

    # -- detail page --------------------------------------------------------
    async def _scrape_detail(self, url: str) -> JobItem | None:
        html = await self.http.fetch(url, referer=BASE_URL)
        if not html:
            return None
        return self.parser.parse_job_detail(html, url, self.keyword)
