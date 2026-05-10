"""VietnamWorks crawler — top-level orchestration."""

from __future__ import annotations

import logging

from src.crawl_layer.data_model.data_class import VietnamWorksJobItem as JobItem

from .browser import VietnamWorksBrowser
from .parser import VietnamWorksParser
from .config import DEFAULT_KEYWORD

logger = logging.getLogger(__name__)


class VietnamWorksCrawler:
    """Async VietnamWorks crawler using nodriver."""

    def __init__(
        self,
        keyword: str = DEFAULT_KEYWORD,
        max_pages: int = 3,
        headless: bool = True,
    ) -> None:
        self.keyword = keyword
        self.max_pages = max_pages
        self.headless = headless

        self.browser = VietnamWorksBrowser(headless=headless)
        self.parser = VietnamWorksParser()

        self._seen_urls: set[str] = set()

    async def crawl(self) -> list[JobItem]:
        items: list[JobItem] = []
        job_urls: list[str] = []

        # Phase 1: Collect URLs
        async with self.browser:
            await self.browser.open_search(self.keyword)

            pages_done = 0
            while pages_done < self.max_pages:
                pages_done += 1
                logger.info("Scanning URLs on page %d/%d", pages_done, self.max_pages)
                
                urls = await self.browser.get_job_urls_on_page()
                for url in urls:
                    if not url.startswith("http"):
                        url = "https://www.vietnamworks.com" + url
                    if url not in self._seen_urls:
                        self._seen_urls.add(url)
                        job_urls.append(url)

                if pages_done >= self.max_pages:
                    break
                if not await self.browser.go_to_next_page():
                    break
            
            logger.info("Finished URL collection. Total unique URLs: %d", len(job_urls))

            # Phase 2: Visit each URL and parse
            for i, url in enumerate(job_urls, 1):
                logger.info("Scraping detail %d/%d: %s", i, len(job_urls), url)

                html = await self.browser.get_job_detail_html(url)
                if not html:
                    continue
                
                try:
                    item = self.parser.parse_job_detail(html, url, self.keyword)
                    items.append(item)
                except Exception as e:
                    logger.error("Failed to parse %s: %s", url, e)

        logger.info("Crawl finished — collected %d items", len(items))
        return items
