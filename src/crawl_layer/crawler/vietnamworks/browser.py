"""Browser layer for the VietnamWorks crawler.

Handles interaction with nodriver: navigation, pagination, scrolling,
and collecting job URLs.
"""

from __future__ import annotations

import asyncio
import logging
import random

import nodriver as uc

from .config import (
    BROWSER_ARGS,
    EXPAND_CONTENT_SELECTOR,
    JOB_URL_SELECTOR,
    NEXT_PAGE_XPATH,
    PAGINATION_DELAY_RANGE,
    SCROLL_INCREMENT,
    SEARCH_URL_TEMPLATE,
)
from .utils import encode_keyword

logger = logging.getLogger(__name__)


class VietnamWorksBrowser:
    """Owns the nodriver browser for the VietnamWorks session."""

    def __init__(self, headless: bool = True) -> None:
        self.headless = headless
        self._browser: uc.Browser | None = None
        self._tab: uc.Tab | None = None

    async def __aenter__(self) -> "VietnamWorksBrowser":
        self._browser = await uc.start(
            headless=self.headless,
            browser_args=list(BROWSER_ARGS),
        )
        self._tab = self._browser.main_tab
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._browser is not None:
            try:
                self._browser.stop()
                # Thêm sleep ngắn để asyncio kịp đóng các pipe của subprocess trên Windows
                # Tránh lỗi "ValueError: I/O operation on closed pipe" trong __del__
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning("Browser stop raised: %s", e)
            self._browser = None
            self._tab = None

    @property
    def tab(self) -> uc.Tab:
        assert self._tab is not None, "Browser must be used as `async with`"
        return self._tab

    async def open_search(self, keyword: str) -> None:
        """Navigate to the search results page."""
        slug = encode_keyword(keyword)
        url = SEARCH_URL_TEMPLATE.format(slug=slug)
        await self.tab.get(url)
        # Wait a bit for JS to init
        await asyncio.sleep(2)
        logger.info("Search page loaded for keyword: %s", keyword)

    async def get_job_urls_on_page(self) -> list[str]:
        """Scroll to bottom and collect all job URLs."""
        await self._scroll_to_bottom()
        
        links = await self.tab.select_all(JOB_URL_SELECTOR)
        urls = []
        for link in links:
            href = link.attrs.get("href")
            if href:
                urls.append(href)
                
        logger.info("Found %d job URLs on current page", len(urls))
        return urls

    async def go_to_next_page(self) -> bool:
        """Click the next page button if it exists."""
        try:
            buttons = await self.tab.xpath(NEXT_PAGE_XPATH)
            if not buttons:
                logger.info("No next page button found")
                return False
                
            next_button = buttons[0]
            await next_button.scroll_into_view()
            await asyncio.sleep(1) # wait after scroll
            
            await next_button.click()
            await asyncio.sleep(random.uniform(*PAGINATION_DELAY_RANGE))
            return True
        except Exception as e:
            logger.warning("Failed to navigate to next page: %s", e)
            return False

    async def get_job_detail_html(self, url: str) -> str | None:
        """Visit a job detail page, expand description, and return HTML."""
        try:
            await self.tab.get(url)
            
            # Đợi một chút để JS render và bypass bot check nếu có
            await asyncio.sleep(3)
            
            # Cuộn xuống một chút để kích hoạt lazy load của React/Next.js
            await self.tab.evaluate("window.scrollBy(0, 500);")
            await asyncio.sleep(1)
            await self.tab.evaluate("window.scrollBy(0, 1000);")
            await asyncio.sleep(1)
            await self.tab.evaluate("window.scrollTo(0, 0);")
            await asyncio.sleep(1)

            # Try to click the "Xem thêm" button to expand content
            try:
                expand_btn = await self.tab.query_selector(EXPAND_CONTENT_SELECTOR)
                if expand_btn:
                    await expand_btn.scroll_into_view()
                    await asyncio.sleep(0.5)
                    await expand_btn.click()
                    await asyncio.sleep(0.7) # Wait for expansion animation
            except Exception as e:
                logger.debug("Could not click expand button: %s", e)
                
            html = await self.tab.evaluate("document.documentElement.outerHTML")
            return html
        except Exception as e:
            logger.error("Error loading job detail %s: %s", url, e)
            return None

    async def _scroll_to_bottom(self) -> None:
        try:
            # Wait until body has real content
            for _ in range(20):
                h = await self.tab.evaluate("document.body.scrollHeight", return_by_value=True)
                if isinstance(h, int) and h > 200:
                    break
                await asyncio.sleep(0.5)
            else:
                logger.warning("Body never grew; aborting scroll")
                return

            last_height = h
            current_pos = 0
            stable_rounds = 0
            while stable_rounds < 2:                         # need 2 stable checks, not 1
                current_pos = min(current_pos + SCROLL_INCREMENT, last_height + SCROLL_INCREMENT)
                await self.tab.evaluate(f"window.scrollTo(0, {current_pos});")
                await asyncio.sleep(random.uniform(0.8, 1.5))
                new_height = await self.tab.evaluate("document.body.scrollHeight", return_by_value=True)
                if isinstance(new_height, int) and new_height > last_height:
                    last_height = new_height
                    stable_rounds = 0
                elif current_pos >= last_height:
                    stable_rounds += 1
                    await asyncio.sleep(1.5)
        except Exception as e:
            logger.exception("Error during scrolling: %s", e)   # use exception, not warning
