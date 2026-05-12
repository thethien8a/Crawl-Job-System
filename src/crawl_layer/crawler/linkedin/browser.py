"""Browser layer for the LinkedIn crawler.

LinkedIn requires:
  * A logged-in session — public job pages redirect to authwall after a few
    detail clicks.
  * Slow, jittered interactions — typing cadence and per-card delays are
    part of LinkedIn's anti-automation heuristics.
  * JavaScript-rendered job results that load detail into a side panel
    on click rather than via separate URLs (same shape as ITviec).
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import AsyncIterator

import nodriver as uc
from dotenv import load_dotenv

load_dotenv()

from .config import (
    BROWSER_ARGS,
    CARD_CLICK_DELAY,
    CLICK_DELAY_RANGE,
    DEFAULT_ACCEPT_LANGUAGE,
    DEFAULT_USER_AGENTS,
    DEFAULT_VIEWPORTS,
    DETAIL_PANEL_SELECTOR,
    JOB_CARD_SELECTOR,
    JOB_CONTAINER_SELECTOR,
    JOB_LINK_SELECTOR,
    LOGIN_AFTER_SUBMIT_DELAY_RANGE,
    LOGIN_CHALLENGE_MARKERS,
    LOGIN_SUCCESS_MARKERS,
    LOGIN_TIMEOUT,
    LOGIN_URL,
    NEXT_BUTTON_TIMEOUT,
    NEXT_PAGE_SELECTOR,
    PAGE_LOAD_TIMEOUT,
    PAGINATION_DELAY_RANGE,
    PANEL_LOAD_TIMEOUT,
    PASSWORD_ENV,
    PASSWORD_INPUT_SELECTOR,
    SEARCH_URL_TEMPLATE,
    SUBMIT_BUTTON_XPATH,
    TYPING_DELAY_RANGE,
    USER_DATA_DIR,
    USERNAME_ENV,
    USERNAME_INPUT_SELECTOR,
    )
    
from .utils import human_like_typing, press_tab, type_into_focused

logger = logging.getLogger(__name__)


class LinkedinLoginError(RuntimeError):
    """Raised when login cannot be completed (bad creds, blocked, layout drift)."""


class LinkedinBrowser:
    """Owns the nodriver browser for the LinkedIn session.

    Use as an async context manager so the browser is properly stopped
    even on exceptions.
    """

    def __init__(self, headless: bool = True) -> None:
        self.headless = headless
        self._browser: uc.Browser | None = None
        self._tab: uc.Tab | None = None

    # -- lifecycle ----------------------------------------------------------
    async def __aenter__(self) -> "LinkedinBrowser":
        width, height = random.choice(DEFAULT_VIEWPORTS)
        user_agent = random.choice(DEFAULT_USER_AGENTS)
        args = list(BROWSER_ARGS) + [
            f"--window-size={width},{height}",
            f"--user-agent={user_agent}",
        ]
        self._browser = await uc.start(
            headless=self.headless,
            browser_args=args,
            lang=DEFAULT_ACCEPT_LANGUAGE.split(",")[0],
            user_data_dir=USER_DATA_DIR,
        )
        # nodriver starts with one tab open already; reuse it.
        self._tab = self._browser.main_tab
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._browser is not None:
            try:
                self._browser.stop()
                # Give Windows asyncio a beat to close subprocess pipes.
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning("Browser stop raised: %s", e)
            self._browser = None
            self._tab = None

    @property
    def tab(self) -> uc.Tab:
        assert self._tab is not None, "LinkedinBrowser must be used as `async with`"
        return self._tab

    # -- login --------------------------------------------------------------
    async def login(self) -> None:
        """Log in using credentials from the environment.

        Raises LinkedinLoginError if creds are missing, the login form
        layout drifted, or LinkedIn served a security challenge.
        """
        username = os.getenv(USERNAME_ENV)
        password = os.getenv(PASSWORD_ENV)
        if not username or not password:
            raise LinkedinLoginError(
                f"Missing credentials: set {USERNAME_ENV} and {PASSWORD_ENV}"
            )

        tab = self.tab
        await tab.get(LOGIN_URL)
        logger.info("Navigated to login page")
        
        # Chờ 3s xem có được tự động chuyển hướng không (nếu đã lưu phiên trước đó)
        await asyncio.sleep(3)
        if LOGIN_SUCCESS_MARKERS in tab.target.url or ("login" not in tab.target.url and "checkpoint" not in tab.target.url):
            logger.info("Session restored from previous login. Skipping credential entry.")
            return

        try:
            user_input = await tab.wait_for(
                selector=USERNAME_INPUT_SELECTOR, timeout=LOGIN_TIMEOUT
            )
            # human_like_typing thực hiện CDP-level mouse click + CDP key events.
            # React không bắt được focus thực từ chuột nên username sẽ vào đúng ô.
            await human_like_typing(user_input, username, TYPING_DELAY_RANGE)

            await asyncio.sleep(random.uniform(0.3, 0.7))

            # Đảm bảo ô password đã render xong trước khi gõ Tab.
            await tab.wait_for(
                selector=PASSWORD_INPUT_SELECTOR, timeout=LOGIN_TIMEOUT
            )
            
            await press_tab(tab)
            await asyncio.sleep(random.uniform(0.3, 0.5))

            # Gõ thẳng vào element đang focus (password field sau khi Tab).
            await type_into_focused(tab, password, TYPING_DELAY_RANGE)

            await asyncio.sleep(random.uniform(0.3, 0.7))

            submit_buttons = await tab.xpath(SUBMIT_BUTTON_XPATH)
            if not submit_buttons:
                raise LinkedinLoginError("Sign-in submit button not found")
            # mouse_click thay vì click() để cùng lý do: React intercept
            # JS-level click nhưng không cản được CDP mouse event.
            await submit_buttons[0].mouse_click()
            logger.info("Submitted login form")
        except LinkedinLoginError:
            raise
        except Exception as e:
            await self._save_screenshot("linkedin_login_error.png")
            raise LinkedinLoginError(f"Unexpected error during login: {e}") from e

        await asyncio.sleep(random.uniform(*LOGIN_AFTER_SUBMIT_DELAY_RANGE))
        await self._verify_login()

    async def _verify_login(self) -> None:
        """Inspect the post-submit URL and reject security challenges."""
        for _ in range(int(LOGIN_TIMEOUT)):
            url = self.tab.target.url
            if LOGIN_SUCCESS_MARKERS in url:
                logger.info("Login verified, landed on: %s", url)
                return
            if any(m in url for m in LOGIN_CHALLENGE_MARKERS):
                await self._save_screenshot("linkedin_challenge.png")
                raise LinkedinLoginError(f"Hit security challenge at URL: {url}")
            await asyncio.sleep(1)
        
        url = self.tab.target.url
        await self._save_screenshot("linkedin_login_timeout.png")
        raise LinkedinLoginError(f"Login timed out or stuck at: {url}")

    # -- search navigation --------------------------------------------------
    async def open_search(self, keyword: str, location: str) -> None:
        """Navigate to the search results page for `keyword` + `location`."""
        url = SEARCH_URL_TEMPLATE.format(keyword=keyword, location=location)
        await self.tab.get(url)
        logger.info(
            "Search page loaded for keyword: %s (location: %s)", keyword, location
        )

    async def iter_job_panels(self) -> AsyncIterator[tuple[str, str]]:
        """Yield (job_url, detail_panel_html) for every card on the page.

        Each iteration scrolls a card into view, clicks it, waits for the
        side panel to refresh, then snapshots its HTML for parsing.
        """
        try:
            # Dùng wait_for thay vì query_selector để đợi lazy-load
            container_matches = await self.tab.wait_for(
                selector=JOB_CONTAINER_SELECTOR, 
                timeout=PAGE_LOAD_TIMEOUT
            )
        except Exception:
            logger.error("Job container not found on current page (Timeout)")
            return

        # Tìm các job card CHỈ nằm bên trong container_matches
        cards = await container_matches.query_selector_all(JOB_CARD_SELECTOR)
        logger.info("Found %d job cards on current page", len(cards))

        for card in cards:
            try:
                await card.scroll_into_view()
                await asyncio.sleep(CARD_CLICK_DELAY)
                
                # LinkedIn thường bắt sự kiện click ở thẻ div con bên trong thay vì thẻ li ngoài cùng
                clickable = await card.query_selector(".job-card-container--clickable")
                if clickable:
                    await clickable.click()
                else:
                    await card.click()

                await self.tab.wait_for(
                    selector=DETAIL_PANEL_SELECTOR, timeout=PANEL_LOAD_TIMEOUT
                )
 
                panel = await self.tab.query_selector(DETAIL_PANEL_SELECTOR)
                if panel is None:
                    logger.warning("Detail panel missing")
                    continue
                panel_html = await panel.get_html()
                yield panel_html

                await asyncio.sleep(random.uniform(*CLICK_DELAY_RANGE))
            except Exception as e:
                logger.warning("Skip job card due to error: %s", e)
                continue

    async def go_to_next_page(self) -> bool:
        """Click the "next page" button if present.

        Returns True if pagination advanced, False if we have hit the last
        page (no button found within NEXT_BUTTON_TIMEOUT).
        """
        try:
            next_btn = await self.tab.wait_for(
                selector=NEXT_PAGE_SELECTOR, timeout=NEXT_BUTTON_TIMEOUT
            )
        except Exception:
            logger.info("No next page button — reached end of results")
            return False

        try:
            await next_btn.scroll_into_view()
            await next_btn.click()
            await asyncio.sleep(random.uniform(*PAGINATION_DELAY_RANGE))
            return True
        except Exception as e:
            logger.warning("Failed to navigate to next page: %s", e)
            return False

    # -- internals ----------------------------------------------------------
    @staticmethod
    async def _extract_job_url(card) -> str | None:
        """Find the canonical job URL from a card's nested anchor."""
        try:
            link = await card.query_selector(JOB_LINK_SELECTOR)
            if link is None:
                return None
            return link.attrs.get("href")
        except Exception:
            return None

    async def _save_screenshot(self, path: str) -> None:
        """Best-effort screenshot for debugging — never fatal."""
        if self._tab is None:
            return
        try:
            await self._tab.save_screenshot(path)
        except Exception as e:
            logger.debug("Screenshot save failed (%s): %s", path, e)