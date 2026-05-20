"""Browser layer for the ITviec crawler.

ITviec requires:
  * Cloudflare bypass — handled by `nodriver` automatically.
  * Account login — selectors and flow ported from the original spider.
  * JavaScript-rendered search results that load detail into a side panel
    on click rather than via separate URLs.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import AsyncIterator
from dotenv import load_dotenv

load_dotenv()

import nodriver as uc

from .config import (
    BROWSER_ARGS,
    CARD_CLICK_DELAY,
    EMAIL_INPUT_SELECTOR,
    JOB_CARD_SELECTOR,
    LOGGED_IN_MARKER_SELECTOR,
    LOGIN_MARKER_RETRY_INTERVAL,
    LOGIN_MARKER_RETRY_TIMEOUT,
    LOGIN_MARKER_TOTAL_TIMEOUT,
    LOGIN_TIMEOUT,
    LOGIN_URL,
    NEXT_BUTTON_TIMEOUT,
    NEXT_PAGE_SELECTOR,
    PAGE_LOAD_TIMEOUT,
    PAGINATION_DELAY_RANGE,
    PANEL_LOAD_TIMEOUT,
    PASSWORD_ENV,
    PASSWORD_INPUT_SELECTOR,
    POST_LOGIN_MODAL_SELECTOR,
    POST_SUBMIT_SETTLE,
    PREVIEW_PANEL_SELECTOR,
    REMIND_LATER_BUTTON_XPATH,
    SEARCH_LOADED_SELECTOR,
    SEARCH_URL_TEMPLATE,
    SUBMIT_BUTTON_XPATH,
    USERNAME_ENV,
)
from .utils import absolute_url, encode_keyword

logger = logging.getLogger(__name__)


class ItviecLoginError(RuntimeError):
    """Raised when login cannot be completed (bad creds, blocked, layout drift)."""


class ItviecBrowser:
    """Owns the nodriver browser for the ITviec session.

    Use as an async context manager so the browser is properly stopped
    even on exceptions.
    """

    def __init__(self, headless: bool = True) -> None:
        self.headless = headless
        self._browser: uc.Browser | None = None
        self._tab: uc.Tab | None = None

    # -- lifecycle ----------------------------------------------------------
    async def __aenter__(self) -> "ItviecBrowser":
        self._browser = await uc.start(
            headless=self.headless,
            browser_args=list(BROWSER_ARGS),
        )
        # nodriver starts with one tab open already; reuse it.
        self._tab = self._browser.main_tab
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._browser is not None:
            try:
                self._browser.stop()
            except Exception as e:
                logger.warning("Browser stop raised: %s", e)
            self._browser = None
            self._tab = None

    @property
    def tab(self) -> uc.Tab:
        assert self._tab is not None, "ItviecBrowser must be used as `async with`"
        return self._tab

    # -- login --------------------------------------------------------------
    async def login(self) -> None:
        """Log in using credentials from the environment.

        Raises ItviecLoginError if creds are missing or the post-login
        marker never appears.
        """
        username = os.getenv(USERNAME_ENV)
        password = os.getenv(PASSWORD_ENV)
        if not username or not password:
            raise ItviecLoginError(
                f"Missing credentials: set {USERNAME_ENV} and {PASSWORD_ENV}"
            )

        tab = self.tab
        await tab.get(LOGIN_URL)

        logger.info("Navigated to login page")

        email_input = await tab.wait_for(
            selector=EMAIL_INPUT_SELECTOR, timeout=LOGIN_TIMEOUT
        )
        await email_input.click()
        await email_input.clear_input()
        await email_input.send_keys(username)

        password_input = await tab.select(PASSWORD_INPUT_SELECTOR)
        await password_input.click()

        await password_input.clear_input()
        await password_input.send_keys(password)

        # nodriver's xpath() returns a list of matching elements.
        submit_buttons = await tab.xpath(SUBMIT_BUTTON_XPATH)
        if not submit_buttons:
            raise ItviecLoginError("Sign-in submit button not found")
        await submit_buttons[0].click()
        logger.info("Clicked submit button")

        # Give the post-submit redirect time to start before we probe the DOM;
        # otherwise nodriver may grab a node id that gets invalidated mid-nav.
        await asyncio.sleep(POST_SUBMIT_SETTLE)

        if not await self._wait_for_logged_in_marker():
            raise ItviecLoginError(
                "Post-login marker never appeared after "
                f"{LOGIN_MARKER_TOTAL_TIMEOUT:.0f}s of retries"
            )

        logger.info("Logged in to ITviec")
        await self._dismiss_post_login_modal()

    async def _wait_for_logged_in_marker(self) -> bool:
        """Poll for the logged-in marker, surviving CDP node-id churn.

        nodriver's `wait_for` occasionally fails with
        "Could not find node with given id [code: -32000]" because the
        page redirects after sign-in and any element handle it acquired
        becomes stale. Retrying with fresh selector queries works around
        that without giving up on the whole login.
        """
        tab = self.tab
        loop = asyncio.get_event_loop()
        deadline = loop.time() + LOGIN_MARKER_TOTAL_TIMEOUT
        attempt = 0
        last_error: Exception | None = None

        while loop.time() < deadline:
            attempt += 1
            try:
                await tab.wait_for(
                    selector=LOGGED_IN_MARKER_SELECTOR,
                    timeout=LOGIN_MARKER_RETRY_TIMEOUT,
                )
                logger.info(
                    "Logged-in marker found on attempt %d", attempt
                )
                return True
            except Exception as e:
                last_error = e
                logger.debug(
                    "Login marker not visible yet (attempt %d): %s",
                    attempt,
                    e,
                )
                await asyncio.sleep(LOGIN_MARKER_RETRY_INTERVAL)

        if last_error is not None:
            logger.warning(
                "Gave up waiting for logged-in marker after %d attempts: %s",
                attempt,
                last_error,
            )
        return False

    async def _dismiss_post_login_modal(self) -> None:
        """Close the "Remind me later" upsell modal that blurs the page."""
        try:
            modal = await self.tab.query_selector(POST_LOGIN_MODAL_SELECTOR)
            if not modal:
                return
            buttons = await self.tab.xpath(REMIND_LATER_BUTTON_XPATH)
            if buttons:
                await buttons[0].click()
                logger.info("Dismissed post-login modal")
        except Exception as e:
            # Modal is best-effort — don't abort the crawl if it isn't there.
            logger.debug("No post-login modal to dismiss: %s", e)

    # -- search navigation --------------------------------------------------
    async def open_search(self, keyword: str) -> None:
        """Navigate to the search results page for `keyword`."""
        slug = encode_keyword(keyword)
        url = SEARCH_URL_TEMPLATE.format(slug=slug)
        await self.tab.get(url)
        await self.tab.wait_for(
            selector=SEARCH_LOADED_SELECTOR, timeout=PAGE_LOAD_TIMEOUT
        )
        logger.info("Search page loaded for keyword: %s", keyword)

    async def iter_job_panels(self) -> AsyncIterator[tuple[str, str]]:
        """Yield (job_url, preview_panel_html) for every card on the page.

        Each iteration scrolls a card into view, clicks it, waits for the
        side panel to refresh, then snapshots its HTML for parsing.
        """
        cards = await self.tab.select_all(JOB_CARD_SELECTOR)
        logger.info("Found %d job cards on current page", len(cards))

        for card in cards:
            job_url = absolute_url(card.attrs.get("data-url"))
            if not job_url:
                continue

            try:
                await card.scroll_into_view()
                await asyncio.sleep(CARD_CLICK_DELAY)
                await card.click()
                
                # Đợi một chút để JS fetch và render nội dung mới vào panel
                await asyncio.sleep(1.5)

                await self.tab.wait_for(
                    selector=PREVIEW_PANEL_SELECTOR, timeout=PANEL_LOAD_TIMEOUT
                )
                panel = await self.tab.query_selector(PREVIEW_PANEL_SELECTOR)
                if panel is None:
                    logger.warning("Preview panel missing for %s", job_url)
                    continue
                panel_html = await panel.get_html()
                yield job_url, panel_html
            except Exception as e:
                logger.warning("Failed to read panel for %s: %s", job_url, e)
                continue

    async def go_to_next_page(self) -> bool:
        """Click the "next page" link if present.

        Returns True if pagination advanced, False if we have hit the last
        page (no link found within NEXT_BUTTON_TIMEOUT).
        """
        try:
            next_link = await self.tab.wait_for(
                selector=NEXT_PAGE_SELECTOR, timeout=NEXT_BUTTON_TIMEOUT
            )
        except Exception:
            logger.info("No next page link — reached end of results")
            return False

        try:
            await next_link.scroll_into_view()
            await next_link.click()
            await self._sleep_pagination()
            await self.tab.wait_for(
                selector=SEARCH_LOADED_SELECTOR, timeout=PAGE_LOAD_TIMEOUT
            )
            return True
        except Exception as e:
            logger.warning("Failed to navigate to next page: %s", e)
            return False

    @staticmethod
    async def _sleep_pagination() -> None:
        await asyncio.sleep(random.uniform(*PAGINATION_DELAY_RANGE))
