"""HTTP layer for the TopCV crawler.

Two responsibilities, kept together because they share the curl_cffi session
and the captured browser identity:
  1. `warm_up()` — drive a real Chromium via nodriver so Cloudflare's JS
     challenge auto-solves, then harvest cookies + UA into the curl_cffi
     session.
  2. `fetch()` — async GET with semaphore throttling, exponential backoff,
     and on-403 re-warm.

Why split out from the crawler: parsing/orchestration changes much more
often than the HTTP/anti-bot stack. Isolating the network surface means a
future swap (nodriver → camoufox, curl_cffi → tls_client, etc.) only
touches this file.
"""

from __future__ import annotations

import asyncio
import logging
import random

import nodriver as uc
from curl_cffi.requests import AsyncSession, RequestsError

from .config import (
    BLOCK_STATUS,
    CF_COOKIE_NAMES,
    DEFAULT_ACCEPT_LANGUAGE,
    HOME_URL,
    IMPERSONATE_PROFILE,
    RETRY_STATUS,
)

logger = logging.getLogger(__name__)


class TopcvHttpClient:
    """Owns the curl_cffi session + browser-warmup state.

    Use as an async context manager so the session is created/destroyed
    deterministically and the warm-up runs exactly once on entry.
    """

    def __init__(
        self,
        concurrency: int = 4,
        request_delay: tuple[float, float] = (4.0, 8.0),
        max_retries: int = 6,
        timeout: float = 30.0,
        headless: bool = False,
        warmup_wait: float = 8.0,
        warmup_cooldown: float = 30.0,
    ) -> None:
        self.concurrency = concurrency
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.timeout = timeout
        # nodriver: headless=False is far less detectable; only flip to True
        # on a server with Xvfb or after verifying the target accepts it.
        self.headless = headless
        self.warmup_wait = warmup_wait
        # Skip a re-warm if a peer just refreshed cookies within this window.
        self.warmup_cooldown = warmup_cooldown

        self._semaphore = asyncio.Semaphore(concurrency)
        # Captured from nodriver; CF binds cf_clearance to UA, so curl_cffi
        # MUST send the same one Chromium presented during warm-up.
        self._user_agent: str | None = None
        # Serialise warm-up so concurrent 403s don't open multiple browsers.
        self._warmup_lock = asyncio.Lock()
        self._last_warmup_at: float = 0.0

        self._session: AsyncSession | None = None

    # -- lifecycle ----------------------------------------------------------
    async def __aenter__(self) -> "TopcvHttpClient":
        self._session = AsyncSession(
            impersonate=IMPERSONATE_PROFILE,
            timeout=self.timeout,
        )
        await self._session.__aenter__()
        await self.warm_up()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.__aexit__(exc_type, exc, tb)
            self._session = None

    @property
    def session(self) -> AsyncSession:
        # Internal invariant: only ever called between __aenter__ / __aexit__.
        assert self._session is not None, "TopcvHttpClient must be used as `async with`"
        return self._session

    # -- warm-up ------------------------------------------------------------
    async def warm_up(self) -> None:
        # Lock prevents concurrent 403s from spawning N browsers in parallel;
        # whoever gets the lock refreshes, the rest reuse the fresh cookies.
        async with self._warmup_lock:
            now = asyncio.get_event_loop().time()
            if self._user_agent and (now - self._last_warmup_at) < self.warmup_cooldown:
                return

            logger.info("Warming up via nodriver (headless=%s)", self.headless)
            browser = None
            try:
                browser = await uc.start(
                    headless=self.headless,
                    # --no-sandbox makes nodriver work in containers / WSL;
                    # the AutomationControlled flag suppresses one of the
                    # easy CDP fingerprints CF and others look for.
                    browser_args=[
                        "--no-sandbox",
                        "--disable-blink-features=AutomationControlled",
                    ],
                )
                page = await browser.get(HOME_URL)
                # Let CF's JS challenge auto-solve. nodriver doesn't trip
                # the CDP detection that vanilla Playwright/Selenium does,
                # so the challenge passes silently within a few seconds.
                await asyncio.sleep(self.warmup_wait)

                ua = await page.evaluate("navigator.userAgent")
                if isinstance(ua, str) and ua:
                    self._user_agent = ua

                cookies = await browser.cookies.get_all()
                injected, cf_present = self._inject_cookies(cookies)
                logger.info(
                    "Warm-up done: injected %d cookies (cf_clearance present=%s) UA=%s",
                    injected, cf_present, (self._user_agent or "")[:60],
                )
                self._last_warmup_at = asyncio.get_event_loop().time()
            except Exception as exc:
                logger.warning("nodriver warm-up failed: %s", exc)
            finally:
                if browser is not None:
                    try:
                        browser.stop()
                    except Exception:
                        pass

    def _inject_cookies(self, cookies) -> tuple[int, bool]:
        injected = 0
        cf_present = False
        for c in cookies:
            # Only port cookies for topcv.vn (and its CF subdomains), not
            # third-party trackers Chromium accumulated.
            domain = (getattr(c, "domain", "") or "").lstrip(".")
            if "topcv.vn" not in domain:
                continue
            name = getattr(c, "name", None)
            value = getattr(c, "value", None)
            if not name or value is None:
                continue
            if name in CF_COOKIE_NAMES:
                cf_present = True
            self.session.cookies.set(
                name, value,
                domain=getattr(c, "domain", None) or "topcv.vn",
                path=getattr(c, "path", "/") or "/",
            )
            injected += 1
        return injected, cf_present

    # -- fetch --------------------------------------------------------------
    async def fetch(self, url: str, referer: str | None = None) -> str | None:
        # Semaphore + jittered delay replicate Scrapy's per-domain throttling.
        async with self._semaphore:
            await asyncio.sleep(random.uniform(*self.request_delay))

            for attempt in range(1, self.max_retries + 1):
                try:
                    resp = await self.session.get(
                        url,
                        headers=self._extra_headers(referer),
                        impersonate=IMPERSONATE_PROFILE,
                    )
                    status = resp.status_code

                    if status in BLOCK_STATUS:
                        # CF revoked our clearance. Re-run nodriver warm-up
                        # to get a fresh cf_clearance cookie.
                        logger.warning(
                            "HTTP %d (blocked) on %s (attempt %d/%d) - "
                            "re-warming via nodriver",
                            status, url, attempt, self.max_retries,
                        )
                        await asyncio.sleep(random.uniform(5.0, 12.0))
                        await self.warm_up()
                    elif status in RETRY_STATUS:
                        logger.warning(
                            "HTTP %d on %s (attempt %d/%d)",
                            status, url, attempt, self.max_retries,
                        )
                    elif 200 <= status < 300:
                        return resp.text
                    else:
                        logger.warning(
                            "HTTP %d on %s (non-retryable, giving up)",
                            status, url,
                        )
                        return None
                except RequestsError as exc:
                    logger.warning(
                        "Network error on %s (attempt %d/%d): %s",
                        url, attempt, self.max_retries, exc,
                    )

                # Exponential backoff capped at 120s, matching Scrapy defaults.
                sleep_for = min(120.0, 2.0 * (2 ** (attempt - 1)))
                await asyncio.sleep(sleep_for + random.uniform(0, 1))

            logger.error("Giving up on %s after %d retries", url, self.max_retries)
            return None

    def _extra_headers(self, referer: str | None) -> dict[str, str]:
        # curl_cffi auto-injects sec-ch-ua, sec-fetch-*, Accept, etc. for the
        # impersonation profile. We force-override the User-Agent to match
        # exactly what nodriver presented during warm-up — CF's cf_clearance
        # is bound to (UA + JA3 + IP), and a UA mismatch drops us back to 403.
        headers = {"Accept-Language": DEFAULT_ACCEPT_LANGUAGE}
        if self._user_agent:
            headers["User-Agent"] = self._user_agent
        if referer:
            headers["Referer"] = referer
        return headers
