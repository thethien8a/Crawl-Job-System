"""HTTP layer for the TopCV crawler.

Uses curl_cffi for async GET requests with impersonation, semaphore throttling,
and exponential backoff to handle rate limits and basic anti-bot measures.
"""

from __future__ import annotations

import asyncio
import logging
import random

from curl_cffi.requests import AsyncSession, RequestsError

from .config import (
    BLOCK_STATUS,
    DEFAULT_ACCEPT_LANGUAGE,
    IMPERSONATE_PROFILE,
    RETRY_STATUS,
)

logger = logging.getLogger(__name__)


class TopcvHttpClient:
    """Owns the curl_cffi session for fetching pages.

    Use as an async context manager so the session is properly closed.
    """

    def __init__(
        self,
        concurrency: int = 4,
        request_delay: tuple[float, float] = (4.0, 6.0),
        max_retries: int = 5,
        timeout: float = 30.0,
    ) -> None:
        self.concurrency = concurrency
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.timeout = timeout

        self._semaphore = asyncio.Semaphore(concurrency)
        self._session: AsyncSession | None = None

    # -- lifecycle ----------------------------------------------------------
    async def __aenter__(self) -> "TopcvHttpClient":
        self._session = AsyncSession(
            impersonate=IMPERSONATE_PROFILE,
            timeout=self.timeout,
            headers={"Accept-Language": DEFAULT_ACCEPT_LANGUAGE},
        )
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.__aexit__(exc_type, exc, tb)
            self._session = None

    @property
    def session(self) -> AsyncSession:
        assert self._session is not None, "TopcvHttpClient must be used as `async with`"
        return self._session

    # -- fetch --------------------------------------------------------------
    async def fetch(self, url: str, referer: str | None = None) -> str | None:
        async with self._semaphore:
            await asyncio.sleep(random.uniform(*self.request_delay))

            for attempt in range(1, self.max_retries + 1):
                try:
                    headers = {}
                    if referer:
                        headers["Referer"] = referer

                    resp = await self.session.get(url, headers=headers)
                    status = resp.status_code

                    if status in BLOCK_STATUS:
                        logger.warning(
                            "HTTP %d (blocked) on %s (attempt %d/%d)",
                            status, url, attempt, self.max_retries,
                        )
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

                sleep_for = min(120.0, 2.0 * (2 ** (attempt - 1)))
                await asyncio.sleep(sleep_for + random.uniform(0, 1))

            logger.error("Giving up on %s after %d retries", url, self.max_retries)
            return None
