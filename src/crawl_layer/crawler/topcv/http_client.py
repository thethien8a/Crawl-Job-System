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
        max_retries: int = 3,
        timeout: float = 30.0,
    ) -> None:
        self.concurrency = concurrency
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.timeout = timeout

        self._semaphore = asyncio.Semaphore(concurrency)
        self._session: AsyncSession | None = None
        
        # Cơ chế Global Pause (Circuit Breaker)
        self._is_clear = asyncio.Event()
        self._is_clear.set()  # Mặc định là thông đường
        self._prober_lock = asyncio.Lock()

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

            is_prober = False
            try:
                for attempt in range(1, self.max_retries + 1):
                    # Nếu đang có chặn (pause), tất cả các request ngoại trừ thằng "thám báo" (prober) phải đứng chờ ở đây
                    if not is_prober:
                        await self._is_clear.wait()

                    try:
                        headers = {}
                        if referer:
                            headers["Referer"] = referer

                        resp = await self.session.get(url, headers=headers)
                        status = resp.status_code

                        if status in BLOCK_STATUS:
                            # Tranh nhau làm prober
                            async with self._prober_lock:
                                if self._is_clear.is_set():
                                    # Tôi là người đầu tiên phát hiện lỗi, tôi sẽ làm prober và giăng rào chặn các request khác lại
                                    self._is_clear.clear()
                                    is_prober = True
                            
                            if is_prober:
                                logger.warning(
                                    "HTTP %d (blocked) on %s. Bật GLOBAL PAUSE (chờ 62s) (attempt %d/%d)",
                                    status, url, attempt, self.max_retries,
                                )
                                # Ngủ 62 giây
                                await asyncio.sleep(62.0)
                            else:
                                logger.warning(
                                    "HTTP %d (blocked) on %s. Đang chờ GLOBAL PAUSE kết thúc...",
                                    status, url
                                )
                            continue  # Quay lại đầu vòng lặp

                        elif status in RETRY_STATUS:
                            logger.warning(
                                "HTTP %d on %s (attempt %d/%d)",
                                status, url, attempt, self.max_retries,
                            )
                            sleep_for = min(120.0, 2.0 * (2 ** (attempt - 1)))
                            await asyncio.sleep(sleep_for + random.uniform(0, 1))
                            continue

                        elif 200 <= status < 300:
                            if is_prober:
                                # Prober đã chạy thành công, mở rào cho các anh em khác xông lên!
                                logger.info("Prober chạy thành công %s! Tắt GLOBAL PAUSE.", url)
                                self._is_clear.set()
                                is_prober = False
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

            finally:
                # Nếu thằng prober bị dẹo (hết 5 lần retry hoặc lỗi crash), nó phải tự mở rào trước khi chết để anh em khác không bị kẹt vĩnh viễn
                if is_prober:
                    logger.warning("Prober thất bại toàn tập. Mở lại GLOBAL PAUSE để các request khác tiếp tục hoặc tự hủy.")
                    self._is_clear.set()
