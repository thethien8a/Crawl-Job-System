"""VietnamWorks crawler — static configuration.

Centralised so the parser, browser, and orchestrator never disagree on
URLs, selectors, or timing.
"""

from __future__ import annotations

SOURCE_NAME = "vietnamworks.com"

BASE_URL = "https://www.vietnamworks.com/viec-lam"
SEARCH_URL_TEMPLATE = "https://www.vietnamworks.com/viec-lam?q={slug}"

# -- Search page selectors ----------------------------------------------------
JOB_URL_SELECTOR = "a[href*='-jv?']"
NEXT_PAGE_XPATH = "//ul[contains(@class, 'pagination')]//li[contains(@class, 'btn-default')]//button[text()='>']"

# -- Detail page selectors ----------------------------------------------------
TITLE_SELECTOR = "h1[name='title']"
COMPANY_SELECTOR = "a[href*='nha-tuyen-dung']"
SALARY_SELECTOR = "span[class*='cVbwLK']"
LOCATION_SELECTOR = "div[class*='gVpPKv'] span"
DEADLINE_XPATH = "//span[contains(text(), 'Hết hạn trong')]//text()"

# "Xem thêm" button
EXPAND_CONTENT_SELECTOR = "button[aria-label='Xem thêm']"

# -- Timing -------------------------------------------------------------------
DEFAULT_KEYWORD = "data analyst"
PAGE_LOAD_TIMEOUT = 15.0
ELEMENT_TIMEOUT = 10.0
PAGINATION_DELAY_RANGE = (2.0, 4.0)
SCROLL_INCREMENT = 1000

BROWSER_ARGS: tuple[str, ...] = (
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-blink-features=AutomationControlled",
    "--window-size=1920,1080",
    "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)
