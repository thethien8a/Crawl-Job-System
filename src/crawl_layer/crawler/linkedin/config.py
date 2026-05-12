"""LinkedIn crawler — static configuration.

Centralised so the parser, browser, and orchestrator never disagree on
URLs, selectors, or timing. Selectors are ported verbatim from the
original spider — LinkedIn ships hashed/utility classes that look noisy
on purpose; do not "tidy" them without checking the live page.
"""

from __future__ import annotations
import os

SOURCE_NAME = "linkedin.com"

BASE_URL = "https://www.linkedin.com"
LOGIN_URL = "https://www.linkedin.com/login"
SEARCH_URL_TEMPLATE = (
    "https://www.linkedin.com/jobs/search?keywords={keyword}&location={location}&f_TPR=r604800"
)

# Env vars that hold credentials (matches the legacy spider).
USERNAME_ENV = "LINKEDIN_EMAIL"
PASSWORD_ENV = "LINKEDIN_PASS"

# Vietnamese-first to look like a local browser.
DEFAULT_ACCEPT_LANGUAGE = "vi,en;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6"

# -- Login page selectors -----------------------------------------------------
USERNAME_INPUT_SELECTOR = 'input[type="email"][autocomplete="username webauthn"]'
PASSWORD_INPUT_SELECTOR = 'input[type="password"][autocomplete="current-password"]'
SUBMIT_BUTTON_XPATH = "//button[@type='submit']"

# Substrings that mean LinkedIn blocked the login attempt.
LOGIN_CHALLENGE_MARKERS: tuple[str, ...] = (
    "challenge",
    "checkpoint",
    "security-verification",
    "authwall",
)
# URL substrings that indicate a successful post-login landing page.
LOGIN_SUCCESS_MARKERS = "feed"

# -- Search page selectors ----------------------------------------------------
JOB_CONTAINER_SELECTOR = "ul[class='PpKhBdXDfdFmGlMHnIaXEnHrCNpaaa']"
JOB_CARD_SELECTOR = "li[id*='ember']"
JOB_LINK_SELECTOR = "a[href*='/jobs/view/']"
DETAIL_PANEL_SELECTOR = 'div[class*="jobs-search__job-details"]'
NEXT_PAGE_SELECTOR = "button[class*='jobs-search-pagination__button--next']"

# -- Detail panel selectors (scoped to DETAIL_PANEL_SELECTOR) -----------------
TITLE_SELECTOR = "h1[class*='t-24 t-bold inline']"
COMPANY_SELECTOR = "div[class*='job-details-jobs-unified-top-card__company-name']"
LOCATION_SELECTOR = "span[dir='ltr'] span[class='tvm__text tvm__text--low-emphasis']"
DESCRIPTION_SELECTOR = "div[class='mt4'] p[dir='ltr']"
JOB_TYPE_SELECTOR = "div[class='job-details-fit-level-preferences']"
INDUSTRY_INFO_SELECTOR = "div.jobs-company__box div.t-14.mt5"
URL_SELECTOR = "a[id*='ember']"
# Lines inside the t-14.mt5 div that describe company size / followers, not
# the industry itself — used to skip them when guessing the industry.
INDUSTRY_SKIP_KEYWORDS: tuple[str, ...] = (
    "employees",
    "on linkedin",
    "followers",
    "connections",
)

# -- Timing -------------------------------------------------------------------
DEFAULT_KEYWORD = "Data Analyst"
DEFAULT_LOCATION = "Vietnam"
DEFAULT_MAX_PAGES = 2

USER_DATA_DIR = os.path.abspath(os.path.join(os.getcwd(), ".linkedin_profile"))

LOGIN_TIMEOUT = 10
PAGE_LOAD_TIMEOUT = 15.0
PANEL_LOAD_TIMEOUT = 10.0
INDUSTRY_INFO_TIMEOUT = 7.0
NEXT_BUTTON_TIMEOUT = 5.0

# Pause before a click on a freshly scrolled card so the page settles.
CARD_CLICK_DELAY = 0.5

# Per-character delay for human-like typing in the login form.
TYPING_DELAY_RANGE = (0.05, 0.15)

# Pause between successive card clicks (LinkedIn rate-limits aggressive UIs).
CLICK_DELAY_RANGE = (2.0, 5.0)

# Pause after pagination click — page swap is heavier than a card click.
PAGINATION_DELAY_RANGE = (1.5, 2.5)

# How long to give LinkedIn after submit() before checking for challenges.
LOGIN_AFTER_SUBMIT_DELAY_RANGE = (3.0, 5.0)

# Random viewports / user agents so each session looks slightly different.
DEFAULT_VIEWPORTS: tuple[tuple[int, int], ...] = (
    (1366, 768),
    (1920, 1080),
    (1440, 900),
    (1536, 864),
)
DEFAULT_USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
)

# Browser arguments mirror the original spider's stealth-tuned profile,
# trimmed to flags that nodriver/Chromium still respects.
BROWSER_ARGS: tuple[str, ...] = (
    "--disable-extensions",
    "--disable-plugins-discovery",
    "--disable-popup-blocking",
    "--disable-features=IsolateOrigins,site-per-process,TranslateUI",
    "--allow-running-insecure-content",
    "--use-gl=swiftshader",
    "--enable-webgl",
    "--ignore-gpu-blocklist",
    "--font-render-hinting=none",
    "--start-maximized",
    "--disable-gpu",
)
