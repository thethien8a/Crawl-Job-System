"""ITviec crawler — static configuration.

Centralised so the parser, browser, and orchestrator never disagree on
URLs, selectors, or timing. Selectors are ported verbatim from the
original Scrapy spider — do not "tidy" them without testing the live page,
ITviec class names are partially hashed and look noisy on purpose.
"""

from __future__ import annotations

SOURCE_NAME = "itviec"
ENTITY_NAME = "jobs"

BASE_URL = "https://itviec.com"
LOGIN_URL = "https://itviec.com/sign_in"
SEARCH_URL_TEMPLATE = "https://itviec.com/it-jobs/{slug}"

# Env vars that hold credentials (matches .env.example, not the legacy
# ITVIEC_EMAIL / ITVIEC_PASS used by the old spider).
USERNAME_ENV = "ITVIEC_USERNAME"
PASSWORD_ENV = "ITVIEC_PASSWORD"

# Vietnamese-first to look like a local browser (ITviec is a VN site).
DEFAULT_ACCEPT_LANGUAGE = "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"

# -- Login page selectors -----------------------------------------------------
EMAIL_INPUT_SELECTOR = "input#user_email"
PASSWORD_INPUT_SELECTOR = "input#user_password"
SUBMIT_BUTTON_XPATH = (
    "//button[@type='submit' and .//span[contains(text(),'Sign In with Email')]]"
)
LOGGED_IN_MARKER_SELECTOR = "input#query"

# Modal that occasionally blurs the page after login; dismiss with "Remind me later".
POST_LOGIN_MODAL_SELECTOR = "div.modal-content.text-center"
REMIND_LATER_BUTTON_XPATH = "//button[contains(text(), 'Remind me later')]"

# -- Search page selectors ----------------------------------------------------
SEARCH_LOADED_SELECTOR = "h1[class*='headline-total-jobs']"
JOB_CARD_SELECTOR = "h3[data-url*='/it-jobs/']"
PREVIEW_PANEL_SELECTOR = "div[class*='preview-job-wrapper']"
NEXT_PAGE_SELECTOR = "a[rel='next']"

# -- Detail panel selectors (scoped to PREVIEW_PANEL_SELECTOR) ----------------
TITLE_SELECTOR = "h2[class*='text-it-black text-hover-red']"
COMPANY_SELECTOR = "a[href*='/companies/'][class*='normal-text']"
SALARY_SELECTOR = "span.ips-2.fw-500"
LOCATION_SELECTOR = "div.d-inline-block.text-dark-grey"
DESCRIPTION_SELECTOR = "section[class='job-description']"
REQUIREMENTS_SELECTOR = "section[class='job-experiences']"
BENEFITS_SELECTOR = "section[class='job-why-love-working']"
INDUSTRY_SELECTOR = "div[class='d-inline-flex text-wrap']"
COMPANY_SIZE_SELECTOR = "small[class='normal-text text-it-black col']"
# -- Timing -------------------------------------------------------------------
DEFAULT_KEYWORD = "data"
LOGIN_TIMEOUT = 20.0
PAGE_LOAD_TIMEOUT = 15.0
PANEL_LOAD_TIMEOUT = 10.0
NEXT_BUTTON_TIMEOUT = 5.0

# Pause between successive card clicks; ITviec re-renders the side panel and
# we want to give it room to settle without thrashing the JS event loop.
CARD_CLICK_DELAY = 0.5

# Pause after pagination click — page swap is heavier than a card click.
PAGINATION_DELAY_RANGE = (2.0, 4.0)

# Browser arguments mirror the original spider's stealth-tuned UC profile.
BROWSER_ARGS: tuple[str, ...] = (
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--disable-blink-features=AutomationControlled",
    "--disable-extensions",
    "--disable-default-apps",
    "--disable-sync",
    "--disable-translate",
    "--hide-scrollbars",
    "--no-first-run",
    "--disable-prompt-on-repost",
    "--window-size=1920,1080",
    f"--lang={DEFAULT_ACCEPT_LANGUAGE.split(',')[0]}",
)
