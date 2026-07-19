"""TopCV crawler — static configuration.

Centralised so the parser, HTTP client, and orchestrator never disagree on
URLs, status semantics, or impersonation profile.
"""

from __future__ import annotations

SOURCE_NAME = "topcv"

BASE_URL = "https://www.topcv.vn/tim-viec-lam"
HOME_URL = "https://www.topcv.vn/"
SEARCH_URL_TEMPLATE = "https://www.topcv.vn/tim-viec-lam-{slug}"

DEFAULT_KEYWORD = "data"
DEFAULT_MAX_PAGES = 2

# -- Search page selectors ----------------------------------------------------
JOB_URL_SELECTOR = 'a[href*="/viec-lam/"], a[href*="/brand/"][href*="tuyen-dung"]'
NEXT_PAGE_SELECTOR = 'a[data-href*="?page="]'

# -- Browser timing -----------------------------------------------------------
PAGE_LOAD_DELAY = 3.0
PAGINATION_DELAY_RANGE = (2.0, 4.0)
SCROLL_INCREMENT = 1000

BROWSER_ARGS: tuple[str, ...] = (
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-blink-features=AutomationControlled",
    "--window-size=1920,1080",
)

# HTTP statuses worth retrying — transient server errors + rate-limit (429).
RETRY_STATUS: frozenset[int] = frozenset({408, 500, 502, 503, 504, 522, 524})

# 403 means Cloudflare invalidated the session — re-run nodriver warm-up
# to refresh cf_clearance instead of just retrying.
BLOCK_STATUS: frozenset[int] = frozenset({403, 429})

# Pin a single curl_cffi profile that matches the Chromium nodriver bundles.
# Rotating would invalidate cf_clearance, which CF binds to (UA + JA3 + IP).
IMPERSONATE_PROFILE = "chrome124"

# Cookie names that prove CF granted us clearance — checked after warm-up.
CF_COOKIE_NAMES: frozenset[str] = frozenset(
    {"cf_clearance", "__cf_bm", "__cflb", "_cfuvid"}
)

# Vietnamese-first Accept-Language so we look like a real local browser.
DEFAULT_ACCEPT_LANGUAGE = "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"

# Title noise TopCV occasionally injects into the H1 from a side menu.
UNWANTED_TITLE_FRAGMENTS: tuple[str, ...] = (
    "Thông tin Địa điểm làm việc Mô tả công việc Yêu cầu ứng viên "
    "Quyền lợi được hưởng Phân tích mức độ phù hợp của bạn với công việc New",
    "Thông tin Tóm tắt Địa điểm làm việc (đã được cập nhật theo Danh mục Hành chính mới) "
    "Mô tả công việc Yêu cầu ứng viên Thu nhập Quyền lợi được hưởng "
    "Thời gian làm việc Phân tích mức độ phù hợp của bạn với công việc New",
)
