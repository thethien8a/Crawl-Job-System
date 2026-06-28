# Hướng dẫn xây dựng Module Thu thập Dữ liệu (Crawl Layer)

Tài liệu này mô tả kiến trúc, các quy ước, và quy trình từng bước để xây dựng
một module crawler mới trong `src/crawl_layer/`. Mọi crawler trong dự án đều
tuân theo một pattern chung, nên sau khi hiểu pattern này bạn có thể thêm một
source mới (ví dụ: LinkedIn, CareerBuilder...) chỉ bằng cách "sao chép và điều chỉnh".

> Mọi tham chiếu code/file dưới dạng link có thể click để nhảy thẳng tới dòng tương ứng.

---

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Cấu trúc thư mục của một module crawler](#2-cấu-trúc-thư-mục-của-một-module-crawler)
3. [Data Model — JobItem & per-site subclass](#3-data-model--jobitem--per-site-subclass)
4. [File config.py — cấu hình tĩnh](#4-file-configpy--cấu-hình-tĩnh)
5. [Lớp Crawler — orchestration flow](#5-lớp-crawler--orchestration-flow)
6. [Hai stack kỹ thuật: aiohttp vs nodriver](#6-hai-stack-kỹ-thuật-aiohttp-vs-nodriver)
7. [Parser — chuyển HTML thành JobItem](#7-parser--chuyển-html-thành-jobitem)
8. [utils.py — helper nhỏ](#8-utilspy--helper-nhỏ)
9. [Entry point __main__.py — CLI & Windows patch](#9-entry-point-__main__py--cli--windows-patch)
10. [loader.save_to_temp — ghi dữ liệu ra temp JSONL](#10-loadersave_to_temp--ghi-dữ-liệu-ra-temp-jsonl)
11. [Tích hợp với Bronze layer (pipeline hạ lưu)](#11-tích-hợp-với-bronze-layer-pipeline-hạ-lưu)
12. [Tích hợp với Airflow (DAG factory)](#12-tích-hợp-với-airflow-dag-factory)
13. [Quy trình từng bước: thêm một crawler mới](#13-quy-trình-từng-bước-thêm-một-crawler-mới)
14. [Các gotcha bắt buộc phải nhớ](#14-các-gotcha-bắt-buộc-phải-nhớ)
15. [Cách verify (không cần pytest)](#15-cách-verify-không-cần-pytest)

---

## 1. Tổng quan kiến trúc

`crawl_layer` là **stage đầu tiên** của pipeline Lakehouse-Lite. Nhiệm vụ duy nhất:
cào dữ liệu thô từ các site tuyển dụng và ghi ra file JSONL cục bộ trong
`src/crawl_layer/temp_data/`. Các stage phía sau (Validate → Bronze → Silver →
Supabase → Gold) sẽ đọc các file temp này.

Luồng tổng thể (chỉ crawl):

```
CLI / Airflow DAG
   │  python -m src.crawl_layer.crawler.<site> --keyword ... --max-pages N
   ▼
Crawler.crawl()          ← orchestration: phân trang, dedup URL, batch save
   ├── (aiohttp stack)   HttpClient.fetch() → HTML
   ├── (nodriver stack)  Browser.login()/iter_job_panels() → HTML
   ▼
Parser.parse_*()         ← HTML → JobItem dataclass
   ▼
Crawler._flush_batch()   → save_to_temp([asdict(item)], SOURCE_NAME, ENTITY_NAME)
   ▼
src/crawl_layer/temp_data/<source>_jobs_YYYYMMDD.jsonl   (append-only)
```

Nguyên tắc cốt lõi: **crawl layer chỉ biết ghi ra temp file**, không bao giờ
talking tới S3/Supabase trực tiếp. Việc upload lên Bronze là trách nhiệm của
[`load_to_bronze()`](src/crawl_layer/utils/loader.py:1) trong storage layer.

---

## 2. Cấu trúc thư mục của một module crawler

Mỗi site là một package con trong [`src/crawl_layer/crawler/`](src/crawl_layer/crawler/).
Cấu trúc chuẩn (xem [`topcv`](src/crawl_layer/crawler/topcv/) và
[`itviec`](src/crawl_layer/crawler/itviec/)):

```
src/crawl_layer/crawler/<site>/
├── __init__.py          # rỗng (chỉ đánh dấu package)
├── __main__.py          # CLI entry point: argparse + asyncio.run
├── config.py            # hằng số tĩnh: URL, selector, timeout, tên source
├── crawler.py           # class <Site>Crawler — orchestration flow
├── parser.py            # class <Site>Parser — HTML -> JobItem
├── http_client.py       # (stack aiohttp) class <Site>HttpClient — fetch + retry
│   ── hoặc ──
├── browser.py           # (stack nodriver) class <Site>Browser — login + nav
└── utils.py             # helper nhỏ (encode keyword, sanitize text...)
```

Lưu ý: **không bao giờ có `__init__.py` ở cấp `src/`** — toàn bộ import dùng
absolute `src.*`. Xem thêm [gotcha #1](#14-các-gotcha-bắt-buộc-phải-nhớ).

---

## 3. Data Model — JobItem & per-site subclass

Toàn bộ crawler chia sẻ một dataclass gốc [`JobItem`](src/crawl_layer/data_model/data_class.py:3)
chứa các trường chung mà mọi site đều có:

```python
@dataclass
class JobItem:
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    location: str | None = None
    job_industry: str | None = None
    job_description: str | None = None
    source_site: str | None = None
    job_url: str | None = None
    search_keyword: str | None = None
    scraped_at: str | None = None
    salary: Optional[str] = None
    benefits: str | None = None
    requirements: str | None = None
```

Mỗi site kế thừa và **chỉ thêm các trường đặc thù**:

- [`TopCVJobItem`](src/crawl_layer/data_model/data_class.py:18) thêm `company_size`,
  `job_type`, `experience_level`, `education_level`, `job_position`, `job_deadline`.
- [`ITViecJobItem`](src/crawl_layer/data_model/data_class.py:27) chỉ thêm `company_size`.

Quy tắc khi thêm source mới:
1. Tạo subclass `<NewSite>JobItem(JobItem)` trong
   [`src/crawl_layer/data_model/data_class.py`](src/crawl_layer/data_model/data_class.py).
2. Chỉ thêm trường mà site đó **thực sự có** — không sao chép bừa.
3. Mọi trường mặc định `None` để parser có thể bỏ qua trường thiếu.
4. Các trường ở đây quyết định **schema Silver hạ lưu** (Silver được derive từ
   [`SilverJobItem`](src/storage_layer/MinIO_S3/layer/silver/data_model/data_class.py)
   chứ không phải crawl layer, nhưng phải khớp tên cột khi clean).

---

## 4. File config.py — cấu hình tĩnh

Mọi hằng số (URL, CSS/XPath selector, timeout, cookie, profile trình duyệt)
phải tập trung trong `config.py` để parser/http/browser/orchestrator **không bao giờ
lệch nhau**. Ví dụ:

**Stack aiohttp** — [`topcv/config.py`](src/crawl_layer/crawler/topcv/config.py:9):

```python
SOURCE_NAME = "topcv"
BASE_URL = "https://www.topcv.vn/tim-viec-lam"
DEFAULT_KEYWORD = "data"
DEFAULT_MAX_PAGES = 2
RETRY_STATUS: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504, 522, 524})
BLOCK_STATUS: frozenset[int] = frozenset({403})       # Cloudflare -> warm-up lại
IMPERSONATE_PROFILE = "chrome131"                     # curl_cffi profile cố định
```

**Stack nodriver** — [`itviec/config.py`](src/crawl_layer/crawler/itviec/config.py:13):

```python
from src.storage_layer.MinIO_S3.config.path import DEFAULT_ENTITY_NAME

SOURCE_NAME = "itviec"
ENTITY_NAME = DEFAULT_ENTITY_NAME                     # = "jobs"
BASE_URL = "https://itviec.com"
LOGIN_URL = "https://itviec.com/sign_in"
USERNAME_ENV = "ITVIEC_USERNAME"                      # tên env var trong .env
PASSWORD_ENV = "ITVIEC_PASSWORD"

# Selectors — copy y nguyên từ trang thật, class name có thể bị hash cố tình
JOB_CARD_SELECTOR = "h3[data-url*='/it-jobs/']"
PREVIEW_PANEL_SELECTOR = "div[class*='preview-job-wrapper']"
NEXT_PAGE_SELECTOR = "a[rel='next']"

# Timing
LOGIN_TIMEOUT = 20.0
PAGINATION_DELAY_RANGE = (2.0, 4.0)

# Browser stealth args
BROWSER_ARGS: tuple[str, ...] = ("--no-sandbox", "--disable-blink-features=AutomationControlled", ...)
```

Quy tắc:
- `SOURCE_NAME` phải khớp **chính xác** với tiền tố file temp (`<source>_jobs_*.jsonl`)
  và với `bronze_source` trong Airflow [`SITE_CONFIGS`](src/orchestration_layer/dags/_dag_factory.py:22).
- Không hardcode secret — chỉ trỏ tới tên env var.
- Selector "nhìn xấu/đang hash" thì giữ nguyên, không "dọn dẹp" nếu chưa test trang thật.

---

## 5. Lớp Crawler — orchestration flow

`crawler.py` chỉ chứa **luồng high-level**, không chứa chi tiết fetch/parse.
Pattern chung cho cả hai stack (xem [`TopcvCrawler`](src/crawl_layer/crawler/topcv/crawler.py:32)
và [`ItviecCrawler`](src/crawl_layer/crawler/itviec/crawler.py:26)):

```python
class <Site>Crawler:
    def __init__(self, keyword, max_pages, ...):
        self.keyword = keyword
        self.max_pages = max_pages
        self.<transport> = <Site>HttpClient(...) or <Site>Browser(...)
        self.parser = <Site>Parser()
        self._seen_urls: set[str] = set()      # dedup URL giữa các trang

    async def crawl(self) -> list[<Site>JobItem]:
        items: list[<Site>JobItem] = []
        async with self.<transport>:           # mở/đóng session hoặc browser
            for page_num in range(1, self.max_pages + 1):
                temp_items: list[<Site>JobItem] = []
                page_items = await self._scrape_current_page()   # hoặc _collect + _scrape_details_batch
                items.extend(page_items)
                temp_items.extend(page_items)
                self._flush_batch(temp_items, page_num)          # ghi ra temp NGAY mỗi trang
                # ... next page ...
        return items

    def _flush_batch(self, page_items, page_num) -> None:
        if not page_items:
            return
        save_to_temp([asdict(item) for item in page_items], SOURCE_NAME, ENTITY_NAME)
```

Hai điểm bắt buộc:
1. **Dedup URL** bằng `self._seen_urls` để không cào trùng khi các trang overlap.
2. **Streaming per-page save** qua [`_flush_batch()`](src/crawl_layer/crawler/topcv/crawler.py:137):
   ghi ra temp sau mỗi trang, nên nếu crash ở trang 5 thì trang 1–4 vẫn an toàn
   trên đĩa và bộ nhớ luôn bị chặn (bounded).

Khác biệt duy nhất giữa hai stack nằm ở cách lấy HTML:
- **aiohttp (TopCV)**: phân 2 pha — [`_collect_page_urls()`](src/crawl_layer/crawler/topcv/crawler.py:101)
  lấy list URL từ trang search, rồi [`_scrape_details_batch()`](src/crawl_layer/crawler/topcv/crawler.py:116)
  dùng `asyncio.gather` fetch song song (bounded bởi semaphore trong HttpClient).
- **nodriver (ITviec)**: 1 pha — [`_scrape_current_page()`](src/crawl_layer/crawler/itviec/crawler.py:86)
  duyệt qua [`browser.iter_job_panels()`](src/crawl_layer/crawler/itviec/browser.py) tuần tự
  (click từng card → snapshot panel).

---

## 6. Hai stack kỹ thuật: aiohttp vs nodriver

Dự án dùng **hai stack khác nhau** tùy độ "khó" của site:

| Tiêu chí | aiohttp (TopCV) | nodriver (ITviec / VietnamWorks) |
|---|---|---|
| File transport | [`http_client.py`](src/crawl_layer/crawler/topcv/http_client.py) | [`browser.py`](src/crawl_layer/crawler/itviec/browser.py) |
| Có cần login? | Không | Có (Cloudflare + cookie đăng nhập) |
| Concurrency | Cao — `asyncio.gather` + semaphore | Thấp — 1 session tuần tự |
| Linux/Docker cần | không | `xvfb-run` (headless Chrome) |
| Windows cần patch ProactorEventLoop? | không | **có** (xem [§9](#9-entry-point-__main__py--cli--windows-patch)) |

### 6.1. Stack aiohttp — HttpClient

[`TopcvHttpClient`](src/crawl_layer/crawler/topcv/http_client.py) sở hữu
session (curl_cffi để giả lập JA3), semaphore giới hạn concurrency, và logic
retry/backoff. Phương thức cốt lõi là [`fetch()`](src/crawl_layer/crawler/topcv/http_client.py:71):

- `BLOCK_STATUS` (403) → cơ chế "prober": request đầu tiên phát hiện block sẽ
  giăng `asyncio.Event` chặn toàn bộ request khác, ngủ 62s để Cloudflare clearance
  tái lập, rồi mở rào khi thành công. Tránh 4 request cùng đập vào CF.
- `RETRY_STATUS` (429/5xx) → exponential backoff `min(120, 2**attempt) + jitter`.
- Hỗ trợ `async with` qua `__aenter__`/`__aexit__` để tự đóng session.

Khi thêm source mới dùng aiohttp: sao chép cấu trúc `HttpClient` này, đổi
`IMPERSONATE_PROFILE`/`RETRY_STATUS`/`BLOCK_STATUS` theo đặc thù site.

### 6.2. Stack nodriver — Browser

[`ItviecBrowser`](src/crawl_layer/crawler/itviec/browser.py) quản lý headless
Chrome qua `nodriver`, có các method:
- `login()` — điền form, chờ marker đăng nhập, dismiss modal (raise
  `ItviecLoginError` khi fail).
- `open_search(keyword)` — mở trang search.
- `iter_job_panels()` — **async generator** yielding `(job_url, panel_html)` cho
  mỗi card (click → snapshot panel).
- `go_to_next_page()` — click nút "next", trả `bool`.

Lý do ITviec dùng 1 session tuần tự: Cloudflare gắn clearance với (UA + IP +
cookie đăng nhập), nên concurrency sẽ tự đánh bại mục đích.

---

## 7. Parser — chuyển HTML thành JobItem

`parser.py` thuần túy: nhận HTML (và URL + keyword), trả về `<Site>JobItem`.
Không có I/O, không có state — rất dễ test độc lập.

- [`TopcvParser`](src/crawl_layer/crawler/topcv/parser.py): [`parse_search_page()`](src/crawl_layer/crawler/topcv/crawler.py:101)
  trả `(list[str] job_urls, next_url)`, và `parse_job_detail(html, url, keyword)` → `TopCVJobItem`.
  Dùng `parsel`/`selectolax` để trích từng trường (`_extract_title`, `_extract_salary`...).
- [`ItviecParser`](src/crawl_layer/crawler/itviec/parser.py): `parse_preview_panel(html, url, keyword)` → `ITViecJobItem`
  (parse ngay panel preview thay vì load trang detail riêng).

Quy tắc:
- Đặt mọi selector trong [`config.py`](src/crawl_layer/crawler/itviec/config.py:13), không hardcode trong parser.
- Trả `None` cho trường thiếu thay vì raise — để batch không bị vỡ vì 1 tin lỗi.
- Khi site đổi markup, **chỉ sửa config + parser**, không đụng crawler flow.

---

## 8. utils.py — helper nhỏ

Các helper nhỏ gọn, không phụ thuộc network. Ví dụ
[`topcv/utils.py`](src/crawl_layer/crawler/topcv/utils.py):
- [`encode_input()`](src/crawl_layer/crawler/topcv/utils.py:5) — `"data analyst"` → `"data-analyst"` cho URL slug.
- `join_clean()`, `sanitize_title()` — dọn text thừa.

Giữ utils **thuần function, không side-effect**.

---

## 9. Entry point __main__.py — CLI & Windows patch

`__main__.py` là điểm vào duy nhất khi chạy `python -m src.crawl_layer.crawler.<site>`.
Pattern (xem [`itviec/__main__.py`](src/crawl_layer/crawler/itviec/__main__.py:58)):

```python
async def _run(keyword, max_pages, headless) -> None:
    logging.basicConfig(level=logging.INFO, format="...")
    crawler = <Site>Crawler(keyword=keyword, max_pages=max_pages, headless=headless)
    items = await crawler.crawl()
    logging.info("Exported %d items to <site>_jobs.jsonl", len(items))

def main() -> None:
    parser = argparse.ArgumentParser(description="<Site> async crawler")
    parser.add_argument("--keyword", default=DEFAULT_KEYWORD)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--headless", action="store_true")   # chỉ nodriver
    args = parser.parse_args()
    asyncio.run(_run(args.keyword, args.max_pages, ...))

if __name__ == "__main__":
    main()
```

### Windows ProactorEventLoop patch (BẮT BUỘC với nodriver)

Các crawler nodriver (ITviec, VietnamWorks) có một khối patch ở đầu
[`__main__.py`](src/crawl_layer/crawler/itviec/__main__.py:16) để **im lặng lỗi
`__del__`** của `ProactorEventLoop` trên Windows (Chrome subprocess đóng socket
gây exception ồn khi thoát):

```python
if sys.platform == "win32":
    from asyncio.proactor_events import _ProactorBasePipeTransport
    from asyncio.base_subprocess import BaseSubprocessTransport
    def silence_del(cls): ...
    silence_del(_ProactorBasePipeTransport)
    silence_del(BaseSubprocessTransport)
```

**Không được xóa khối này** — xem [gotcha #4](#14-các-gotcha-bắt-buộc-phải-nhớ).
Crawler aiohttp (TopCV) không cần patch này.

---

## 10. loader.save_to_temp — ghi dữ liệu ra temp JSONL

Hàm duy nhất mọi crawler dùng để lưu: [`save_to_temp()`](src/crawl_layer/utils/loader.py:11).

```python
def save_to_temp(data, source_name, entity_name="jobs"):
    # file: <source>_jobs_<YYYYMMDD>.jsonl  trong TEMP_DIR
    # mode 'a' (APPEND) — ghi mỗi item một dòng JSON
```

Đặc điểm quan trọng:
- **Append-only**: chạy lại cùng ngày sẽ **cộng dồn**, không ghi đè. Bronze upload
  sau đó mới dọn temp (xem [§11](#11-tích-hợp-với-bronze-layer-pipeline-hạ-lưu)).
- Tên file theo ngày: `<source>_jobs_YYYYMMDD.jsonl`, ví dụ
  [`itviec_jobs_20260621.jsonl`](src/crawl_layer/temp_data/itviec_jobs_20260621.jsonl).
- Đường dẫn gốc [`TEMP_DIR`](src/crawl_layer/config/path.py:4) = `src/crawl_layer/temp_data`.
- `ENTITY_NAME` mặc định `"jobs"` (từ [`DEFAULT_ENTITY_NAME`](src/storage_layer/MinIO_S3/config/path.py:9)).

Crawler gọi qua `_flush_batch`: `save_to_temp([asdict(item) for item in page_items], SOURCE_NAME, ENTITY_NAME)`.

---

## 11. Tích hợp với Bronze layer (pipeline hạ lưu)

Crawl layer **không tự upload S3**. Sau khi crawl xong, Bronze layer đọc temp:

```bash
python -m src.storage_layer.MinIO_S3.layer.bronze.main --source topcv
```

File [`bronze/main.py`](src/storage_layer/MinIO_S3/layer/bronze/main.py:14) gọi
[`load_to_bronze()`](src/crawl_layer/utils/loader.py:1) (cùng file với `save_to_temp`):

1. Gzip các file `<source>_jobs_*.jsonl` trong `temp_data/`.
2. Upload `.gz` lên S3 Bronze theo key `source/<source>/...` (xem
   [`BronzeBucketPaths`](src/storage_layer/MinIO_S3/config/path.py:13)).
3. **Xóa** cả `.gz` lẫn `.jsonl` của source đó cục bộ → destructive.

Hệ quả: tên `SOURCE_NAME` trong `config.py` phải khớp `--source` của bronze và
`bronze_source` trong Airflow. Mismatch = dữ liệu nằm lại temp không được upload.

---

## 12. Tích hợp với Airflow (DAG factory)

Airflow **không import** module crawler — nó chỉ điều phối qua `DockerOperator`.
Mỗi site có một DAG siêu mỏng gọi factory, ví dụ
[`crawl_topcv.py`](src/orchestration_layer/dags/crawl_topcv.py:4):

```python
from _dag_factory import create_crawl_dag
dag = create_crawl_dag("topcv")
```

Factory [`_dag_factory.py`](src/orchestration_layer/dags/_dag_factory.py) giữ hai
bảng cấu hình mà bạn **phí phải cập nhật khi thêm source**:

1. [`SITE_CONFIGS`](src/orchestration_layer/dags/_dag_factory.py:22) — map tên site
   sang `crawl_module`, `validate_module`, `bronze_source`, `silver_module`, `supabase_site`.
2. [`CRAWL_SCHEDULES`](src/orchestration_layer/dags/_dag_factory.py:49) — cron mỗi site
   (offset để không cào song song: topcv `0 */3`, itviec `15 */3`, vietnamworks `30 */3`).

Lệnh crawl chạy trong container ([dòng 130–144](src/orchestration_layer/dags/_dag_factory.py:130)):

```
xvfb-run --server-args='-screen 0 1280x1024x24' \
  python -m src.crawl_layer.crawler.<crawl_module> \
  --keyword {{ params.keyword }} --max-pages {{ params.max_pages }}
```

`xvfb-run` chỉ cần cho nodriver; với aiohttp nó vô hại. DAG params override được
qua Airflow UI "Trigger DAG w/ config": `{"keyword": "...", "max_pages": N}`.

---

## 13. Quy trình từng bước: thêm một crawler mới

Giả sử thêm site `careerbuzz` (dùng nodriver vì cần login):

**Bước 1 — Data model.** Thêm subclass trong
[`src/crawl_layer/data_model/data_class.py`](src/crawl_layer/data_model/data_class.py):

```python
@dataclass
class CareerBuzzJobItem(JobItem):
    company_size: Optional[str] = None
    # thêm các trường đặc thù CareerBuzz, mặc định None
```

**Bước 2 — Tạo package.** Tạo thư mục `src/crawl_layer/crawler/careerbuzz/` với:
`__init__.py` (rỗng), `config.py`, `browser.py` (hoặc `http_client.py`),
`parser.py`, `crawler.py`, `utils.py`, `__main__.py`.

**Bước 3 — `config.py`.** Đặt `SOURCE_NAME = "careerbuzz"`, `ENTITY_NAME = DEFAULT_ENTITY_NAME`,
URL, selector, timeout, env var cho credential, `BROWSER_ARGS` (nếu nodriver).

**Bước 4 — `parser.py`.** Viết `CareerBuzzParser` với method parse trả `CareerBuzzJobItem`.
Test độc lập với HTML sample trước.

**Bước 5 — `browser.py` / `http_client.py`.** Sao chép từ site tương đồng stack
(ITviec cho nodriver, TopCV cho aiohttp), đổi selector/timing. Đảm bảo có
`__aenter__`/`__aexit__` và async generator `iter_job_panels()` (nodriver) hoặc
`fetch()` (aiohttp).

**Bước 6 — `crawler.py`.** Viết `CareerBuzzCrawler` theo pattern [§5](#5-lớp-crawler--orchestration-flow):
`crawl()` → per-page scrape → `_flush_batch()` → `save_to_temp`.

**Bước 7 — `__main__.py`.** Sao chép từ [`itviec/__main__.py`](src/crawl_layer/crawler/itviec/__main__.py)
**bao gồm khối Windows ProactorEventLoop patch** nếu dùng nodriver. Đặt
`argparse` với `--keyword`/`--max-pages`/`--headless`.

**Bước 8 — Verify cục bộ.** Chạy với 1 trang (xem [§15](#15-cách-verify-không-cần-pytest)):

```bash
python -m src.crawl_layer.crawler.careerbuzz --keyword data --max-pages 1 --headless
```

Kiểm tra `src/crawl_layer/temp_data/careerbuzz_jobs_<today>.jsonl` có dữ liệu.

**Bước 9 — Tích hợp Airflow.** Trong [`_dag_factory.py`](src/orchestration_layer/dags/_dag_factory.py):
thêm entry `SITE_CONFIGS["careerbuzz"] = {...}` và `CRAWL_SCHEDULES["careerbuzz"] = "45 */3 * * *"`.
Tạo file DAG `src/orchestration_layer/dags/crawl_careerbuzz.py`:

```python
from _dag_factory import create_crawl_dag
dag = create_crawl_dag("careerbuzz")
```

**Bước 10 — Tích hợp Bronze.** Đảm bảo `bronze_source = "careerbuzz"` khớp
`SOURCE_NAME`. Nếu cần validate, thêm module trong
[`src/storage_layer/MinIO_S3/layer/local_temp/validation/`](src/storage_layer/MinIO_S3/layer/local_temp/validation/)
và cập nhật `validate_module` trong `SITE_CONFIGS`.

---

## 14. Các gotcha bắt buộc phải nhớ

1. **Không có `__init__.py` ở `src/`** — toàn bộ import absolute `src.*`. Luôn
   `cd` về repo root trước khi `python -m`. Dockerfile đặt `PYTHONPATH=/app` để mirror.
2. **Temp file append-only** — chạy lại cùng ngày **cộng dồn**. Dọn temp chỉ do
   Bronze upload thực hiện (destructive: gz + xóa). Dùng `--source` để cô lập 1 site.
3. **`MinIO_S3` là tên legacy** — thực ra nói chuyện AWS S3 qua `boto3`
   (`get_s3_client()`), không phải MinIO.
4. **Windows ProactorEventLoop patch** ở `__main__.py` của mọi crawler nodriver
   (ITviec, VietnamWorks) — **không xóa**. aiohttp (TopCV) không cần.
5. **nodriver cần `xvfb-run`** trên Linux/Docker (DAG factory đã wrap sẵn).
6. **`SOURCE_NAME` phải nhất quán** xuyên suốt: `config.py` ↔ tên file temp
   (`<source>_jobs_*.jsonl`) ↔ `bronze_source` trong `SITE_CONFIGS` ↔
   `--source` của [`bronze/main.py`](src/storage_layer/MinIO_S3/layer/bronze/main.py:14).
7. **Selector "xấu" có thể cố tình** — nhiều site hash class name để chống bot;
   copy y nguyên, test trang thật trước khi "dọn dẹp".
8. **Streaming per-page save** qua `_flush_batch` là bắt buộc — không gom hết rồi
   mới ghi, sẽ mất toàn bộ nếu crash cuối crawl.
9. **Dedup URL** (`self._seen_urls`) giữa các trang để tránh cào trùng khi overlap.
10. **Airflow chỉ orchestrate** — không bao giờ import module crawler/storage;
    mọi logic chạy trong `lakehouse-crawler` container qua `DockerOperator`.

---

## 15. Cách verify (không cần pytest)

Dự án **không có pytest/ruff/CI**. Verify bằng cờ tiết kiệm:

```bash
# Từ repo root (d:/Practice/Scrapy/Lakehouse-Lite)

# Crawler — giới hạn 1 trang để nhanh
python -m src.crawl_layer.crawler.topcv --keyword data --max-pages 1
python -m src.crawl_layer.crawler.itviec --keyword data --max-pages 1 --headless
python -m src.crawl_layer.crawler.vietnamworks --keyword data --max-pages 1 --headless

# Sau đó kiểm tra temp file
ls src/crawl_layer/temp_data/

# Silver — dùng --no_save để chạy dry-run không ghi
python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_topcv.main_process \
  --from_date 2026-06-21 --to_date 2026-06-21 --no_save

# Bronze — upload 1 source để thử
python -m src.storage_layer.MinIO_S3.layer.bronze.main --source topcv
```

Kiểm tra nhanh schema: mở file `*_jobs_*.jsonl`, mỗi dòng là 1 JSON object; các
key phải khớp trường trong `<Site>JobItem` (do dùng `asdict()`).

---

## Tham chiếu nhanh

| Vai trò | File |
|---|---|
| Data model gốc | [`JobItem`](src/crawl_layer/data_model/data_class.py:3) |
| Đường dẫn temp | [`TEMP_DIR`](src/crawl_layer/config/path.py:4) |
| Hàm lưu | [`save_to_temp()`](src/crawl_layer/utils/loader.py:11) |
| Crawler mẫu (aiohttp) | [`TopcvCrawler`](src/crawl_layer/crawler/topcv/crawler.py:32) |
| Crawler mẫu (nodriver) | [`ItviecCrawler`](src/crawl_layer/crawler/itviec/crawler.py:26) |
| HTTP client mẫu | [`TopcvHttpClient.fetch()`](src/crawl_layer/crawler/topcv/http_client.py:71) |
| Browser mẫu | [`ItviecBrowser`](src/crawl_layer/crawler/itviec/browser.py) |
| Config mẫu | [`itviec/config.py`](src/crawl_layer/crawler/itviec/config.py:13) |
| Entry point mẫu | [`itviec/__main__.py`](src/crawl_layer/crawler/itviec/__main__.py:58) |
| Bronze entry | [`bronze/main.py`](src/storage_layer/MinIO_S3/layer/bronze/main.py:14) |
| DAG factory | [`_dag_factory.py`](src/orchestration_layer/dags/_dag_factory.py) |
| SITE_CONFIGS | [`SITE_CONFIGS`](src/orchestration_layer/dags/_dag_factory.py:22) |
