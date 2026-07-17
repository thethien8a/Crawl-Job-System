# Tài liệu Công nghệ — Lakehouse-Lite

> Tài liệu tham khảo toàn diện về các công nghệ được sử dụng trong dự án Lakehouse-Lite, mục đích sử dụng trong hệ thống, và lý do lựa chọn từng công nghệ.

---

## Mục lục

1. [Tổng quan Kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Công nghệ Thu thập Dữ liệu](#2-công-nghệ-thu-thập-dữ-liệu)
3. [Công nghệ Lưu trữ Dữ liệu](#3-công-nghệ-lưu-trữ-dữ-liệu)
4. [Công nghệ Xử lý Dữ liệu](#4-công-nghệ-xử-lý-dữ-liệu)
5. [Công nghệ Phục vụ & Báo cáo Dữ liệu](#5-công-nghệ-phục-vụ--báo-cáo-dữ-liệu)
6. [Công nghệ Điều phối Pipeline](#6-công-nghệ-điều-phối-pipeline)
7. [Công nghệ Giám sát](#7-công-nghệ-giám-sát)
8. [Công nghệ Hạ tầng & DevOps](#8-công-nghệ-hạ-tầng--devops)
9. [Bảng Tổng hợp Công nghệ](#9-bảng-tổng-hợp-công-nghệ)
10. [Kiến trúc Luồng Dữ liệu](#10-kiến-trúc-luồng-dữ-liệu)
11. [Lý do Lựa chọn Công nghệ trong Bối cảnh Đồ án Tốt nghiệp](#11-lý-do-lựa-chọn-công-nghệ-trong-bối-cảnh-đồ-án-tốt-nghiệp)

---

## 1. Tổng quan Kiến trúc

Lakehouse-Lite triển khai **Kiến trúc Medallion** (Bronze → Silver → Gold) cho dữ liệu thị trường việc làm được thu thập từ ba trang web tuyển dụng Việt Nam: TopCV, ITviec và VietnamWorks. Hệ thống được tổ chức thành sáu tầng riêng biệt:

| Tầng | Trách nhiệm |
|------|-------------|
| **Crawl Layer** | Thu thập dữ liệu web từ các trang tuyển dụng |
| **Storage Layer** | Lưu trữ dữ liệu qua các giai đoạn Bronze, Silver, Gold của kiến trúc Medallion và phục vụ qua Supabase |
| **Processing Layer** | Làm sạch, biến đổi, xác minh, và embedding dữ liệu |
| **BI Report Layer** | Báo cáo trí tuệ doanh nghiệp và bảng điều khiển tương tác qua Power BI |
| **CV Recommendation Layer** | Gợi ý việc làm theo CV dựa trên vector similarity + skill overlap taxonomy |
| **Orchestration Layer** | Lập lịch pipeline và quản lý phụ thuộc |
| **Monitoring Layer** | Khả năng quan sát, thu thập chỉ số, và trực quan hóa bảng điều khiển |

Dữ liệu chạy qua pipeline như sau:

```
Crawl → Validate → Bronze (S3) → Silver (S3) ──→ Gold (MotherDuck/DuckDB)
                                         │                        │
                                    ┌────┤                        ↓
                                    │    │                   Power BI (.pbix)
                          Seed      │    ↓                        │
                     Taxonomy ──────┤  Supabase              BI Reports
                   (Google Sheets   │  (PostgreSQL)
                    + CSV fallback)  ↓
                                    Frontend API

Silver (S3) ──→ Embed (OpenRouter) ──→ Qdrant (Vector DB) ──→ CV Recommendation
                                         ↑
                                    CV Upload ──→ Parse + Embed (OpenRouter)
```

Các công nghệ trong hệ thống được phân loại theo chức năng như sau:

| Danh mục chức năng | Công nghệ chính | Vai trò |
|---------------------|-----------------|---------|
| **Thu thập dữ liệu** | aiohttp, curl_cffi, nodriver, parsel, lxml | Thu thập dữ liệu web từ các trang tuyển dụng |
| **Lưu trữ dữ liệu** | Amazon S3, DuckDB, MotherDuck, Supabase, Qdrant | Lưu trữ dữ liệu qua các giai đoạn Bronze → Silver → Gold và phục vụ |
| **Xử lý dữ liệu** | Polars, FlashText, RapidFuzz, Great Expectations, gspread, OpenRouter Embedding, pdfplumber, python-docx | Làm sạch, biến đổi, xác minh, embedding dữ liệu |
| **Phục vụ & Báo cáo** | Power BI | Báo cáo trí tuệ doanh nghiệp và trực quan hóa |
| **Điều phối pipeline** | Apache Airflow, PostgreSQL, StatsD Exporter | Lập lịch pipeline và quản lý phụ thuộc |
| **Giám sát** | Prometheus, Grafana, Caddy, Altair, Nginx | Khả năng quan sát, thu thập chỉ số, và trực quan hóa |
| **Hạ tầng & DevOps** | Docker, Docker Compose, Python, python-dotenv, Pydantic, PyYAML | Nền tảng container hóa và quản lý cấu hình |

---

## 2. Công nghệ Thu thập Dữ liệu

> Tầng thu thập dữ liệu sử dụng hai phương pháp khác nhau: HTTP client nhẹ cho trang không có bảo vệ anti-bot mạnh (TopCV) và tự động hóa trình duyệt headless cho trang có bảo vệ Cloudflare hoặc yêu cầu JavaScript rendering (ITviec, VietnamWorks).

### 2.1 aiohttp

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 3.13.5 |
| **Vai trò** | HTTP client bất đồng bộ cho crawler TopCV |
| **Sử dụng trong** | `src/crawl_layer/crawler/topcv/` |

**Mục đích:** `aiohttp` cung cấp tầng truyền tải HTTP bất đồng bộ cho crawler TopCV. Nó xử lý các HTTP request đồng thời đến các trang kết quả tìm kiếm và trang chi tiết tuyển dụng của TopCV, cho phép thu thập dữ liệu thông lượng cao mà không làm chặn event loop.

**Lý do lựa chọn:**
- TopCV không sử dụng bảo vệ anti-bot mạnh (không có Cloudflare), nên HTTP client nhẹ là đủ — không cần tự động hóa trình duyệt.
- Hỗ trợ `async/await`原生 của `aiohttp` tích hợp tự nhiên với asyncio event loop của Python, cho phép crawler lấy nhiều trang đồng thời với điều tiết dựa trên semaphore.
- Tiêu thụ tài nguyên thấp hơn so với thu thập dựa trên trình duyệt (không có chi phí chạy Chrome).

---

### 2.2 curl_cffi

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 0.15.0 |
| **Vai trò** | HTTP client với khả năng mạo danh trình duyệt để vượt qua anti-bot |
| **Sử dụng trong** | `src/crawl_layer/crawler/topcv/http_client.py` |

**Mục đích:** `curl_cffi` bao bọc libcurl với khả năng mạo danh TLS fingerprint, cho phép crawler TopCV tạo HTTP request trông giống như từ trình duyệt thật. Nó xử lý giới hạn tốc độ, exponential backoff, và logic thử lại cho phản hồi HTTP 429/5xx.

**Lý do lựa chọn:**
- TopCV sử dụng phát hiện anti-bot cơ bản kiểm tra TLS fingerprint. `curl_cffi` có thể mạo danh TLS handshake của trình duyệt (ví dụ: Chrome, Firefox) mà không cần chi phí khởi chạy trình duyệt thật.
- Cung cấp API `AsyncSession` tương thích với `asyncio`, phù hợp liền mạch với kiến trúc crawl hiện có.
- Nhanh hơn đáng kể và ít tốn tài nguyên hơn so với giải pháp headless browser trong khi đạt hiệu quả vượt qua tương đương cho các trang có mức bảo vệ trung bình.

---

### 2.3 nodriver

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 0.48.1 |
| **Vai trò** | Tự động hóa Chrome headless cho crawler ITviec và VietnamWorks |
| **Sử dụng trong** | `src/crawl_layer/crawler/itviec/browser.py`, `src/crawl_layer/crawler/vietnamworks/browser.py` |

**Mục đích:** `nodriver` (thay thế cho `undetected-chromedriver`) cung cấp tự động hóa trình duyệt ẩn danh cho các trang web yêu cầu render JavaScript và có bảo vệ anti-bot mạnh. ITviec sử dụng bảo vệ Cloudflare và yêu cầu đăng nhập, trong khi VietnamWorks yêu cầu nội dung được render bằng JavaScript.

**Lý do lựa chọn:**
- ITviec sử dụng bảo vệ Cloudflare Turnstile không thể vượt qua bằng HTTP request đơn giản. Cần phiên trình duyệt thật để vượt qua thử thách Cloudflare.
- VietnamWorks render danh sách tuyển dụng động qua JavaScript, khiến việc thu thập bằng HTTP tĩnh là không đủ.
- `nodriver` vá các cờ tự động hóa của Chrome tại thời điểm chạy, làm cho phiên trình duyệt không thể phân biệt với người dùng thật — không giống như Selenium hay Playwright dễ bị phát hiện.
- Hỗ trợ chế độ headless với `xvfb-run` trên máy chủ Linux, cho phép triển khai trong container Docker mà không cần màn hình vật lý.

---

### 2.4 parsel

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 1.11.0 |
| **Vai trò** | Phân tích HTML với CSS selector và XPath |
| **Sử dụng trong** | `src/crawl_layer/crawler/itviec/parser.py`, `src/crawl_layer/crawler/vietnamworks/parser.py` |

**Mục đích:** `parsel` cung cấp giao diện phân tích HTML tương thích với Scrapy để trích xuất dữ liệu có cấu trúc từ HTML thô. Nó hỗ trợ cả CSS selector và biểu thức XPath để nhắm mục tiêu phần tử chính xác.

**Lý do lựa chọn:**
- `parsel` là parser tiêu chuẩn thực tế trong hệ sinh thái Scrapy, cung cấp API quen thuộc và tài liệu tốt cho web scraping.
- Hỗ trợ dual selector (CSS + XPath) mang lại sự linh hoạt — CSS cho lựa chọn đơn giản, XPath cho duyệt phức tạp như "next sibling text node."
- Được xây dựng trên `lxml`, cung cấp hiệu suất phân tích nhanh được tăng tốc bằng C.

---

### 2.5 lxml

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 6.1.0 |
| **Vai trò** | Engine phân tích XML/HTML hiệu năng cao |
| **Sử dụng trong** | Dependency gián tiếp qua `parsel` |

**Mục đích:** `lxml` đóng vai trò là engine phân tích HTML/XML được tăng tốc bằng C nằm dưới `parsel`'s selector engine. Nó cung cấp khả năng duyệt cây nhanh và chọn phần tử cần thiết để phân tích các trang web đã thu thập.

**Lý do lựa chọn:**
- Triển khai dựa trên C cung cấp hiệu suất phân tích nhanh hơn một bậc so với các thay thế pure-Python như `html.parser`.
- Xử lý mạnh mẽ HTML không đúng định dạng thường gặp trên các trang web thực tế.
- Là dependency bắt buộc của `parsel` — không cần nỗ lực tích hợp thêm.

---

## 3. Công nghệ Lưu trữ Dữ liệu

> Các công nghệ lưu trữ dữ liệu trong Lakehouse-Lite triển khai Kiến trúc Medallion: S3 cho tầng Bronze và Silver (data lake), DuckDB/MotherDuck cho tầng Gold (kho dữ liệu phân tích), Supabase cho phục vụ frontend, và Qdrant cho tìm kiếm vector ngữ nghĩa.

### 3.1 Amazon S3 (qua boto3)

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Thư viện** | boto3 1.43.6 |
| **Vai trò** | Lưu trữ đối tượng đám mây cho tầng dữ liệu Bronze và Silver |
| **Sử dụng trong** | `src/storage_layer/MinIO_S3/` |

**Mục đích:** Amazon S3 đóng vai trò là lưu trữ data lake trung tâm cho Kiến trúc Medallion. Tầng Bronze lưu trữ các file JSONL thô đã nén gzip, và tầng Silver lưu trữ các file Parquet đã làm sạch — cả hai được tổ chức với phân vùng kiểu Hive (`source_site=/year=/month=/day=`).

**Lý do lựa chọn:**
- **Khả năng mở rộng:** S3 cung cấp lưu trữ hầu như không giới hạn mà không cần lập kế hoạch dung lượng, điều cần thiết cho data lake liên tục thu thập dữ liệu tuyển dụng.
- **Hiệu quả chi phí:** Giá lưu trữ S3 Standard rẻ hơn đáng kể so với lưu trữ cơ sở dữ liệu, phù hợp cho dữ liệu thô và bán xử lý.
- **Tương thích với Hive partitioning:** Tổ chức dựa trên đường dẫn của S3 hoạt động tự nhiên với Hive partitioning reader của DuckDB, cho phép tầng Gold đọc file Silver Parquet trực tiếp từ S3 mà không cần tải xuống trung gian.
- **Độ bền:** S3 cung cấp độ bền 99.999999999% (11 số 9), đảm bảo không mất dữ liệu.
- **Tiêu chuẩn ngành:** S3 là tiêu chuẩn thực tế cho lưu trữ data lake, đảm bảo tính di động và quen thuộc cho người bảo trì tương lai.

---

### 3.2 DuckDB

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 1.5.3 |
| **Vai trò** | Engine cơ sở dữ liệu phân tích trong tiến trình cho tầng Gold |
| **Sử dụng trong** | `src/storage_layer/MotherDuck/client.py`, thực thi SQL Gold |

**Mục đích:** DuckDB đóng vai trò là engine truy vấn phân tích cho tầng Gold. Nó đọc file Silver Parquet trực tiếp từ S3 qua Hive partitioning, thực thi các biến đổi SQL để xây dựng bảng star-schema (fact jobs, dimension date, bridge table cho industries/benefits/requirements), và lưu trữ kết quả trong MotherDuck Cloud.

**Lý do lựa chọn:**
- **Truy cập S3 trực tiếp:** DuckDB có thể truy vấn file Parquet trên S3原生 sử dụng `read_parquet()` với Hive partitioning, loại bỏ cần bước tải xuống dữ liệu trung gian.
- **Kiến trúc nhúng:** DuckDB chạy trong tiến trình Python (không cần máy chủ riêng), giảm độ phức tạp vận hành và độ trễ.
- **Phân tích cột:** Engine thực thi vectorized của DuckDB được tối ưu cho workload phân tích (OLAP), phù hợp cho các truy vấn tổng hợp và biến đổi ở tầng Gold.
- **Tương thích SQL:** Hỗ trợ SQL đầy đủ cho phép diễn đạt các biến đổi tầng Gold phức tạp (deduplication, unnesting, xây dựng star-schema) theo cách khai báo thay vì mệnh lệnh.

---

### 3.3 MotherDuck

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Vai trò** | Dịch vụ DuckDB lưu trữ trên đám mây cho tầng Gold |
| **Sử dụng trong** | `src/storage_layer/MotherDuck/` |

**Mục đích:** MotherDuck cung cấp phiên bản DuckDB lưu trữ trên đám mây để duy trì dữ liệu tầng Gold. Pipeline kết nối qua token xác thực, thiết lập thông tin xác thực S3 làm persistent secret, và thực thi SQL để xây dựng bảng Gold. Điều này làm cho tầng Gold có thể truy cập được cho công cụ BI (Power BI) và phân tích ad-hoc.

**Lý do lựa chọn:**
- **Không cần quản lý hạ tầng:** MotherDuck loại bỏ cần cung cấp, duy trì, và mở rộng máy chủ cơ sở dữ liệu cho tầng Gold.
- **Tương thích DuckDB原生:** Vì MotherDuck được xây dựng trên DuckDB, tất cả SQL và tính năng hoạt động giống hệt — không có khác biệt dialect hay giới hạn tính năng.
- **Quản lý thông tin xác thực an toàn:** Tính năng persistent secret của MotherDuck lưu trữ thông tin xác thực AWS một cách an toàn trên đám mây, nên cấu hình truy cập S3 chỉ cần chạy một lần.
- **Kết nối công cụ BI:** MotherDuck hỗ trợ các connector cơ sở dữ liệu tiêu chuẩn (ODBC, JDBC), cho phép kết nối trực tiếp từ Power BI và các công cụ BI khác.

---

### 3.4 Supabase (PostgreSQL)

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Thư viện** | psycopg2-binary 2.9.10 |
| **Vai trò** | Cơ sở dữ liệu PostgreSQL Backend-as-a-Service cho phục vụ frontend |
| **Sử dụng trong** | `src/storage_layer/Supabase/` |

**Mục đích:** Supabase lưu trữ cơ sở dữ liệu PostgreSQL đóng vai trò là kho dữ liệu hướng ứng dụng. Pipeline tải dữ liệu Silver đã làm sạch vào bảng `ready_jobs` của Supabase sử dụng thao tác UPSERT (giải quyết xung đột trên `job_url`), cung cấp cho frontend quyền truy cập thời gian thực vào dữ liệu tuyển dụng mới nhất.

**Lý do lựa chọn:**
- **REST API tức thì:** Supabase tự động tạo REST API từ schema PostgreSQL, cho phép frontend truy vấn dữ liệu tuyển dụng mà không cần viết mã backend.
- **Subscription thời gian thực:** Engine thời gian thực của Supabase có thể đẩy cập nhật đến frontend khi tuyển dụng mới được tải, cho phép cập nhật bảng điều khiển trực tiếp.
- **Row Level Security:** Chính sách RLS của PostgreSQL cho phép kiểm soát truy cập chi tiết, bảo vệ dữ liệu nhạy cảm trong khi cho phép quyền đọc công khai cho danh sách tuyển dụng.
- **Hỗ trợ UPSERT:** Mẫu `ON CONFLICT ... DO UPDATE` đảm bảo tải idempotent — chạy lại pipeline cập nhật tuyển dụng hiện tại thay vì tạo bản trùng lặp.
- **Dịch vụ được quản lý:** Supabase xử lý sao lưu, nhân bản, và mở rộng, giảm gánh nặng vận hành cho đồ án tốt nghiệp.

---

### 3.5 Qdrant

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | Cloud free tier |
| **Vai trò** | Vector database cho tìm kiếm ngữ nghĩa việc làm |
| **Sử dụng trong** | `src/storage_layer/Qdrant/` (indexing), `backend/app/services/recommender.py` (query) |

**Mục đích:** Qdrant lưu trữ vector embedding của tin tuyển dụng (768 chiều, cosine similarity) cùng payload metadata (job_url, title, company, location, salary, skills, v.v.). Khi người dùng upload CV, hệ thống embed CV thành vector query, tìm kiếm top-N việc làm tương tự nhất qua Qdrant, sau đó re-rank kết hợp với skill overlap taxonomy.

**Lý do lựa chọn:**
- **Managed cloud free tier:** Không cần tự host, phù hợp quy mô hàng nghìn job của đồ án tốt nghiệp.
- **Hỗ trợ filter + vector search:** Cho phép kết hợp filter cứng (location, experience, deadline) với tìm kiếm vector, đảm bảo kết quả tuân thủ ràng buộc nghiệp vụ.
- **Payload index:** Tạo index trên `clean_location`, `min_exp_level`, `deadline_ts` để filter nhanh mà không cần quét toàn bộ collection.
- **Idempotent upsert:** Sử dụng UUID5 dựa trên `job_url` làm point ID, cho phép chạy lại index mà không tạo bản ghi trùng lặp.

---

## 4. Công nghệ Xử lý Dữ liệu

> Các công nghệ xử lý dữ liệu bao gồm làm sạch và biến đổi (Polars), trích xuất nhãn taxonomy (FlashText), khớp chuỗi mờ (RapidFuzz), xác minh chất lượng (Great Expectations), quản lý taxonomy seed (gspread), embedding ngữ nghĩa (OpenRouter Embedding), và trích xuất text từ CV (pdfplumber, python-docx).

### 4.1 Polars

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 1.40.1 |
| **Vai trò** | Thư viện DataFrame cho việc làm sạch và biến đổi dữ liệu tầng Silver |
| **Sử dụng trong** | Pipeline làm sạch Silver, Supabase loader, Bronze/Silver reader, tạo dashboard |

**Mục đích:** Polars là engine xử lý dữ liệu chính cho tầng Silver. Nó đọc dữ liệu Bronze JSONL từ S3, áp dụng chuỗi biến đổi làm sạch (chuẩn hóa tiêu đề tuyển dụng, chuẩn hóa địa điểm, trích xuất kỹ năng qua khớp taxonomy, v.v.), và ghi file Parquet đã làm sạch trở lại S3. Nó cũng hỗ trợ Supabase loader và dashboard giám sát.

**Lý do lựa chọn:**
- **Hiệu suất:** Polars sử dụng mô hình bộ nhớ dựa trên Apache Arrow với lazy evaluation và thực thi đa luồng, cung cấp xử lý nhanh gấp 5-10 lần so với pandas cho workload ETL điển hình.
- **Lazy evaluation:** API `LazyFrame` cho phép Polars tối ưu toàn bộ kế hoạch truy vấn trước khi thực thi, tránh materialization không cần thiết của kết quả trung gian — điều quan trọng khi xử lý tập dữ liệu lớn từ S3.
- **Hỗ trợ S3原生:** Polars có thể đọc file Parquet trực tiếp từ S3 sử dụng `scan_parquet()` với storage options, loại bỏ cần tải xuống thủ công qua boto3.
- **An toàn kiểu:** Hệ thống schema nghiêm ngặt của Polars phát hiện vấn đề chất lượng dữ liệu tại thời điểm chạy pipeline thay vì làm hỏng dữ liệu âm thầm.
- **Hiệu quả bộ nhớ:** Định dạng cột và thao tác zero-copy giảm dấu chân bộ nhớ, cho phép xử lý tập dữ liệu lớn hơn RAM.

---

### 4.2 FlashText

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 2.7 |
| **Vai trò** | Trích xuất từ khóa nhanh cho khớp nhãn dựa trên taxonomy |
| **Sử dụng trong** | `src/storage_layer/MinIO_S3/layer/silver/utils/flashtext_extractor.py` |

**Mục đích:** FlashText hỗ trợ `HybridKeywordExtractor` trong pipeline làm sạch Silver. Nó trích xuất các nhãn taxonomy (kỹ năng, ngành nghề, quyền lợi, v.v.) từ mô tả tuyển dụng và yêu cầu bằng cách khớp từ khóa theo nghĩa đen trong thời gian O(n) mỗi lần quét, trong đó n là độ dài tài liệu.

**Lý do lựa chọn:**
- **Độ phức tạp O(n):** FlashText quét văn bản trong thời gian tuyến tính bất kể số lượng từ khóa, so với cách tiếp cận dựa trên regex có độ phức tạp O(n × m) trong đó m là số mẫu.
- **Cách tiếp cận hybrid:** Hệ thống sử dụng chiến lược hybrid — FlashText xử lý khớp từ khóa theo nghĩa đen trong khi regex fallback bắt các khớp dựa trên mẫu (ví dụ: `\bpy3?\b`), đạt được độ bao phủ không mất mát so với regex thuần trong khi nhanh hơn đáng kể.
- **Dựa trên taxonomy:** Các file CSV taxonomy seed chứa hàng trăm mục từ khóa. FlashText làm cho việc khớp tất cả chúng với mỗi mô tả tuyển dụng khả thi mà không suy giảm hiệu suất.

---

### 4.3 RapidFuzz

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 3.10.1 |
| **Vai trò** | Khớp chuỗi mờ cho chuẩn hóa tên công ty |
| **Sử dụng trong** | `src/storage_layer/MinIO_S3/layer/silver/utils/script_fuzzy.py`, làm sạch tên công ty |

**Mục đích:** RapidFuzz cung cấp khớp chuỗi mờ để chuẩn hóa tên công ty qua các nguồn khác nhau. Ví dụ, "FPT Corporation" và "FPT Corp" nên được ánh xạ đến cùng một tên chuẩn. Nó hỗ trợ pipeline phân cụm và ánh xạ tên công ty.

**Lý do lựa chọn:**
- **Tăng tốc bằng C:** RapidFuzz là fork được tăng tốc C++ của `fuzzywuzzy`, cung cấp tính toán tương tự chuỗi nhanh hơn 10-100 lần.
- **Nhiều thuật toán:** Hỗ trợ khoảng cách Levenshtein, Jaro-Winkler, và các metric tương tự khác, cho phép chọn thuật toán tốt nhất cho khớp tên công ty.
- **Trích xuất xử lý:** `extractBests()` tìm hiệu quả các khớp gần nhất từ danh sách tham chiếu mà không cần tính toán tất cả khoảng cách theo cặp.

---

### 4.4 Great Expectations

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 1.17.2 |
| **Vai trò** | Framework kiểm tra chất lượng dữ liệu cho tầng Bronze |
| **Sử dụng trong** | `src/storage_layer/MinIO_S3/layer/local_temp/validation/` |

**Mục đích:** Great Expectations (GX) xác minh chất lượng dữ liệu đã thu thập trước khi nó vào tầng Bronze. Nó định nghĩa các expectation suite (ví dụ: `topcv_suite`) kiểm tra tính đầy đủ cột, phạm vi giá trị, và ràng buộc định dạng. Kết quả xác minh quyết định liệu dữ liệu có phù hợp để nhập vào Bronze hay không.

**Lý do lựa chọn:**
- **Xác minh khai báo:** GX cho phép định nghĩa quy tắc chất lượng dữ liệu dưới dạng expectation suite, tách biệt logic xác minh khỏi mã pipeline.
- **Khả năng kiểm tra:** GX tạo kết quả xác minh với thống kê chi tiết, cung cấp dấu vết kiểm toán cho quyết định chất lượng dữ liệu.
- **Tích hợp:** Hoạt động liền mạch với pandas DataFrame, phù hợp tự nhiên với pipeline xác minh hiện có.
- **Expectation có điều kiện:** Hỗ trợ điều kiện cấp hàng (ví dụ: "company_size chỉ có thể null khi job_url chứa 'brand/'"), cho phép quy tắc xác minh tinh tế phản ánh mẫu dữ liệu thực tế.

---

### 4.5 gspread + Seed Taxonomy (Google Sheets)

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 6.2.1 |
| **Vai trò** | Google Sheets API client cho quản lý taxonomy seed |
| **Sử dụng trong** | `src/storage_layer/MinIO_S3/layer/silver/utils/google_sheets.py`, `config_loader.py`, `sync_local_csv_to_google_sheets.py` |

**Mục đích:** `gspread` cho phép pipeline làm sạch Silver đọc dữ liệu taxonomy seed trực tiếp từ Google Sheets. Hệ thống lưu trữ 13 file CSV taxonomy seed tại `src/storage_layer/MinIO_S3/layer/silver/seeds/`, đồng thời đồng bộ lên Google Sheets để chỉnh sửa cộng tác. Khi pipeline chạy, hàm `read_seeds()` trong `config_loader.py` ưu tiên đọc từ Google Sheets trước; nếu không khả dụng (thiếu thông tin xác thực hoặc lỗi mạng), hệ thống tự động dự phòng về file CSV cục bộ.

**Danh sách file Seed Taxonomy:**

| File CSV | Mục đích | Sử dụng trong |
|----------|----------|---------------|
| `benefit_taxonomy.csv` | Phân loại quyền lợi tuyển dụng (lương tháng 13, bảo hiểm, v.v.) | `clean_benefit.py` |
| `program_lang_taxonomy.csv` | Phân loại ngôn ngữ lập trình (Python, Java, Go, v.v.) | `clean_requirement.py` |
| `framework_taxonomy.csv` | Phân loại framework/thư viện (React, Django, Spring, v.v.) | `clean_requirement.py` |
| `tools_taxonomy.csv` | Phân loại công cụ phần mềm (Docker, Git, Kubernetes, v.v.) | `clean_requirement.py` |
| `cloud_skill_taxonomy.csv` | Phân loại kỹ năng đám mây (AWS, Azure, GCP, v.v.) | `clean_requirement.py` |
| `knowledge_taxonomy.csv` | Phân loại kiến thức chuyên môn (Machine Learning, DevOps, v.v.) | `clean_requirement.py` |
| `domain_taxonomy.csv` | Phân loại lĩnh vực chuyên môn (Backend, Frontend, Data, v.v.) | `clean_requirement.py` |
| `language_taxonomy.csv` | Phân loại ngôn ngữ tự nhiên (Tiếng Anh, Tiếng Việt, v.v.) | `clean_requirement.py` |
| `domain_university_taxonomy.csv` | Phân loại lĩnh vực đại học | `clean_requirement.py` |
| `industry_taxonomy.csv` | Phân loại ngành nghề (IT, Tài chính, Sản xuất, v.v.) | `clean_job_industry.py` |
| `location_mapping.csv` | Ánh xạ địa điểm tuyển dụng chuẩn hóa (TP. HCM, Hà Nội, v.v.) | `clean_location.py` |
| `company_mapping.csv` | Ánh xạ tên công ty chuẩn hóa (FPT, VNG, v.v.) | `map_company_name.py` |
| `tinh_thanh.csv` | Bảng tham chiếu mã/tên tỉnh thành Việt Nam | `clean_location.py` |

**Cấu trúc file Seed:** Mỗi file CSV có các cột `canonical_vi`, `canonical_en`, `parent_vi`, `parent_en`, `keywords`. Cột `keywords` chứa các mẫu regex phân cách bằng dấu `|` (ví dụ: `python|\bpy3?\b|python3`), được `HybridKeywordExtractor` (FlashText + regex) sử dụng để trích xuất nhãn từ mô tả tuyển dụng.

**Cơ chế đọc Seed (`config_loader.py`):**
1. Kiểm tra cấu hình Google Sheets (`GOOGLE_SHEETS_CREDENTIALS_FILE` + `GOOGLE_SHEETS_SPREADSHEET_ID` trong `.env`).
2. Nếu khả dụng: đọc worksheet tương ứng từ Google Sheets qua `gspread`, chuyển thành Polars DataFrame.
3. Nếu không khả dụng: dự phòng về file CSV cục bộ trong thư mục `seeds/`.

**Cơ chế đồng bộ (`sync_local_csv_to_google_sheets.py`):**
- Script đồng bộ một chiều từ file CSV cục bộ lên Google Sheets (tạo hoặc thay thế worksheet).
- Mỗi file CSV tương ứng với một worksheet (tên worksheet = tên file không có đuôi `.csv`).
- Chạy lệnh: `python -m src.storage_layer.MinIO_S3.layer.silver.scripts.sync_local_csv_to_google_sheets`

**Lý do lựa chọn:**
- **Chỉnh sửa cộng tác:** Chuyên gia miền có thể cập nhật mục taxonomy trong Google Sheets mà không cần chạm vào mã hay file CSV, giảm rào cản duy trì quy tắc chất lượng dữ liệu.
- **Cơ chế dự phòng:** Hệ thống dự phòng về file CSV cục bộ khi thông tin xác thực Google Sheets không khả dụng, đảm bảo pipeline không bao giờ bị chặn bởi lỗi dịch vụ bên ngoài.
- **Xác thực Service Account:** Sử dụng Google Service Account để truy cập tự động an toàn mà không cần đăng nhập tương tác.
- **Kiểm soát phiên bản:** File CSV cục bộ được lưu trong kho mã, cho phép theo dõi thay đổi taxonomy qua Git trong khi Google Sheets cung cấp giao diện chỉnh sửa thân thiện.

---

### 4.6 OpenRouter Embedding

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Model** | `openai/text-embedding-3-small` |
| **Dimension** | 768 (`dimensions=768`, L2-normalized) |
| **Vai trò** | Mô hình embedding đa ngôn ngữ cho CV và tin tuyển dụng |
| **Sử dụng trong** | `src/storage_layer/Qdrant/scripts/embed.py` (job), `backend/app/services/embedder.py` (CV) |

**Mục đích:** OpenRouter Embedding tạo vector biểu diễn ngữ nghĩa cho văn bản tuyển dụng và CV qua một API thống nhất. Job text được embed với `input_type=search_document`, CV text với `input_type=search_query`. Vector 768 chiều được L2-normalize để đảm bảo cosine similarity chính xác.

**Lý do lựa chọn:**
- **Đa ngôn ngữ:** Hỗ trợ tốt cả tiếng Anh và tiếng Việt — phù hợp thị trường tuyển dụng Việt Nam.
- **Free tier:** API miễn phí với giới hạn request đủ cho quy mô đồ án (hàng nghìn job/ngày).
- **Dimension cố định:** Dùng `dimensions=768` để giảm chi phí lưu trữ trên Qdrant và giữ contract đồng nhất giữa index/query.
- **Hai input type:** Phân biệt `search_document` (cho job) và `search_query` (cho CV) khi model/provider hỗ trợ tối ưu hóa retrieval.

---

### 4.7 pdfplumber

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Vai trò** | Trích xuất text từ file PDF (CV) |
| **Sử dụng trong** | `backend/app/services/cv_parser.py` |

**Mục đích:** `pdfplumber` đọc nội dung text từ file PDF upload bởi người dùng. Nó trích xuất từng trang, giữ cấu trúc dòng, và trả về text thô để đưa vào pipeline làm sạch và trích xuất kỹ năng.

**Lý do lựa chọn:**
- **Chất lượng trích xuất:** pdfplumber xử lý tốt layout phức tạp (bảng, đa cột) phổ biến trong CV định dạng PDF.
- **Kiểm soát trang:** Cho phép đếm số trang để áp dụng giới hạn <= 5 trang theo quyết định C3.
- **Không cần OCR:** Phù hợp với quyết định C1 — chỉ xử lý PDF có text layer, không hỗ trợ PDF scan/ảnh.

---

### 4.8 python-docx

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Vai trò** | Trích xuất text từ file DOCX (CV) |
| **Sử dụng trong** | `backend/app/services/cv_parser.py` |

**Mục đích:** `python-docx` đọc nội dung text từ file DOCX upload bởi người dùng. Nó trích xuất văn bản từ các paragraph và bảng trong tài liệu Word, trả về text thô cho pipeline xử lý tiếp theo.

**Lý do lựa chọn:**
- **Hỗ trợ định dạng phổ biến:** DOCX là định dạng CV phổ biến thứ hai sau PDF, đảm bảo trải nghiệm upload linh hoạt cho người dùng.
- **Nhẹ và ổn định:** Thư viện thuần Python, không cần cài đặt hệ điều hành phụ thuộc.
- **Cùng interface:** Module `cv_parser.py` sử dụng pdfplumber cho PDF và python-docx cho DOCX với cùng interface trả về text thô, đơn giản hóa logic downstream.

---

## 5. Công nghệ Phục vụ & Báo cáo Dữ liệu

> Tầng phục vụ và báo cáo dữ liệu cung cấp khả năng truy vấn và trực quan hóa cho người dùng cuối. Supabase phục vụ dữ liệu cho frontend (đã mô tả ở mục 3.4), trong khi Power BI cung cấp báo cáo trí tuệ doanh nghiệp nâng cao.

### 5.1 Microsoft Power BI

| Thuộc tính | Chi tiết |
|-------------|----------|
| **File** | `src/bi_report_layer/analysis_dashboard.pbix` |
| **Vai trò** | Báo cáo trí tuệ doanh nghiệp và trực quan hóa dữ liệu tương tác |
| **Nguồn dữ liệu** | MotherDuck (tầng Gold) qua ODBC connector |

**Mục đích:** Power BI Desktop đóng vai trò là tầng báo cáo trí tuệ doanh nghiệp của hệ thống Lakehouse-Lite. Nó kết nối trực tiếp đến tầng Gold lưu trữ trên MotherDuck Cloud, nơi hiển thị mô hình dữ liệu star-schema bao gồm:

- **Bảng fact:** `gold.jobs` — chứa danh sách tuyển dụng đã làm sạch với các khóa (job_url, source_site, date_key) và các phép đo (mức lương, cấp kinh nghiệm).
- **Bảng dimension:** `gold.dim_date` — dimension ngày liên tục cho phép tính toán thời gian (tăng trưởng YoY, trung bình cuộn).
- **Bridge/child table:** `gold.job_industries`, `gold.job_benefits`, `gold.job_requirements` — các bảng chuẩn hóa unnest các trường `List[str]` của tầng Silver thành quan hệ many-to-one thân thiện với BI.

File `analysis_dashboard.pbix` cung cấp bảng điều khiển tương tác để phân tích xu hướng thị trường việc làm, bao gồm:

- Xu hướng khối lượng tuyển dụng theo thời gian (theo trang nguồn, ngành nghề, địa điểm).
- Phân tích phân phối lương qua các cấp kinh nghiệm và ngành nghề.
- Xếp hạng nhu cầu kỹ năng (ngôn ngữ lập trình, framework, công cụ, kỹ năng đám mây).
- Hoạt động tuyển dụng và phân phối quy mô công ty.
- Phân phối địa lý cơ hội việc làm trên Việt Nam.

**Lý do lựa chọn:**

- **Tối ưu hóa star-schema:** Tầng Gold được thiết kế đặc biệt dưới dạng star schema (bảng fact + bảng dimension + bridge table) để khớp với mẫu mô hình dữ liệu ưa thích của Power BI. Điều này cho phép tính toán DAX hiệu quả và quan hệ drill-down tự nhiên.
- **DirectQuery đến MotherDuck:** Power BI kết nối đến MotherDuck Cloud qua DuckDB ODBC connector, cho phép truy vấn thời gian thực đối với tầng Gold mà không cần nhập dữ liệu cục bộ. Điều này đảm bảo bảng điều khiển luôn phản ánh kết quả pipeline mới nhất.
- **Công cụ BI tiêu chuẩn ngành:** Power BI là công cụ BI được áp dụng rộng rãi nhất trong môi trường doanh nghiệp, làm cho dự án có thể chuyển giao ngay lập tức cho các nhóm phân tích thực tế.
- **Thư viện trực quan phong phú:** Power BI cung cấp bộ trực quan tích hợp toàn diện (bản đồ, cây phân tích, yếu tố ảnh hưởng chính) vượt xa những gì Grafana hay Altair có thể cung cấp cho báo cáo doanh nghiệp.
- **DAX và M query:** Ngôn ngữ Data Analysis Expressions (DAX) và Power Query (M) của Power BI cho phép các phép đo tính toán phức tạp và biến đổi dữ liệu khó diễn đạt bằng SQL đơn thuần.
- **Không cần hạ tầng thêm:** Là ứng dụng desktop, Power BI không cần triển khai máy chủ — file `.pbix` có thể được chia sẻ trực tiếp hoặc xuất bản lên Power BI Service để truy cập toàn nhóm.

**Luồng kết nối mô hình dữ liệu:**

```
Power BI Desktop (.pbix)
    |
    |  DirectQuery / ODBC
    v
MotherDuck Cloud (DuckDB)
    |
    |  SQL: read_parquet('s3://...silver.../*.parquet', hive_partitioning=true)
    v
Amazon S3 — Silver Parquet files
    |
    v
Gold Layer Tables:
  +-- gold.jobs              (bảng fact)
  +-- gold.dim_date          (dimension ngày)
  +-- gold.job_industries    (bridge table)
  +-- gold.job_benefits      (bridge table)
  +-- gold.job_requirements  (bridge table)
```

---

## 6. Công nghệ Điều phối Pipeline

### 6.1 Apache Airflow

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 2.10.3 |
| **Executor** | LocalExecutor |
| **Vai trò** | Điều phối pipeline, lập lịch, và quản lý phụ thuộc |
| **Sử dụng trong** | `src/orchestration_layer/` |

**Mục đích:** Apache Airflow điều phối toàn bộ data pipeline — từ kích hoạt crawl, qua tải lên Bronze, làm sạch Silver, tải Gold, đến đồng bộ Supabase. Mỗi giai đoạn pipeline được định nghĩa là DAG (Directed Acyclic Graph) với phụ thuộc task rõ ràng. Bộ lập lịch Airflow thực thi task theo lịch cron, và giao diện web cung cấp khả năng hiển thị trạng thái pipeline.

**Lý do lựa chọn:**
- **Điều phối dựa trên DAG:** Mô hình DAG của Airflow thể hiện tự nhiên các phụ thuộc tuần tự của Kiến trúc Medallion (crawl → validate → bronze → silver → gold → supabase).
- **Cách ly DockerOperator:** Mỗi task chạy trong container `lakehouse-crawler` Docker qua `DockerOperator`, đảm bảo cách ly phụ thuộc hoàn toàn — Airflow không bao giờ import module crawler/storage trực tiếp.
- **DAG tham số hóa:** Mẫu DAG factory với `SITE_CONFIGS` cho phép thêm nguồn dữ liệu mới bằng cách mở rộng dictionary cấu hình, không cần viết DAG mới.
- **Linh hoạt lập lịch:** Lịch crawl khác nhau cho mỗi trang (TopCV mỗi 3 giờ, ITviec lệch 15 phút, VietnamWorks lệch 30 phút) ngăn thu thập đồng thời có thể kích hoạt giới hạn tốc độ.
- **Thử lại và cảnh báo:** Logic thử lại tích hợp với backoff có thể cấu hình, cộng với phát chỉ số StatsD để tích hợp giám sát.
- **Tiêu chuẩn ngành:** Airflow là công cụ điều phối workflow được áp dụng rộng rãi nhất trong kỹ thuật dữ liệu, làm cho dự án dễ hiểu ngay lập tức cho người thực hành.

---

### 6.2 PostgreSQL (Airflow Metadata)

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 16 |
| **Vai trò** | Cơ sở dữ liệu metadata của Airflow |
| **Sử dụng trong** | `src/orchestration_layer/docker-compose.yaml` |

**Mục đích:** PostgreSQL lưu trữ metadata của Airflow — trạng thái chạy DAG, task instance, biến, kết nối, và log. Nó là xương sống của LocalExecutor Airflow, cho phép thực thi task đồng thời và duy trì trạng thái qua các lần khởi động lại.

**Lý do lựa chọn:**
- **Yêu cầu Airflow:** PostgreSQL là backend cơ sở dữ liệu được khuyến nghị và kiểm tra kỹ nhất cho Airflow, cung cấp hiệu suất và hỗ trợ tính năng tốt hơn SQLite hay MySQL.
- **Cấp sản xuất:** Tuân thủ ACID của PostgreSQL đảm bảo không mất metadata, ngay cả khi bộ lập lịch Airflow khởi động lại.
- **Triển khai Dockerized:** Chạy như container cùng Airflow, đơn giản hóa triển khai và đảm bảo nhất quán phiên bản.

---

### 6.3 StatsD Exporter

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Image** | prom/statsd-exporter:v0.28.0 |
| **Vai trò** | Cầu nối giữa chỉ số Airflow và Prometheus |
| **Sử dụng trong** | `src/orchestration_layer/docker-compose.yaml` |

**Mục đích:** StatsD Exporter nhận các chỉ số định dạng StatsD của Airflow (thời lượng task, số lượng thành công/thất bại, scheduler heartbeat) và chuyển đổi chúng thành chỉ số tương thích Prometheus có thể được thu thập và lưu trữ.

**Lý do lựa chọn:**
- **Tích hợp原生 Airflow:** Airflow phát chỉ số qua giao thức StatsDB原生 — không cần mã exporter tùy chỉnh.
- **Ánh xạ chỉ số:** File cấu hình `statsd_mapping.yml` ánh xạ tên chỉ số StatsD thành nhãn Prometheus, cho phép truy vấn và cảnh báo có ý nghĩa.
- **Sidecar nhẹ:** Chạy như container tối thiểu cùng Airflow với chi phí tài nguyên không đáng kể.

---

## 7. Công nghệ Giám sát

### 7.1 Prometheus

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | v3.12.0 |
| **Vai trò** | Thu thập và lưu trữ chỉ số time-series |
| **Sử dụng trong** | `src/monitoring_layer/` |

**Mục đích:** Prometheus thu thập và lưu trữ chỉ số time-series từ hai nguồn: (1) chỉ số pipeline Airflow qua StatsD Exporter, và (2) chỉ số hạ tầng Supabase qua Supabase Metrics API. Nó cung cấp ngôn ngữ truy vấn mạnh mẽ (PromQL) để phân tích sức khỏe pipeline, độ tươi dữ liệu, và hiệu suất hạ tầng.

**Lý do lựa chọn:**
- **Mô hình pull-based:** Prometheus thu thập các endpoint chỉ số theo khoảng có thể cấu hình, loại bỏ cần agent trên các dịch vụ được giám sát.
- **Tổng hợp đa nguồn:** Một phiên bản Prometheus duy nhất thu thập cả chỉ số vận hành Airflow và chỉ số hạ tầng Supabase, cung cấp chế độ xem quan sát thống nhất.
- **PromQL:** Cho phép truy vấn phức tạp như "thời gian crawl trung bình trong 7 ngày qua" hoặc "phần trăm task Silver thất bại" để xây dựng bảng điều khiển.
- **Tiêu chuẩn ngành:** Prometheus là tiêu chuẩn thực tế cho giám sát cloud-native, đảm bảo quen thuộc và hỗ trợ cộng đồng rộng rãi.

---

### 7.2 Grafana

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 11.6 |
| **Vai trò** | Trực quan hóa bảng điều khiển và cảnh báo |
| **Sử dụng trong** | `src/monitoring_layer/grafana/` |

**Mục đích:** Grafana cung cấp bảng điều khiển tương tác để trực quan hóa sức khỏe pipeline và chỉ số chất lượng dữ liệu. Các bảng điều khiển được cấu hình sẵn hiển thị tỷ lệ thành công/thất bại task Airflow, hiệu suất cơ sở dữ liệu Supabase, và xu hướng khối lượng dữ liệu. Nó kết nối đến Prometheus làm nguồn dữ liệu.

**Lý do lựa chọn:**
- **Trực quan phong phú:** Grafana cung cấp nhiều loại panel (time series, heatmap, bảng, gauge) phù hợp cho bảng điều khiển vận hành và doanh nghiệp.
- **Cấu hình được cung cấp:** Datasource và bảng điều khiển được cung cấp qua file YAML, đảm bảo triển khai có thể tái tạo mà không cần cấu hình UI thủ công.
- **Quyền xem ẩn danh:** Được cấu hình với quyền đọc ẩn danh, cho phép thành viên nhóm xem bảng điều khiển mà không cần tài khoản cá nhân.
- **Khả năng cảnh báo:** Có thể mở rộng với quy tắc cảnh báo để thông báo về lỗi pipeline hoặc suy giảm chất lượng dữ liệu.

---

### 7.3 Caddy

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Image** | caddy:alpine |
| **Vai trò** | Reverse proxy với xác thực và HTTPS tự động |
| **Sử dụng trong** | `src/monitoring_layer/Caddyfile` |

**Mục đích:** Caddy đóng vai trò là điểm vào duy nhất cho tầng giám sát, cung cấp: (1) xác thực HTTP basic để bảo vệ Grafana và bảng điều khiển doanh nghiệp, (2) định tuyến reverse proxy (`/grafana/` → Grafana, `/business/` → Nginx), và (3) HTTPS tự động với quản lý chứng chỉ.

**Lý do lựa chọn:**
- **HTTPS tự động:** Caddy tự động cấp và gia hạn chứng chỉ TLS qua Let's Encrypt, loại bỏ quản lý chứng chỉ thủ công.
- **Cấu hình đơn giản:** Cú pháp Caddyfile dễ đọc hơn đáng kể so với cấu hình Nginx, giảm độ phức tạp vận hành.
- **Tầng xác thực:** Chỉ thị `basic_auth` của Caddy bảo vệ tất cả endpoint giám sát với một cấu hình duy nhất, nên dịch vụ backend (Grafana, Prometheus) không cần hệ thống xác thực riêng.
- **Nhẹ:** Image Alpine chỉ khoảng 40MB, giảm thiểu tài nguyên container.

---

### 7.4 Altair

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 6.1.0 |
| **Vai trò** | Thư viện trực quan khai báo cho báo cáo HTML bảng điều khiển doanh nghiệp |
| **Sử dụng trong** | `src/monitoring_layer/business/bronze_dashboard.py`, `silver_dashboard.py` |

**Mục đích:** Altair tạo đặc tả biểu đồ Vega-Lite khai báo cho các bảng điều khiển giám sát doanh nghiệp. Nó đọc dữ liệu Bronze/Silver từ S3, tính toán thống kê tóm tắt (kích thước lưu trữ, số lượng bản ghi, phạm vi ngày), và render biểu đồ tương tác nhúng trong báo cáo HTML tĩnh phục vụ qua Nginx.

**Lý do lựa chọn:**
- **API khai báo:** Cách tiếp cận grammar of graphics của Altair tạo biểu đồ nhất quán, thiết kế tốt với mã tối thiểu.
- **Xuất HTML:** Biểu đồ được render dưới dạng JSON Vega-Lite nhúng trong HTML, xem được trong bất kỳ trình duyệt nào mà không cần máy chủ chạy.
- **Tích hợp Polars:** Hoạt động liền mạch với Polars DataFrame, tránh chi phí chuyển đổi pandas.

---

### 7.5 Nginx

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Image** | nginx:alpine |
| **Vai trò** | Máy chủ file tĩnh cho báo cáo HTML bảng điều khiển doanh nghiệp |
| **Sử dụng trong** | `src/monitoring_layer/` (dashboards container) |

**Mục đích:** Nginx phục vụ các báo cáo HTML bảng điều khiển tĩnh được tạo bởi script bảng điều khiển Bronze và Silver. Các báo cáo này cung cấp khả năng hiển thị nhanh về khối lượng dữ liệu, mức tiêu thụ lưu trữ, và phạm vi nguồn mà không cần Grafana.

**Lý do lựa chọn:**
- **Hiệu suất cao:** Nginx vượt trội trong việc phục vụ file tĩnh với mức sử dụng tài nguyên tối thiểu.
- **Container nhẹ:** Image Alpine dưới 10MB, lý tưởng cho máy chủ file tĩnh đơn giản.
- **Entrypoint tùy chỉnh:** Script entrypoint tạo động trang danh sách thư mục, cung cấp chỉ mục có thể duyệt của tất cả báo cáo đã tạo.

---

## 8. Công nghệ Hạ tầng & DevOps

### 8.1 Docker

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Vai trò** | Nền tảng container hóa để cách ly pipeline |
| **Sử dụng trong** | `Dockerfile`, tất cả file `docker-compose.yaml` |

**Mục đích:** Docker cung cấp container hóa cho toàn bộ hệ thống Lakehouse-Lite. Image `lakehouse-crawler` đóng gói tất cả dependency Python (bao gồm Chrome cho crawler dựa trên nodriver) vào môi trường runtime có thể tái tạo. `DockerOperator` của Airflow thực thi mỗi task pipeline trong container này, đảm bảo cách ly hoàn toàn giữa điều phối và logic doanh nghiệp.

**Lý do lựa chọn:**
- **Cách ly dependency:** Crawler yêu cầu Chrome, `nodriver`, `curl_cffi`, và nhiều gói Python. Docker đóng gói tất cả dependency, ngăn xung đột với môi trường Airflow.
- **Khả năng tái tạo:** Cùng image Docker chạy giống hệt trên máy phát triển và máy chủ sản xuất, loại bỏ vấn đề "chạy được trên máy tôi."
- **Bảo mật:** Airflow chạy mã pipeline trong container với quyền truy cập máy chủ hạn chế (chỉ bind-mounted volume), giảm bán kính ảnh hưởng của bất kỳ lỗi crawler nào.
- **Chrome trong Docker:** Dockerfile cài đặt Google Chrome và `xvfb` cho tự động hóa trình duyệt headless, một thiết lập phức tạp mà Docker làm cho có thể di chuyển và lặp lại.

---

### 8.2 Docker Compose

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Vai trò** | Điều phối multi-container cho phát triển và triển khai cục bộ |
| **Sử dụng trong** | `src/orchestration_layer/docker-compose.yaml`, `src/monitoring_layer/docker-compose.yaml` |

**Mục đích:** Docker Compose định nghĩa và quản lý kiến trúc multi-service: Airflow (webserver + scheduler + init), PostgreSQL, StatsD Exporter, Prometheus, Grafana, Caddy, và máy chủ bảng điều khiển Nginx. Script `init.sh` điều phối khởi động cả hai stack compose.

**Lý do lựa chọn:**
- **Triển khai một lệnh:** `docker compose up -d` khởi động toàn bộ hệ thống, làm cho triển khai dễ tiếp cận cho đồ án tốt nghiệp.
- **Quản lý phụ thuộc dịch vụ:** `depends_on` của Docker Compose với health check đảm bảo dịch vụ khởi động đúng thứ tự (PostgreSQL trước Airflow, Prometheus trước Grafana).
- **Quản lý volume:** Named volume duy trì dữ liệu qua các lần khởi động lại container (dữ liệu PostgreSQL, chỉ số Prometheus, bảng điều khiển Grafana).
- **Tiêm biến môi trường:** Tích hợp file `.env` giữ bí mật ngoài kiểm soát phiên bản trong khi cho phép cấu hình theo môi trường.

---

### 8.3 Python 3.11

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Vai trò** | Ngôn ngữ lập trình chính cho tất cả thành phần pipeline |
| **Sử dụng trong** | Toàn bộ thư mục `src/` |

**Mục đích:** Python 3.11 là môi trường runtime cho tất cả mã pipeline — crawler, xử lý dữ liệu, thao tác lưu trữ, và script giám sát. Dockerfile sử dụng `python:3.11-slim` làm image cơ sở.

**Lý do lựa chọn:**
- **Độ phong phú hệ sinh thái:** Hệ sinh thái dữ liệu Python (Polars, DuckDB, boto3, Great Expectations) cung cấp thư viện đã được kiểm tra cho mọi giai đoạn pipeline.
- **Hỗ trợ async:** `asyncio` của Python 3.11 với `aiohttp` và `nodriver` cho phép crawl đồng thời hiệu quả.
- **Cải thiện hiệu suất:** Tối ưu hóa CPython của Python 3.11 cung cấp tăng tốc 10-60% so với 3.10 miễn phí.
- **Type hints:** Hệ thống chú thích kiểu của Python hiện đại (được sử dụng rộng rãi trong định nghĩa `dataclass`) cải thiện khả năng đọc mã và cho phép phân tích tĩnh.

---

### 8.4 python-dotenv

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 1.2.2 |
| **Vai trò** | Quản lý biến môi trường từ file `.env` |
| **Sử dụng trong** | Tất cả module yêu cầu cấu hình (thông tin xác thực S3, token MotherDuck, thông tin xác thực Supabase, v.v.) |

**Mục đích:** `python-dotenv` tải cấu hình và bí mật từ file `.env` vào biến môi trường. Điều này bao gồm thông tin xác thực AWS, token xác thực MotherDuck, chuỗi kết nối Supabase, thông tin đăng nhập ITviec, và khóa API Google Sheets.

**Lý do lựa chọn:**
- **Bảo mật:** Giữ bí mật khỏi mã nguồn và kiểm soát phiên bản (`.env` bị git-ignore).
- **Tuân thủ 12-factor app:** Tuân thủ nguyên tắc lưu trữ cấu hình trong môi trường.
- **Trải nghiệm nhà phát triển:** Đơn giản hóa phát triển cục bộ bằng cách tự động tải thông tin xác thực mà không cần thiết lập biến môi trường thủ công.

---

### 8.5 Pydantic

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 2.13.4 |
| **Vai trò** | Xác thực dữ liệu và quản lý cài đặt |
| **Sử dụng trong** | Xác thực cấu hình và mô hình dữ liệu |

**Mục đích:** Pydantic cung cấp xác thực dữ liệu runtime với chú thích kiểu Python. Nó xác minh schema cấu hình và đảm bảo tính toàn vẹn dữ liệu tại ranh giới pipeline.

**Lý do lựa chọn:**
- **Cấu hình an toàn kiểu:** Mô hình Pydantic xác minh biến môi trường và file cấu hình khi khởi động, thất bại nhanh khi cấu hình sai.
- **Tuần tự hóa:** Chuyển đổi tự động giữa đối tượng Python và biểu diễn JSON/dict đơn giản hóa trao đổi dữ liệu giữa các giai đoạn pipeline.

---

### 8.6 PyYAML

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Phiên bản** | 6.0.3 |
| **Vai trò** | Phân tích file cấu hình YAML |
| **Sử dụng trong** | Các file YAML cấu hình phụ trợ và cấu hình Airflow |

**Mục đích:** PyYAML phân tích các file cấu hình dựa trên YAML trong hệ thống; bucket S3 Bronze và Silver hiện được cấu hình qua `S3_BRONZE_BUCKET` và `S3_SILVER_BUCKET` trong `.env`.

**Lý do lựa chọn:**
- **Cấu hình dễ đọc:** YAML dễ đọc hơn JSON cho file cấu hình, giảm lỗi khi chỉnh sửa tên bucket hoặc cài đặt Airflow.
- **Thư viện tiêu chuẩn:** PyYAML là parser YAML được sử dụng rộng rãi nhất trong hệ sinh thái Python.

---

## 9. Bảng Tổng hợp Công nghệ

| Danh mục | Công nghệ | Phiên bản | Chức năng | Mục đích chính |
|----------|-----------|-----------|-----------|-----------------|
| **Thu thập dữ liệu** | aiohttp | 3.13.5 | Thu thập | HTTP client bất đồng bộ cho TopCV |
| **Thu thập dữ liệu** | curl_cffi | 0.15.0 | Thu thập | HTTP client mạo danh trình duyệt |
| **Thu thập dữ liệu** | nodriver | 0.48.1 | Thu thập | Tự động hóa Chrome headless |
| **Thu thập dữ liệu** | parsel | 1.11.0 | Thu thập | Phân tích HTML (CSS/XPath) |
| **Thu thập dữ liệu** | lxml | 6.1.0 | Thu thập | Parser HTML/XML tăng tốc C |
| **Lưu trữ dữ liệu** | Amazon S3 | - | Lưu trữ | Lưu trữ đối tượng đám mây (data lake) |
| **Lưu trữ dữ liệu** | DuckDB | 1.5.3 | Lưu trữ | Cơ sở dữ liệu phân tích trong tiến trình |
| **Lưu trữ dữ liệu** | MotherDuck | - | Lưu trữ | Dịch vụ DuckDB lưu trữ trên đám mây |
| **Lưu trữ dữ liệu** | Supabase (PostgreSQL) | - | Lưu trữ | Cơ sở dữ liệu backend cho frontend |
| **Lưu trữ dữ liệu** | psycopg2 | 2.9.10 | Lưu trữ | Driver PostgreSQL |
| **Lưu trữ dữ liệu** | boto3 | 1.43.6 | Lưu trữ | AWS S3 client |
| **Lưu trữ dữ liệu** | Qdrant | Cloud free tier | Lưu trữ | Vector database cho tìm kiếm ngữ nghĩa |
| **Xử lý dữ liệu** | Polars | 1.40.1 | Xử lý | Engine xử lý DataFrame |
| **Xử lý dữ liệu** | FlashText | 2.7 | Xử lý | Trích xuất từ khóa nhanh |
| **Xử lý dữ liệu** | RapidFuzz | 3.10.1 | Xử lý | Khớp chuỗi mờ |
| **Xử lý dữ liệu** | Great Expectations | 1.17.2 | Xử lý | Xác minh chất lượng dữ liệu |
| **Xử lý dữ liệu** | gspread | 6.2.1 | Xử lý | Quản lý taxonomy seed qua Google Sheets (13 file CSV) |
| **Xử lý dữ liệu** | OpenRouter Embedding | openai/text-embedding-3-small | Xử lý | Mô hình embedding đa ngôn ngữ (768 chiều) |
| **Xử lý dữ liệu** | pdfplumber | - | Xử lý | Trích xuất text từ CV PDF |
| **Xử lý dữ liệu** | python-docx | - | Xử lý | Trích xuất text từ CV DOCX |
| **Phục vụ & Báo cáo** | Power BI | - | Phục vụ | Báo cáo trí tuệ doanh nghiệp |
| **Điều phối** | Apache Airflow | 2.10.3 | Điều phối | Lập lịch pipeline và quản lý DAG |
| **Điều phối** | PostgreSQL | 16 | Điều phối | Cơ sở dữ liệu metadata Airflow |
| **Điều phối** | StatsD Exporter | v0.28.0 | Điều phối | Cầu nối chỉ số Airflow đến Prometheus |
| **Giám sát** | Prometheus | v3.12.0 | Giám sát | Thu thập chỉ số time-series |
| **Giám sát** | Grafana | 11.6 | Giám sát | Trực quan hóa bảng điều khiển |
| **Giám sát** | Caddy | alpine | Giám sát | Reverse proxy với xác thực và HTTPS |
| **Giám sát** | Altair | 6.1.0 | Giám sát | Tạo biểu đồ khai báo |
| **Giám sát** | Nginx | alpine | Giám sát | Máy chủ file tĩnh cho báo cáo |
| **Hạ tầng** | Docker | - | Hạ tầng | Nền tảng container hóa |
| **Hạ tầng** | Docker Compose | - | Hạ tầng | Điều phối multi-container |
| **Hạ tầng** | Python | 3.11 | Hạ tầng | Ngôn ngữ lập trình chính |
| **Hạ tầng** | python-dotenv | 1.2.2 | Hạ tầng | Quản lý biến môi trường |
| **Hạ tầng** | Pydantic | 2.13.4 | Hạ tầng | Xác thực dữ liệu |
| **Hạ tầng** | PyYAML | 6.0.3 | Hạ tầng | Phân tích cấu hình YAML |

---

## 10. Kiến trúc Luồng Dữ liệu

```
┌─────────────────────────────────────────────────────────────────────┐
│                     TẦNG THU THẬP DỮ LIỆU                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐                     │
│  │  TopCV   │  │  ITviec  │  │ VietnamWorks │                     │
│  │ (aiohttp │  │(nodriver)│  │  (nodriver)  │                     │
│  │curl_cffi)│  │          │  │              │                     │
│  └────┬─────┘  └────┬─────┘  └──────┬───────┘                     │
│       │              │               │                              │
│       ▼              ▼               ▼                              │
│  temp_data/*.jsonl  (file cục bộ chỉ ghi thêm)                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  XÁC MINH (GX)      │
                    └──────────┬──────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                     TẦNG LƯU TRỮ                                    │
│                                                                     │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐           │
│  │   BRONZE    │    │   SILVER    │    │    GOLD     │           │
│  │  S3 JSONL   │───▶│ S3 Parquet  │───▶│ MotherDuck  │           │
│  │  (gzip)    │    │ (đã làm sạch)│    │ (star schema)│           │
│  └─────────────┘    └──────┬──────┘    └──────┬──────┘           │
│                            │                   │                  │
│  ┌────────────────────┐   │            ┌──────▼──────┐          │
│  │  SEED TAXONOMY     │   │            │  Power BI   │          │
│  │  (Google Sheets +  │───┤            │  (.pbix)    │          │
│  │   CSV fallback)    │   │            │──▶ Báo cáo BI│         │
│  │  13 file: kỹ năng, │   │            └─────────────┘          │
│  │  ngành, quyền lợi, │   │                                      │
│  │  địa điểm, công ty │   │                                      │
│  └────────────────────┘   │                                      │
│                     ┌────▼──────┐                                  │
│                     │  SUPABASE  │                                  │
│                     │(PostgreSQL)│                                  │
│                     │──▶ Frontend│                                  │
│                     └───────────┘                                  │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                 TẦNG GỢI Ý CV (CV Recommendation)                    │
│                                                                     │
│  ┌──────────────┐    ┌──────────────────┐    ┌──────────────┐     │
│  │ Silver S3    │───▶│ OpenRouter Embed   │───▶│   Qdrant     │     │
│  │ (Parquet)    │    │ (768d, cosine)    │    │  (Vector DB) │     │
│  └──────────────┘    └──────────────────┘    └──────┬───────┘     │
│                                                      │              │
│  ┌──────────────┐    ┌──────────────────┐            │              │
│  │ CV Upload     │    │ pdfplumber +     │            │              │
│  │ (PDF/DOCX)   │───▶│ python-docx       │            │              │
│  └──────────────┘    │ (parse CV text)   │            │              │
│                      └────────┬─────────┘            │              │
│                               │                      │              │
│                      ┌────────▼─────────┐            │              │
│                      │ OpenRouter Embed   │            │              │
│                      │ (search_query)     │            │              │
│                      └────────┬─────────┘            │              │
│                               │                      │              │
│                      ┌────────▼─────────┐            │              │
│                      │ Hybrid Re-rank     │◀───────────┘              │
│                      │ 0.6*cosine +       │                           │
│                      │ 0.4*skill_overlap  │───▶ Top 10 gợi ý         │
│                      └──────────────────┘                            │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                   TẦNG ĐIỀU PHỐI                                     │
│  ┌──────────────────────────────────────────────────┐              │
│  │              Apache Airflow 2.10                  │              │
│  │  ┌─────────┐ ┌──────────┐ ┌────────────────┐   │              │
│  │  │Scheduler│ │Webserver │ │ DockerOperator  │   │              │
│  │  └─────────┘ └──────────┘ └───────┬────────┘   │              │
│  └─────────────────────────────────────┼────────────┘              │
│                                       │                             │
│                              ┌────────▼────────┐                   │
│                              │ lakehouse-crawler│                   │
│                              │  (Docker image)  │                   │
│                              └─────────────────┘                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                    TẦNG GIÁM SÁT                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │Prometheus│◀─│  StatsD  │  │  Grafana  │  │  Caddy   │         │
│  │          │  │ Exporter │  │          │  │(auth+TLS)│         │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘         │
│  ┌──────────┐  ┌──────────┐                                     │
│  │  Altair  │  │  Nginx   │  (Bảng điều khiển doanh nghiệp)    │
│  └──────────┘  └──────────┘                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 11. Lý do Lựa chọn Công nghệ trong Bối cảnh Đồ án Tốt nghiệp

### 11.1 Tại sao chọn kiến trúc Medallion?

Kiến trúc Medallion (Bronze → Silver → Gold) được chọn vì:
- **Dòng dữ liệu:** Mỗi tầng bảo toàn lịch sử biến đổi, cho phép gỡ lỗi và kiểm toán.
- **Xử lý tăng dần:** Dữ liệu mới chạy qua các tầng tăng dần, hỗ trợ vận hành pipeline liên tục.
- **Phân tách trách nhiệm:** Dữ liệu thô (Bronze), dữ liệu đã làm sạch (Silver), và dữ liệu sẵn sàng cho doanh nghiệp (Gold) được phân tách vật lý, ngăn chặn làm hỏng dữ liệu nguồn vô tình.

### 11.2 Tại sao chọn S3 làm Lưu trữ Data Lake?

- Định dạng Parquet phân vùng Hive trên S3 có thể được truy vấn trực tiếp bởi DuckDB/MotherDuck, loại bỏ cần kho dữ liệu riêng biệt.
- Lưu trữ hiệu quả chi phí cho dữ liệu thô có thể được xử lý lại nhiều lần khi logic làm sạch phát triển.
- Cách tiếp cận tiêu chuẩn ngành thể hiện thực hành kỹ thuật dữ liệu thực tế.

### 11.3 Tại sao chọn Polars thay vì Pandas?

- Lazy evaluation và thực thi đa luồng của Polars cung cấp cải thiện hiệu suất đáng kể cho workload ETL.
- Hệ thống kiểu nghiêm ngặt phát hiện vấn đề chất lượng dữ liệu tại thời điểm chạy.
- Thể hiện kiến thức về công cụ xử lý dữ liệu hiện đại vượt ra ngoài hệ sinh thái pandas truyền thống.

### 11.4 Tại sao chọn Airflow thay vì các thay thế (ví dụ: Prefect, Dagster)?

- Airflow là tiêu chuẩn ngành cho điều phối workflow, làm cho dự án có thể chuyển giao ngay lập tức cho môi trường sản xuất.
- Mẫu DockerOperator thể hiện phân tách trách nhiệm đúng cách — bộ điều phối chỉ lập lịch, không bao giờ thực thi logic doanh nghiệp.
- Giao diện phong phú để giám sát trạng thái thực thi pipeline.

### 11.5 Tại sao chọn MotherDuck thay vì DuckDB tự lưu trữ?

- Không cần quản lý hạ tầng cho tầng Gold — phù hợp cho đồ án tốt nghiệp với tài nguyên máy chủ hạn chế.
- Tích hợp S3 cho phép DuckDB đọc Silver Parquet trực tiếp, đơn giản hóa tầng Gold thành các biến đổi SQL thuần.
- Thể hiện thực hành kỹ thuật dữ liệu đám mây.

### 11.6 Tại sao chọn Supabase thay vì backend tùy chỉnh?

- REST API tự động tạo loại bỏ cần xây dựng và duy trì máy chủ backend.
- Hỗ trợ PostgreSQL UPSERT đảm bảo tải dữ liệu idempotent.
- Khả năng thời gian thực cho phép cập nhật bảng điều khiển trực tiếp mà không cần hạ tầng thêm.

### 11.7 Tại sao chọn Power BI cho báo cáo doanh nghiệp?

- **Tương thích star-schema:** Tầng Gold được thiết kế đặc biệt dưới dạng star schema (bảng fact + bảng dimension + bridge table) để khớp với mẫu mô hình dữ liệu ưa thích của Power BI, cho phép tính toán DAX hiệu quả và quan hệ drill-down tự nhiên.
- **DirectQuery đến MotherDuck:** Power BI kết nối đến MotherDuck Cloud qua ODBC, cho phép truy vấn thời gian thực đối với tầng Gold mà không cần nhập dữ liệu cục bộ — bảng điều khiển luôn phản ánh kết quả pipeline mới nhất.
- **Công cụ BI tiêu chuẩn ngành:** Power BI là công cụ BI được áp dụng rộng rãi nhất trong môi trường doanh nghiệp, làm cho dự án có thể chuyển giao ngay lập tức cho các nhóm phân tích thực tế.
- **Trực quan phong phú:** Power BI cung cấp trực quan nâng cao (bản đồ, cây phân tích, yếu tố ảnh hưởng chính) vượt xa những gì Grafana hay Altair có thể cung cấp cho báo cáo doanh nghiệp.

### 11.8 Tại sao chọn Qdrant + OpenRouter Embedding cho gợi ý CV?

- **Tìm kiếm ngữ nghĩa + filter nghiệp vụ:** Qdrant hỗ trợ kết hợp vector similarity search với filter cứng (location, experience, deadline) trong cùng một truy vấn, đảm bảo kết quả gợi ý vừa liên quan ngữ nghĩa vừa tuân thủ ràng buộc thực tế — điều mà tìm kiếm từ khóa truyền thống không thể làm.
- **Hybrid scoring (vector + taxonomy):** Công thức `final = 0.6*cosine + 0.4*skill_overlap` kết hợp điểm ngữ nghĩa (cosine similarity) với điểm kỹ năng trùng khớp (FlashText taxonomy), giải quyết hạn chế của từng phương pháp đơn lẻ: vector bỏ sót kỹ năng chính xác, taxonomy bỏ sót từ đồng nghĩa.
- **Managed cloud free tier:** Qdrant Cloud free tier loại bỏ cần tự host và vận hành vector database, phù hợp quy mô đồ án tốt nghiệp với hàng nghìn job.
- **OpenRouter embedding linh hoạt:** `openai/text-embedding-3-small` là default ổn định, nhưng vẫn có thể đổi sang model embedding khác qua env mà không sửa code.
- **Dimension cố định:** Dùng 768 chiều để giảm chi phí lưu trữ trên Qdrant so với vector 1536 chiều đầy đủ, miễn là cả Lakehouse-Lite và backend query dùng cùng contract.
- **Idempotent indexing:** Sử dụng UUID5 dựa trên `job_url` làm point ID cho phép chạy lại pipeline index nhiều lần mà không tạo bản ghi trùng lặp — quan trọng cho DAG chạy hằng ngày.
