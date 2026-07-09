# Lakehouse-Lite

Một data lakehouse nhẹ (lightweight) thu thập (crawl) tin tuyển dụng từ ba nền tảng tuyển dụng của Việt Nam — **TopCV**, **ITviec**, và **VietnamWorks** — tập trung vào các vị trí liên quan đến dữ liệu (Data Engineer, Data Analyst, Data Scientist, AI/ML, BI).

Pipeline tuân theo **medallion architecture** (kiến trúc phân lớp: Bronze → Silver → Gold) trên AWS S3, cung cấp dữ liệu đã làm sạch cho **Supabase** (OLTP, dùng cho frontend tìm kiếm việc làm) và **MotherDuck** (OLAP, dùng cho BI), được điều phối toàn bộ bởi **Apache Airflow** chạy mọi tác vụ bên trong Docker container.

## Kiến trúc hệ thống

```
╭──────────────────────────── Crawl Layer ────────────────────────────╮
│  TopCV (nodriver + curl_cffi)  ─┐                                   │
│  ITviec (nodriver, login)      ─┼─► temp_data/<source>_jobs_*.jsonl │
│  VietnamWorks (nodriver)       ─┘        (local staging)            │
╰────────────────────────────────┬────────────────────────────────────╯
                                 │  validate (Great Expectations)
                                 ▼
╭─────────────────────── Storage Layer (AWS S3) ──────────────────────╮
│                                                                     │
│   Bronze bucket                      Silver bucket                  │
│   ╭─────────────────────╮   clean    ╭─────────────────────────╮    │
│   │ <source>/jobs/      │  (Polars)  │ jobs/source_site=<site>/│    │
│   │  year=/month=/day=/ │ ─────────► │  year=/month=/day=/     │    │
│   │  *.jsonl.gz         │            │  *.parquet              │    │
│   ╰─────────────────────╯            ╰────────────┬────────────╯    │
╰────────────────────────────────────────────────────┼────────────────╯
                                    ┌────────────────┴───────────────┐
                                    ▼                                ▼
╭──────────── Serving: Supabase (OLTP) ───────╮  ╭──── Gold: MotherDuck (OLAP) ────╮
│ PostgreSQL — UPSERT on job_url              │  │ DuckDB reads Silver parquet     │
│ powers the job-search frontend              │  │ directly from S3:               │
╰─────────────────────────────────────────────╯  │  gold.jobs (fact)               │
                                                 │  gold.dim_date, industries,     │
╭──────────── Monitoring ─────────────────────╮  │  benefits, requirements,        │
│ Caddy ─► Grafana + Prometheus               │  │  dim_*_taxonomy                 │
│       ─► Bronze/Silver HTML dashboards      │  ╰──────────────┬──────────────────╯
╰─────────────────────────────────────────────╯                 ▼
                                                          Power BI dashboard
```

Mọi thứ được lên lịch bởi **Airflow** (`DockerOperator`) — DAG không bao giờ import business logic; chúng chỉ chạy lệnh `python -m …` bên trong image `lakehouse-crawler`.

## Công nghệ sử dụng

| Lớp | Công nghệ |
|---|---|
| Thu thập dữ liệu | [nodriver](https://github.com/ultrafunkamsterdam/nodriver) (headless Chrome), `curl_cffi`, `aiohttp`, `parsel` |
| Kiểm tra dữ liệu | Great Expectations |
| Bronze / Silver | AWS S3 (`boto3`), Polars, FlashText, RapidFuzz |
| Gold | MotherDuck (DuckDB) — đọc S3 parquet qua hive partitioning |
| Phục vụ dữ liệu | Supabase (PostgreSQL, `psycopg2`) |
| Seed phân loại | Google Sheets (`gspread`) với fallback CSV cục bộ |
| Điều phối | Apache Airflow 2.10 + DockerOperator |
| Giám sát | Prometheus, Grafana, StatsD exporter, Caddy, Nginx, Altair dashboards |
| BI | Power BI (`src/bi_report_layer/analysis_dashboard.pbix`) |

## Cấu trúc dự án

```
Lakehouse-Lite/
├── Dockerfile                     # python:3.11-slim + Chrome + xvfb (pipeline image)
├── requirements.txt
├── .env.example                   # copy thành .env và điền thông tin
├── init.sh                        # khởi động cả hai stack cục bộ
├── init_orchestration.sh          # triển khai Airflow stack (EC2-A)
├── init_monitoring.sh             # triển khai monitoring stack (EC2-B)
├── documents/                     # tài liệu thiết kế, hướng dẫn triển khai & bảo mật
└── src/
    ├── crawl_layer/
    │   ├── crawler/{topcv,itviec,vietnamworks}/   # một module cho mỗi site
    │   ├── data_model/data_class.py               # JobItem + dataclass cho từng site
    │   └── temp_data/                             # JSONL staging cục bộ (git-ignored)
    ├── storage_layer/
    │   ├── MinIO_S3/              # tên legacy — thực tế kết nối AWS S3 thật
    │   │   └── layer/
    │   │       ├── local_temp/validation/         # kiểm tra Great Expectations
    │   │       ├── bronze/main.py                 # gzip + upload + xóa temp
    │   │       └── silver/
    │   │           ├── cleaning/{common,clean_topcv,clean_itviec,clean_vnworks}/
    │   │           ├── data_model/data_class.py   # SilverJobItem = nguồn sự thật
    │   │           └── seeds/                     # CSV phân loại (skills, industries…)
    │   ├── Supabase/scripts/load_silver_to_supabase.py
    │   └── MotherDuck/scripts/{load_silver_to_gold,load_taxonomy_to_gold}.py
    ├── orchestration_layer/
    │   ├── dags/                  # Airflow DAGs (chỉ điều phối, không có logic)
    │   └── docker-compose.yaml    # Airflow + Postgres + statsd-exporter
    ├── monitoring_layer/
    │   ├── docker-compose.yaml    # Caddy + Prometheus + Grafana + Nginx
    │   ├── business/              # trình tạo dashboard HTML Bronze/Silver (Altair)
    │   └── grafana/ prometheus/ nginx/ Caddyfile
    └── bi_report_layer/analysis_dashboard.pbix
```

## Bắt đầu nhanh

### Yêu cầu trước

- Python 3.11, Docker + Docker Compose v2
- AWS S3 buckets (bronze + silver), dự án Supabase, token MotherDuck
- Google Chrome (được cài tự động trong Docker image)

### Cài đặt

```bash
git clone https://github.com/thethien8a/Crawl-Job-System.git
cd Lakehouse-Lite

python -m venv venv
venv/Scripts/activate        # Windows  (Linux/macOS: source venv/bin/activate)
pip install -r requirements.txt

cp .env.example .env         # sau đó điền thông tin đăng nhập
```

Biến quan trọng trong `.env` (xem `.env.example` để có danh sách đầy đủ):

| Biến | Mục đích |
|---|---|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | truy cập S3 |
| `S3_BRONZE_BUCKET` / `S3_SILVER_BUCKET` | tên bucket (phải unique toàn cầu) |
| `ITVIEC_USERNAME` / `ITVIEC_PASSWORD` | tự động đăng nhập ITviec crawler |
| `SUPABASE_*` | kết nối Supabase Postgres |
| `MOTHERDUCK_TOKEN` / `MOTHERDUCK_DATABASE` | lớp Gold |
| `GOOGLE_SHEETS_*` | nguồn seed phân loại (tùy chọn, fallback về CSV cục bộ) |
| `FERNET_KEY`, `WEBSERVER_SECRET_KEY`, `_AIRFLOW_WWW_USER_*` | Airflow |

> **Lưu ý:** mọi lệnh `python -m` phải chạy từ thư mục gốc repo — `src/` không có `__init__.py` và import dùng đường dẫn tuyệt đối (`src.*`).

## Chạy pipeline thủ công

Các giai đoạn phải chạy theo thứ tự:

**1. Crawl (thu thập)** → ghi thêm vào `src/crawl_layer/temp_data/<source>_jobs_YYYYMMDD.jsonl`

```bash
python -m src.crawl_layer.crawler.topcv        --keyword "data" --max-pages 2
python -m src.crawl_layer.crawler.itviec       --keyword "data" --max-pages 2
python -m src.crawl_layer.crawler.vietnamworks --keyword "data" --max-pages 2
```

**2. Validate (kiểm tra — tùy chọn)** — Great Expectations kiểm tra trên file JSONL tạm

```bash
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.topcv_validate
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.itviec_validate
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.vnworks_validate
```

**3. Bronze** — nén gzip JSONL tạm → upload lên S3 → xóa file tạm cục bộ

```bash
python -m src.storage_layer.MinIO_S3.layer.bronze.main --source topcv
# bỏ --source để upload tất cả nguồn
```

**4. Silver** — làm sạch theo từng site (Polars), ghi parquet phân vùng theo `source_site=/year=/month=/day=`

```bash
python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_topcv.main_process   --from_date 2026-01-01 --to_date 2026-01-07
python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_itviec.main_process  --from_date 2026-01-01 --to_date 2026-01-07
python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_vnworks.main_process --from_date 2026-01-01 --to_date 2026-01-07
# thêm --no_save để chạy thử, --export_parquet để xuất parquet cục bộ dùng debug
```

**5. Supabase** — UPSERT jobs đã làm sạch vào Postgres (khóa xung đột: `job_url`)

```bash
python -m src.storage_layer.Supabase.scripts.load_silver_to_supabase --from_date 2026-01-01 --to_date 2026-01-07
```

**6. Gold (MotherDuck)** — DuckDB đọc trực tiếp Silver parquet từ S3 và xây dựng bảng fact/dim

```bash
python -m src.storage_layer.MotherDuck.scripts.load_silver_to_gold
python -m src.storage_layer.MotherDuck.scripts.load_taxonomy_to_gold   # seed các bảng dimension
```

## Chạy với Airflow (production)

Build pipeline image và khởi động mọi thứ:

```bash
./init_orchestration.sh   # build lakehouse-crawler:latest + khởi động Airflow stack
./init_monitoring.sh      # khởi động Caddy + Prometheus + Grafana + dashboards
# hoặc, cho một máy cục bộ:
./init.sh
```

- Airflow UI: `http://localhost:8080`
- Giám sát (qua Caddy, Basic Auth): `http://<MONITORING_DOMAIN>/business/` và `/grafana/`

### Tổng quan DAG

| DAG | Lịch trình | Chức năng |
|---|---|---|
| `crawl_topcv` / `crawl_itviec` / `crawl_vietnamworks` | mỗi 3 giờ (lệch nhau :00/:15/:30) | crawl → trigger DAG validate+bronze |
| `validate_bronze_<site>` | được trigger | Great Expectations validate → upload lên Bronze |
| `silver_<site>` | mỗi 8 giờ | làm sạch Bronze → Silver parquet |
| `supabase_load_all` | mỗi 6 giờ | Silver → Supabase UPSERT |
| `load_silver_to_gold` | mỗi 6 giờ | Silver → bảng Gold MotherDuck |
| `load_taxonomy_to_gold` | manual | seed CSV → bảng Gold dimension |
| `cluster_company_name` | manual | RapidFuzz clustering để chuẩn hóa tên công ty |
| `generate_bronze_dashboard` / `generate_silver_dashboard` | hằng ngày | render dashboard chất lượng dữ liệu HTML tĩnh |

Tham số DAG có thể override theo từng lần chạy qua Airflow UI *Trigger DAG w/ config*, ví dụ `{"keyword": "python", "max_pages": 5}` cho crawl hoặc `{"from_date": "...", "to_date": "..."}` cho silver/supabase.

## Triển khai

Hệ thống production phân tán trên hai EC2 chia sẻ volume EFS:

- **EC2-A (điều phối):** Airflow + Postgres + statsd-exporter + pipeline containers — cần nhiều CPU/RAM hơn (headless Chrome).
- **EC2-B (giám sát):** Caddy (TLS + Basic Auth) + Prometheus + Grafana + Nginx static dashboards.

Xem [`documents/deploy-two-ec2-efs.md`](documents/deploy-two-ec2-efs.md) và [`documents/security-group-guide.md`](documents/security-group-guide.md) để có hướng dẫn đầy đủ.

## Tài liệu

| Tài liệu | Nội dung |
|---|---|
| [`documents/system-design-report.md`](documents/system-design-report.md) | kiến trúc đầy đủ & thiết kế các lớp |
| [`documents/system-analysis-report.md`](documents/system-analysis-report.md) | yêu cầu, nguồn dữ liệu, mô hình dữ liệu |
| [`documents/technology-stack-documentation.md`](documents/technology-stack-documentation.md) | mọi công nghệ được dùng và lý do |
| [`documents/build-crawl-module-guide.md`](documents/build-crawl-module-guide.md) | cách thêm crawler cho site mới |
| [`documents/cv-recommendation-feature.md`](documents/cv-recommendation-feature.md) | tính năng gợi ý CV → việc làm dự kiến (Qdrant) |
| [`AGENTS.md`](AGENTS.md) | lưu ý & gotchas pipeline cho AI coding agents |

## Lưu ý & gotchas

- Tên thư mục `MinIO_S3` là **legacy** — thực tế kết nối tới AWS S3 thật qua `boto3`.
- Upload Bronze **xóa dữ liệu cục bộ**: xóa file JSONL tạm của nguồn sau khi upload thành công.
- Schema Silver được sinh từ dataclass `SilverJobItem` — thêm/xóa field ở đó, không bao giờ sửa string schema thủ công.
- Crawler ITviec và VietnamWorks patch lỗi teardown `ProactorEventLoop` chỉ dành cho Windows của nodriver — đừng xóa.
- Chưa có pytest/CI — kiểm tra thay đổi bằng `--no_save` (silver) hoặc `--max-pages 1` (crawlers).

## Giấy phép

[Apache 2.0](LICENSE)
