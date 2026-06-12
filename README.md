# Lakehouse-Lite

A data lakehouse pipeline for job postings from multiple Vietnamese recruitment platforms (TopCV, VietnamWorks, ITviec), focused on data-field roles such as Data Engineer, Data Scientist, Data Analyst, AI/ML Engineer, Business Intelligence, and Machine Learning Engineer.

The pipeline follows a **Bronze → Silver → Gold** medallion architecture stored in AWS S3, with serving layers via Supabase (OLTP) and MotherDuck (OLAP). Orchestration is handled by Apache Airflow with DockerOperator.

## Quick Start

```bash
docker compose --project-directory . -f src/orchestration_layer/docker-compose.yaml up -d
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Crawl Layer                                  │
│   TopCV (HTTP)  ─┐                                                  │
│   VietnamWorks (Browser) ──► temp_data/*.jsonl (local staging)     │
│   ITviec (Browser) ─┘                                              │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Storage Layer (AWS S3)                          │
│                                                                     │
│   Bronze Bucket                Silver Bucket                        │
│   ┌──────────────────┐        ┌──────────────────────────┐         │
│   │ <source>/jobs/    │        │ jobs/source_site=<site>/  │         │
│   │  year=YYYY/       │  clean │  year=YYYY/               │         │
│   │  month=MM/        │ ─────► │  month=MM/                │         │
│   │  day=DD/          │        │  day=DD/                  │         │
│   │  *.jsonl.gz       │        │  *.parquet                │         │
│   └──────────────────┘        └──────────────────────────┘         │
│                                                                     │
│   Gold (MotherDuck) — reads Silver Parquet directly from S3         │
│   ┌──────────────────────────────────────────────────┐             │
│   │  gold.jobs (fact)                                 │             │
│   │  gold.dim_date, gold.job_industries,              │             │
│   │  gold.job_benefits, gold.job_requirements         │             │
│   │  gold.dim_*_taxonomy (dimension tables)           │             │
│   └──────────────────────────────────────────────────┘             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Serving Layer                                   │
│   Supabase (OLTP) ─► Job Search Web (Next.js, planned)            │
│   MotherDuck (OLAP) ─► BI Dashboards (Power BI)                   │
└─────────────────────────────────────────────────────────────────────┘
```

See [`documents/architecture.html`](documents/architecture.html) for the full intended architecture diagram.

## Project Structure

```
Lakehouse-Lite/
├── .env.example                  # Environment variables template
├── .gitignore
├── AGENTS.md                     # Agent instructions & pipeline notes
├── Dockerfile                    # python:3.11-slim + Chrome + xvfb
├── LICENSE                       # Apache 2.0
├── requirements.txt              # Pinned Python dependencies
├── documents/
│   └── architecture.html         # Full architecture diagram
├── src/
│   ├── crawl_layer/              # Web scraping & data collection
│   │   ├── config/
│   │   │   └── path.py           # SRC_DIR, TEMP_DIR constants
│   │   ├── crawler/
│   │   │   ├── topcv/            # TopCV crawler (HTTP-based)
│   │   │   ├── vietnamworks/     # VietnamWorks crawler (browser-based)
│   │   │   └── itviec/           # ITviec crawler (browser-based, requires login)
│   │   ├── data_model/
│   │   │   └── data_class.py     # JobItem, TopCVJobItem, ITViecJobItem, VietnamWorksJobItem
│   │   ├── utils/
│   │   │   ├── loader.py         # save_to_temp, load_to_bronze
│   │   │   └── clean_temp.py     # clean_temp_directory
│   │   └── test/                 # Ad-hoc test scripts
│   ├── storage_layer/
│   │   ├── MinIO_S3/             # AWS S3 object storage (legacy folder name kept)
│   │   │   ├── config/
│   │   │   │   ├── bucket.yml    # Bucket names (thethien-lakehouse-lite-bronze/silver)
│   │   │   │   ├── key.py        # AWS credentials from .env
│   │   │   │   └── path.py       # BronzeBucketPaths, SilverBucketPaths, DEFAULT_ENTITY_NAME
│   │   │   ├── utils/
│   │   │   │   └── minio_connect.py  # get_s3_client()
│   │   │   └── layer/
│   │   │       ├── bronze/       # Bronze layer: raw data upload
│   │   │       │   └── main.py   # Upload temp JSONL → S3 Bronze (--source flag)
│   │   │       ├── local_temp/   # Local staging & validation
│   │   │       │   └── validation/
│   │   │       │       ├── topcv_validate.py
│   │   │       │       ├── itviec_validate.py
│   │   │       │       └── vnworks_validate.py
│   │   │       └── silver/       # Silver layer: cleaned data
│   │   │           ├── data_model/
│   │   │           │   └── data_class.py   # SilverJobItem (single source of truth for schema)
│   │   │           ├── cleaning/
│   │   │           │   ├── common/          # Shared cleaning functions
│   │   │           │   │   ├── pipeline.py  # run_pipeline, main_for_site, enforce_silver_schema
│   │   │           │   │   ├── clean_benefit.py
│   │   │           │   │   ├── clean_company_name.py
│   │   │           │   │   ├── clean_job_title.py
│   │   │           │   │   ├── clean_job_industry.py
│   │   │           │   │   ├── clean_location.py
│   │   │           │   │   ├── clean_salary.py
│   │   │           │   │   ├── clean_requirement.py
│   │   │           │   │   └── ... (more cleaners)
│   │   │           │   ├── clean_topcv/     # TopCV-specific cleaning
│   │   │           │   ├── clean_itviec/    # ITviec-specific cleaning
│   │   │           │   └── clean_vnworks/   # VietnamWorks-specific cleaning
│   │   │           ├── seeds/               # Taxonomy CSV files for classification
│   │   │           ├── utils/
│   │   │           │   ├── reader.py        # Read Bronze/Silver from S3
│   │   │           │   ├── loader.py        # upload_silver_parquet
│   │   │           │   ├── config_loader.py # load_config_yaml, read_seeds
│   │   │           │   ├── flashtext_extractor.py  # HybridKeywordExtractor
│   │   │           │   ├── clean_text.py    # HTML/bullet cleanup
│   │   │           │   └── normalize_data.py
│   │   │           └── test/                # Ad-hoc test scripts
│   │   ├── MotherDuck/            # Gold layer (OLAP) — star schema for BI
│   │   │   ├── client.py          # MotherDuckClient (DuckDB connection + S3 secret)
│   │   │   ├── config.py          # Gold schema, table names, column mappings
│   │   │   ├── main.py            # Entry point → load_silver_to_gold
│   │   │   ├── schema/
│   │   │   │   └── data_class.py  # GoldJobItem dataclass
│   │   │   └── scripts/
│   │   │       ├── load_silver_to_gold.py     # Full Gold star-schema rebuild
│   │   │       └── load_taxonomy_to_gold.py   # Taxonomy CSVs → dim tables
│   │   └── Supabase/              # Serving layer (OLTP) — job search backend
│   │       ├── schema/
│   │       │   └── data_class.py  # JobData dataclass (7 columns for search)
│   │       └── scripts/
│   │           ├── config.py      # Table DDL, UPSERT SQL, type mapping
│   │           ├── connection_config.py  # psycopg2 connection
│   │           └── load_silver_to_supabase.py  # Silver → Supabase UPSERT
│   ├── orchestration_layer/       # Apache Airflow (Docker Compose)
│   │   ├── docker-compose.yaml    # Airflow 2.10.3 + Postgres 16
│   │   ├── config/
│   │   ├── dags/
│   │   │   ├── _dag_factory.py    # Single source of pipeline shape (SITE_CONFIGS)
│   │   │   ├── crawl_*.py         # 1-line DAG wrappers (topcv, itviec, vietnamworks)
│   │   │   ├── validate_bronze_*.py
│   │   │   ├── silver_*.py
│   │   │   └── supabase_load_all.py
│   │   └── plugins/
│   ├── serving_layer/             # (placeholder, not implemented)
│   ├── monitoring_layer/          # (placeholder, not implemented)
│   └── recommend_layer/           # (placeholder, not implemented)
```

## Data Models

### Bronze — [`JobItem`](src/crawl_layer/data_model/data_class.py)

Raw scraped data shared across all sources:

| Field | Type | Description |
|-------|------|-------------|
| `job_title` | `str` | Job title |
| `company_name` | `str` | Company name |
| `location` | `str` | Job location |
| `job_industry` | `str` | Industry (raw text) |
| `job_description` | `str` | Full description |
| `source_site` | `str` | topcv / itviec / vietnamworks |
| `job_url` | `str` | Original posting URL |
| `search_keyword` | `str` | Keyword used to find this job |
| `scraped_at` | `str` | Timestamp of scrape |
| `salary` | `str` | Salary info (raw text) |
| `benefits` | `str` | Benefits (raw text) |
| `requirements` | `str` | Requirements (raw text) |

### Silver — [`SilverJobItem`](src/storage_layer/MinIO_S3/layer/silver/data_model/data_class.py:8)

Cleaned and enriched data with structured fields. The dataclass is the **single source of truth** for the Silver schema — adding or removing a field here automatically propagates to the entire pipeline via [`silver_schema_to_polars()`](src/storage_layer/MinIO_S3/layer/silver/data_model/data_class.py:108).

Key enrichment categories:

- **Job title**: `clean_job_title`, `job_title_special_keywords` (extracted skills in title)
- **Company**: `company_name_canonical` (normalized), `company_size`, `min_company_size`, `max_company_size`
- **Location**: `clean_location` (normalized), `is_vietnam`
- **Industry**: `job_industry_clean` (multi-label list), `job_industry_unmapped`
- **Job details**: `job_type`, `job_position`, `experience_level`, `min_exp_level`, `max_exp_level`, `education_level`
- **Salary**: `min_monthly_salary`, `max_monthly_salary`
- **Benefits**: `benefits_text_clean`, `benefits_categories_vi` (categorized list)
- **Description**: `job_description_cleaned`
- **Requirements**: `requirements_cleaned`, plus 9 taxonomy-backed lists:
  - `require_programming_languages`, `require_frameworks`, `require_tools`
  - `require_cloud_skills`, `require_knowledge`, `require_domain_knowledge`
  - `require_foreign_languages`, `require_domain_university`

### Gold — `GoldJobItem` (MotherDuck star schema)

Curated BI projection modeled as a star schema for Power BI:

| Table | Description |
|-------|-------------|
| `gold.jobs` | Fact table: one row per `job_url` with scalar columns + `source_site` + `date_key` FK |
| `gold.dim_date` | Date dimension: contiguous calendar from 2023-01-01, with year/quarter/month/day/weekday attributes |
| `gold.job_industries` | Bridge: unnested `job_industry_clean` → `(job_url, industry)` |
| `gold.job_benefits` | Bridge: unnested `benefits_categories_vi` → `(job_url, benefit)` |
| `gold.job_requirements` | Bridge: all `require_*` + `job_title_special_keywords` → `(job_url, requirement_type, value)` |
| `gold.dim_*_taxonomy` | Dimension tables loaded from Silver seed CSVs |

### Supabase — [`JobData`](src/storage_layer/Supabase/schema/data_class.py:3)

Lightweight projection for job search (7 columns): `job_url`, `job_title`, `company_name`, `location`, `job_deadline`, `job_title_special_keywords`, `source_site`.

## Storage Layout

### Bronze (S3)

```
s3://<bronze-bucket>/
├── topcv/jobs/year=2026/month=05/day=12/topcv_jobs_20260512_170702.jsonl.gz
├── itviec/jobs/year=2026/month=05/day=12/itviec_jobs_20260512_170702.jsonl.gz
└── vietnamworks/jobs/year=2026/month=05/day=12/vietnamworks_jobs_20260512_170702.jsonl.gz
```

### Silver (S3)

```
s3://<silver-bucket>/
└── jobs/
    ├── source_site=topcv/year=2026/month=05/day=12/clean_bronze_20260512_170702.parquet
    ├── source_site=itviec/year=2026/month=05/day=12/clean_bronze_20260512_170702.parquet
    └── source_site=vietnamworks/year=2026/month=05/day=12/clean_bronze_20260512_170702.parquet
```

> Silver uses Hive-partitioning with `source_site=<site>` (not `source/`). MotherDuck reads these Parquet files directly from S3 via `read_parquet(..., hive_partitioning=true)`.

## Taxonomy Seeds

The Silver cleaning layer uses 12 CSV taxonomy files stored in [`src/storage_layer/MinIO_S3/layer/silver/seeds/`](src/storage_layer/MinIO_S3/layer/silver/seeds) for keyword extraction and classification:

| Seed File | Purpose |
|-----------|---------|
| `program_lang_taxonomy.csv` | Programming language classification |
| `tools_taxonomy.csv` | Data tools classification |
| `framework_taxonomy.csv` | ML/DL framework classification |
| `cloud_skill_taxonomy.csv` | Cloud platform & service classification |
| `language_taxonomy.csv` | Foreign language requirements |
| `knowledge_taxonomy.csv` | Domain knowledge classification |
| `domain_taxonomy.csv` | Business domain classification |
| `domain_university_taxonomy.csv` | University domain mapping |
| `industry_taxonomy.csv` | Industry classification |
| `benefit_taxonomy.csv` | Benefit categorization |
| `location_mapping.csv` | Location normalization |
| `company_mapping.csv` | Company name normalization |

## Quick Start

### 1. Setup Environment

```bash
python -m venv venv
venv\Scripts\activate        # Windows
# or: source venv/bin/activate  # Linux/macOS
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy [`.env.example`](.env.example) to `.env` and fill in required values:

```bash
cp .env.example .env
```

Required variables:
- `ITVIEC_USERNAME` / `ITVIEC_PASSWORD` — for ITviec crawler login
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` — for AWS S3
- `SUPABASE_HOST` / `SUPABASE_PORT` / `SUPABASE_DATABASE` / `SUPABASE_USER` / `SUPABASE_PASSWORD` — for Supabase serving layer
- `MOTHERDUCK_TOKEN` — for MotherDuck Gold layer

For Airflow orchestration, also configure:
- `FERNET_KEY` / `WEBSERVER_SECRET_KEY` / `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`
- `HOST_REPO_PATH` — absolute path to this repo on the host filesystem
- `PIPELINE_IMAGE` — Docker image name (default: `lakehouse-pipeline:latest`)
- `AIRFLOW_UID` (default: `50000`)

### 3. Provision S3 Buckets

Create the two buckets referenced in [`src/storage_layer/MinIO_S3/config/bucket.yml`](src/storage_layer/MinIO_S3/config/bucket.yml) (`thethien-lakehouse-lite-bronze` and `thethien-lakehouse-lite-silver`). Rename them in the YAML if those names are already taken globally on S3.

The IAM user needs `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on both buckets.

### 4. Build the Pipeline Docker Image

```bash
docker build -t lakehouse-pipeline:latest .
```

### 5. Run Crawlers

```bash
# TopCV (HTTP-based, no browser needed)
python -m src.crawl_layer.crawler.topcv --keyword "data" --max-pages 2

# VietnamWorks (browser-based)
python -m src.crawl_layer.crawler.vietnamworks --keyword "data analyst" --max-pages 2 [--headless]

# ITviec (browser-based, requires .env login credentials)
python -m src.crawl_layer.crawler.itviec --keyword "data" --max-pages 2 [--headless]
```

Crawled data is appended to daily temp files at `src/crawl_layer/temp_data/<source>_jobs_YYYYMMDD.jsonl`.

### 6. Upload to Bronze

```bash
# All sites
python -m src.storage_layer.MinIO_S3.layer.bronze.main

# Single site
python -m src.storage_layer.MinIO_S3.layer.bronze.main --source topcv
python -m src.storage_layer.MinIO_S3.layer.bronze.main --source itviec
python -m src.storage_layer.MinIO_S3.layer.bronze.main --source vietnamworks
```

Compresses temp `.jsonl` → `.jsonl.gz` and uploads to S3 Bronze, then clears the source's temp files.

### 7. Validate Temp Data (Optional)

```bash
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.topcv_validate
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.itviec_validate
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.vnworks_validate
```

### 8. Run Silver Cleaning

```bash
# TopCV Silver
python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_topcv.main_process \
    --from_date 2026-05-01 --to_date 2026-05-12

# ITviec Silver
python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_itviec.main_process \
    --from_date 2026-05-01 --to_date 2026-05-12

# VietnamWorks Silver
python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_vnworks.main_process \
    --from_date 2026-05-01 --to_date 2026-05-12
```

Common CLI flags (via [`build_argument_parser`](src/storage_layer/MinIO_S3/layer/silver/cleaning/common/pipeline.py:97)):

| Flag | Description |
|------|-------------|
| `--from_date` (required) | Inclusive start date, `YYYY-MM-DD` |
| `--to_date` (required) | Inclusive end date, `YYYY-MM-DD` |
| `--entity_name` | Entity name (default: `jobs`) |
| `--no_save` | Dry run: skip Parquet upload, log cleaned row count |
| `--export_parquet` | Dump cleaned Parquet to `debug_output/` for inspection |

### 9. Load to Supabase (OLTP Serving)

```bash
python -m src.storage_layer.Supabase.scripts.load_silver_to_supabase \
    --from_date 2026-05-01 --to_date 2026-05-12
```

Idempotent: runs `CREATE TABLE IF NOT EXISTS`, then UPSERTs on `job_url`. Processes all three sites with per-site commits.

### 10. Build Gold Layer (MotherDuck OLAP)

```bash
# Load Silver → Gold star schema
python -m src.storage_layer.MotherDuck.main

# Load taxonomy CSVs → dimension tables
python -m src.storage_layer.MotherDuck.scripts.load_taxonomy_to_gold
```

Gold does a full refresh (`CREATE OR REPLACE TABLE`) each run — fully idempotent. MotherDuck reads Silver Parquet directly from S3; no data flows through the local process.

## Orchestration (Airflow)

Boot the orchestration stack:

```bash
docker compose --project-directory . -f src/orchestration_layer/docker-compose.yaml up -d
```

Airflow webserver available at `http://localhost:8080`.

### DAG Architecture

All business logic runs inside sibling `lakehouse-pipeline` containers via `DockerOperator`. Airflow only orchestrates — it never imports crawler/storage modules directly.

[`_dag_factory.py`](src/orchestration_layer/dags/_dag_factory.py) is the single source of pipeline shape. Per-site DAG files (`crawl_topcv.py`, `silver_topcv.py`, ...) are 1-line wrappers.

| DAG | Schedule | Description |
|-----|----------|-------------|
| `crawl_<site>` | `0 */3 * * *` | Crawl → triggers `validate_bronze_<site>` (decoupled) |
| `validate_bronze_<site>` | None (triggered) | Validate temp → upload Bronze |
| `silver_<site>` | `0 */8 * * *` | Bronze → Silver cleaning |
| `supabase_load_all` | `0 */6 * * *` | Silver → Supabase UPSERT (all sites) |

**Requirements for DockerOperator:**
- `HOST_REPO_PATH` env var (absolute path to repo on host) — needed for bind mounts of `temp_data/` and `.env`
- `shm_size=2GB` on containers (required for Chrome)
- Browser-based crawlers run under `xvfb-run` in containers

## Pipeline Behavior

- **Crawlers append** to the same daily temp file (`<source>_jobs_YYYYMMDD.jsonl`). Repeated runs accumulate data until the Bronze loader processes and clears `temp_data`.
- **Bronze upload** partitions data by date: `<source>/jobs/year=YYYY/month=MM/day=DD/<source>_jobs_YYYYMMDD_HHMMSS.jsonl.gz`.
- **Silver cleaning** reads Bronze day-by-day within the specified date range, applies site-specific and common cleaning functions, then uploads cleaned Parquet to the Silver bucket at `jobs/source_site=<site>/year=YYYY/month=MM/day=DD/clean_bronze_TIMESTAMP.parquet`.
- **Silver schema** is derived from [`SilverJobItem`](src/storage_layer/MinIO_S3/layer/silver/data_model/data_class.py:8) via `silver_schema_to_polars()`. The cleaning order is load-bearing: `drop_unecessary_cols(df)` must run first; `clean_location` and `apply_industry_cleaning` drop the original columns.
- **Supabase load** is idempotent: `CREATE TABLE IF NOT EXISTS` + UPSERT on `job_url`. Per-site commits give partial progress on failure.
- **Gold build** is a full refresh (`CREATE OR REPLACE TABLE`). Keeps only the newest snapshot per `job_url` (newest day wins, within a day newest file wins).

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.11 |
| Data Processing | Polars |
| Browser Automation | nodriver (VietnamWorks, ITviec) |
| HTTP Client | curl_cffi, aiohttp, requests (TopCV) |
| HTML Parsing | lxml, parsel |
| Object Storage | AWS S3 via boto3 |
| Keyword Extraction | flashtext (HybridKeywordExtractor) |
| OLAP / Gold | MotherDuck (DuckDB), read_parquet from S3 |
| OLTP / Serving | Supabase (PostgreSQL), psycopg2 |
| Orchestration | Apache Airflow 2.10.3 with DockerOperator |
| Containerization | Docker, Docker Compose |
| License | Apache 2.0 |

## Implementation Status

| Layer | Status |
|-------|--------|
| Crawl Layer (TopCV, VietnamWorks, ITviec) | Implemented |
| Bronze Layer (S3 upload) | Implemented |
| Local Temp Validation | Implemented |
| Silver Layer (cleaning + Parquet upload) | Implemented |
| Gold Layer (MotherDuck star schema + taxonomy) | Implemented |
| Supabase (OLTP serving) | Implemented |
| Airflow Orchestration (DAG factory + DockerOperator) | Implemented |
| Next.js (Job Search Web) | Not implemented |
| Recommend Layer | Not implemented |
| Monitoring Layer | Not implemented |

## Notes

- Code uses absolute `src.*` imports; `src/` has no package `__init__.py`. All commands must run from the repo root.
- There is no pytest/lint/format/typecheck config. Files under `src/**/test/` are ad-hoc scripts, not formal test suites.
- The [`requirements.txt`](requirements.txt) is the only pinned dependency source.
- Bucket names are **hardcoded** in [`bucket.yml`](src/storage_layer/MinIO_S3/config/bucket.yml). Fork or rename them — they are globally unique on S3.
- The `MinIO_S3` folder name is **legacy** — it talks to real AWS S3 via `boto3`, not MinIO.
- The Dockerfile uses `python:3.11-slim` (not 3.13). `IS_DOCKER=1` is set so modules can branch on container vs. local execution.
- `CHROME_BIN=/usr/bin/google-chrome` is set explicitly in the Dockerfile because nodriver's `find_chrome_executable` is unreliable inside DockerOperator-spawned containers. On `amd64` the image installs Google Chrome; on other CPU architectures it installs distro Chromium and symlinks it to the same path.
