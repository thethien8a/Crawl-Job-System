# Lakehouse-Lite

A data lakehouse pipeline for job postings from multiple Vietnamese recruitment platforms (TopCV, VietnamWorks, ITviec), focused on data-field roles such as Data Engineer, Data Scientist, Data Analyst, AI/ML Engineer, Business Intelligence, and Machine Learning Engineer.

The pipeline follows a **Bronze -> Silver** medallion architecture stored in AWS S3, with planned future serving layers via Supabase (OLTP) and ClickHouse (OLAP).

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
│   ┌──────────────────┐        ┌──────────────────┐                 │
│   │ source/jobs/      │        │ jobs/source/      │                │
│   │  year=YYYY/       │  clean │  year=YYYY/       │                │
│   │  month=MM/        │ ─────► │  month=MM/        │                │
│   │  day=DD/          │        │  day=DD/          │                │
│   │  *.jsonl.gz       │        │  *.parquet        │                │
│   └──────────────────┘        └──────────────────┘                 │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼ (planned, not yet implemented)
┌─────────────────────────────────────────────────────────────────────┐
│                     Serving Layer                                   │
│   Supabase (OLTP) ─► Job Search Web (Next.js)                     │
│   MotherDuck (OLAP) ─► BI Dashboards                              │
└─────────────────────────────────────────────────────────────────────┘
```

See [`documents/architecture.html`](documents/architecture.html) for the full intended architecture diagram.

## Project Structure

```
Lakehouse-Lite/
├── .env.example                  # Environment variables template
├── .gitignore
├── AGENTS.md                     # Agent instructions & pipeline notes
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
│   │   │   │   ├── bucket.yml    # Bucket names: bronze, silver
│   │   │   │   ├── key.py        # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
│   │   │   │   └── path.py       # BronzeBucketPaths, SilverBucketPaths
│   │   │   ├── utils/
│   │   │   │   └── minio_connect.py  # get_s3_client()
│   │   │   └── layer/
│   │   │       ├── bronze/       # Bronze layer: raw data upload
│   │   │       │   └── main.py   # Upload temp JSONL -> S3 Bronze
│   │   │       ├── local_temp/   # Local staging & validation
│   │   │       │   └── validation/
│   │   │       │       ├── topcv_validate.py
│   │   │       │       ├── itviec_validate.py
│   │   │       │       └── vnworks_validate.py
│   │   │       └── silver/       # Silver layer: cleaned data
│   │   │           ├── data_model/
│   │   │           │   └── data_class.py   # SilverJobItem
│   │   │           ├── cleaning/
│   │   │           │   ├── common/          # Shared cleaning functions
│   │   │           │   │   ├── pipeline.py  # run_pipeline, main_for_site
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
│   │   ├── MotherDuck/            # (placeholder, not implemented)
│   │   └── Supabase/              # (placeholder, not implemented)
│   ├── bi_serving_layer/          # (placeholder, not implemented)
│   ├── monitoring_layer/          # (placeholder, not implemented)
│   ├── orchestrator_layer/        # (placeholder, not implemented)
│   └── recommend_layer/           # (placeholder, not implemented)
```

## Data Models

### Bronze - [`JobItem`](src/crawl_layer/data_model/data_class.py:3)

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

### Silver - [`SilverJobItem`](src/storage_layer/MinIO_S3/layer/silver/data_model/data_class.py:4)

Cleaned and enriched data with structured fields:

- **Industry enrichment**: `job_industries` (multi-label list), `job_industry_primary`, `job_industry_l1` (rollup group), `industry_mapping_method`, `industry_mapping_confidence`
- **Structured requirements** (12 taxonomy-backed categories):
  - `require_programming_languages`, `require_databases`, `require_cloud_platforms`, `require_cloud_services`
  - `require_big_data_tools`, `require_ml_frameworks`, `require_visualization_tools`
  - `require_nlp_skills`, `require_cv_skills`, `require_devops_tools`
  - `require_domain_knowledge`, `require_foreign_languages`
- **Boolean flags**: `has_sql_requirement`, `has_python_requirement`, `has_cloud_requirement`, `has_ml_requirement`, `has_big_data_requirement`

## Storage Layout

### Bronze (S3)

```
bronze/
├── itviec/jobs/year=2026/month=05/day=12/itviec_jobs_20260512_170702.jsonl.gz
├── topcv/jobs/year=2026/month=05/day=12/topcv_jobs_20260512_170702.jsonl.gz
└── vietnamworks/jobs/year=2026/month=05/day=12/vietnamworks_jobs_20260512_170702.jsonl.gz
```

### Silver (S3)

```
silver/
├── jobs/itviec/year=2026/month=05/day=12/clean_bronze_20260512_170702.parquet
├── jobs/topcv/year=2026/month=05/day=12/clean_bronze_20260512_170702.parquet
└── jobs/vietnamworks/year=2026/month=05/day=12/clean_bronze_20260512_170702.parquet
```

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
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy `.env.example` to `.env` and fill in required values:

```bash
cp .env.example .env
```

Required variables:
- `ITVIEC_USERNAME` / `ITVIEC_PASSWORD` - for ITviec crawler login
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` - for AWS S3

### 3. Provision S3 Buckets

In the AWS Console, create the two buckets referenced by
[`src/storage_layer/MinIO_S3/config/bucket.yml`](src/storage_layer/MinIO_S3/config/bucket.yml)
(by default `bronze` and `silver`; rename in the YAML if those names are already
taken globally on S3). The IAM user behind your AWS keys needs
`s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on both.

### 4. Run Crawlers

```bash
# TopCV (HTTP-based, no browser needed)
python -m src.crawl_layer.crawler.topcv --keyword "data" --max-pages 2

# VietnamWorks (browser-based)
python -m src.crawl_layer.crawler.vietnamworks --keyword "data analyst" --max-pages 2 [--headless]

# ITviec (browser-based, requires .env login credentials)
python -m src.crawl_layer.crawler.itviec --keyword "data" --max-pages 2 [--headless]
```

Crawled data is appended to daily temp files at `src/crawl_layer/temp_data/<source>_jobs_YYYYMMDD.jsonl`.

### 5. Upload to Bronze

```bash
python -m src.storage_layer.MinIO_S3.layer.bronze.main
```

This uploads all temp JSONL files to the S3 Bronze bucket (compressed as `.jsonl.gz`) and then clears the local `temp_data` directory.

### 6. Validate Temp Data (Optional)

Before Bronze upload, you can validate the local temp JSONL files:

```bash
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.topcv_validate
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.itviec_validate
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.vnworks_validate
```

> Note: These scripts require `pandas` and `great_expectations`, which are not pinned in `requirements.txt`.

### 7. Run Silver Cleaning

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

Common CLI flags (via [`build_argument_parser`](src/storage_layer/MinIO_S3/layer/silver/cleaning/common/pipeline.py:27)):
- `--from_date` (required) - Inclusive start date, format `YYYY-MM-DD`
- `--to_date` (required) - Inclusive end date, format `YYYY-MM-DD`
- `--entity_name` (default: `jobs`)
- `--no_save` - Dry run: skip Parquet upload, just log cleaned row count

## Pipeline Behavior

- **Crawlers append** to the same daily temp file (`<source>_jobs_YYYYMMDD.jsonl`). Repeated runs accumulate data until the Bronze loader processes and clears `temp_data`.
- **Bronze upload** partitions data by date: `source/jobs/year=YYYY/month=MM/day=DD/source_jobs_YYYYMMDD_HHMMSS.jsonl.gz`.
- **Silver cleaning** reads Bronze day-by-day within the specified date range, applies site-specific and common cleaning functions, then uploads cleaned Parquet to the Silver bucket.
- **Silver upload** path: `jobs/source/year=YYYY/month=MM/day=DD/clean_bronze_TIMESTAMP.parquet`.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.13 |
| Data Processing | Polars |
| Browser Automation | nodriver (VietnamWorks, ITviec) |
| HTTP Client | curl_cffi, aiohttp, requests (TopCV) |
| HTML Parsing | lxml, parsel |
| Object Storage | AWS S3 via boto3 |
| Keyword Extraction | flashtext (HybridKeywordExtractor) |
| Containerization | Docker Compose |
| License | Apache 2.0 |

## Implementation Status

| Layer | Status |
|-------|--------|
| Crawl Layer (TopCV, VietnamWorks, ITviec) | Implemented |
| Bronze Layer (S3 upload) | Implemented |
| Local Temp Validation | Implemented (requires unpinned deps) |
| Silver Layer (cleaning + Parquet upload) | Implemented |
| Supabase (OLTP serving) | Not implemented |
| MotherDuck (OLAP serving) | Not implemented |
| Next.js (Job Search Web) | Not implemented |
| BI Serving Layer | Not implemented |
| Orchestrator Layer | Not implemented |
| Recommend Layer | Not implemented |
| Monitoring Layer | Not implemented |

## Notes

- Code uses absolute `src.*` imports; `src/` has no package `__init__.py`. All commands must run from the repo root.
- There is no pytest/lint/format/typecheck config. Files under `src/**/test/` are ad-hoc scripts, not formal test suites.
- The `requirements.txt` is the only pinned dependency source. Validation scripts additionally require `pandas` and `great_expectations`.