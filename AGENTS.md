# AGENTS.md

## Project Main Target:
- Build a data lakehouse pipeline for job postings from multiple sources (TopCV, VietnamWorks, ITviec) mainly for data field like Data Engineer, Data Scientist, Data Analyst, AI/ML Engineer, Business Intelligence, Machine Learning Engineer to Bronze -> Silver (Store in MinIO). Supabase for OLTP serving for "job search web", Clickhouse for OLAP

## Commands
- Run commands from the repo root; code uses absolute `src.*` imports and `src/` has no package `__init__.py`.
- Setup: `python -m venv venv && venv\Scripts\activate && pip install -r requirements.txt`. The checked local venv is Python 3.13, but `requirements.txt` is the only pinned dependency source.
- Crawler entrypoints currently present: `python -m src.crawl_layer.crawler.topcv --keyword "data" --max-pages 2`, `python -m src.crawl_layer.crawler.vietnamworks --keyword "data analyst" --max-pages 2 [--headless]`, `python -m src.crawl_layer.crawler.itviec --keyword "data" --max-pages 2 [--headless]`.
- ITviec login requires `ITVIEC_USERNAME` and `ITVIEC_PASSWORD` in the root `.env`; `.env.example` only documents these two vars.
- Start MinIO from the repo root with the root env file: `docker compose --env-file .env -f src/storage_layer/MinIO_S3/docker-compose.yaml up -d`. Compose needs `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY`; Python code defaults to `minio` / `minio123` if they are unset.
- Upload local temp JSONL to Bronze: `python -m src.storage_layer.MinIO_S3.layer.bronze.main`.
- Validate latest temp JSONL by source: `python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.topcv_validate`, `...itviec_validate`, or `...vnworks_validate`. These scripts import `pandas` and `great_expectations`, which are not pinned in `requirements.txt`.
- There is no pytest/lint/format/typecheck config, and `pytest` is not installed in the local venv; files under `src/**/test/` are ad-hoc scripts.

## Pipeline Notes
- Current executable flow is crawler -> `src/crawl_layer/temp_data/*.jsonl` -> Bronze MinIO bucket. Silver cleaning is incomplete; `clean_<site>/main_process.py` and `silver/utils/loader.py` are empty.
- Crawlers append to the same daily temp file named `<source>_jobs_YYYYMMDD.jsonl`; repeated crawler runs append until the Bronze loader uploads and then clears `temp_data`.
- Bronze upload path is `source/jobs/year=YYYY/month=MM/day=DD/source_jobs_YYYYMMDD_HHMMSS.jsonl.gz`; bucket names come from `src/storage_layer/MinIO_S3/config/bucket.yml`.
- `documents/architecture.html` is the intended architecture: crawlers, MinIO Bronze/Silver, then future Supabase/ClickHouse serving. Supabase, ClickHouse, Next.js, and BI apps are not implemented in this repo.