# AGENTS.md

## Project Target
- Data lakehouse for Vietnamese job postings from TopCV, VietnamWorks, and ITviec, focused on data roles; executable flow is crawlers -> local JSONL temp -> MinIO Bronze -> MinIO Silver Parquet.
- Supabase, ClickHouse, Next.js, BI, orchestration, monitoring, and recommendation layers are placeholders or planned only.

## Commands
- Run Python module commands from the repo root; imports assume `src.*` and `src/` has no `__init__.py`.
- Setup: `python -m venv venv`, `venv\Scripts\activate`, then `pip install -r requirements.txt`; `requirements.txt` is the only dependency manifest and there is no lockfile or `pyproject.toml`.
- Start MinIO: `docker compose --env-file .env -f src/storage_layer/MinIO_S3/docker-compose.yaml up -d`; add `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` to `.env` because `.env.example` only contains ITviec login vars, while Python falls back to `minio` / `minio123`.
- Crawl TopCV: `python -m src.crawl_layer.crawler.topcv --keyword "data" --max-pages 2`.
- Crawl VietnamWorks: `python -m src.crawl_layer.crawler.vietnamworks --keyword "data analyst" --max-pages 2 [--headless]`.
- Crawl ITviec: `python -m src.crawl_layer.crawler.itviec --keyword "data" --max-pages 2 [--headless]`; requires `ITVIEC_USERNAME` and `ITVIEC_PASSWORD` in the root `.env`.
- Validate the newest local temp JSONL before Bronze upload: `python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.topcv_validate`, `...itviec_validate`, or `...vnworks_validate`.
- Upload all local temp JSONL to Bronze and clear temp data: `python -m src.storage_layer.MinIO_S3.layer.bronze.main`.
- Run Silver cleaning: `python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_<site>.main_process --from_date YYYY-MM-DD --to_date YYYY-MM-DD`; valid site dirs are `clean_topcv`, `clean_itviec`, and `clean_vnworks`.
- Silver flags: `--no_save` dry-runs without MinIO upload; `--export_parquet` writes local debug Parquet under `src/storage_layer/MinIO_S3/layer/silver/cleaning/debug_output/`.
- There is no pytest, lint, formatter, or typecheck config; files under `src/**/test/` are ad-hoc scripts, not a formal test suite.

## Pipeline Gotchas
- Crawlers append to `src/crawl_layer/temp_data/<source>_jobs_YYYYMMDD.jsonl`; repeated runs accumulate rows until Bronze upload clears the directory.
- Bronze paths are `<source>/jobs/year=YYYY/month=MM/day=DD/<source>_jobs_YYYYMMDD_HHMMSS.jsonl.gz`; bucket names come from `src/storage_layer/MinIO_S3/config/bucket.yml`.
- Silver writer stores Parquet as `jobs/<source>/year=YYYY/month=MM/day=DD/clean_bronze_TIMESTAMP.parquet`; `SilverBucketPaths` currently formats `<source>/jobs/...`, so verify reader/writer path assumptions before relying on Silver reads.
- VietnamWorks uses `vietnamworks` for temp/Bronze/Silver path prefixes, but JSON rows use `source_site="vietnamworks.com"`.
- Silver cleaners read Bronze one day at a time across the inclusive date range and skip days with no Bronze objects.
- ITviec selectors are intentionally noisy because class names are partially hashed; do not simplify them without testing the live page.
