# AGENTS.md

This file provides guidance to agents when working with code in this repository.

# Lakehouse-Lite — Agent Notes

## Pipeline stages (must run in order)

1. **Crawl** → appends to `src/crawl_layer/temp_data/<source>_jobs_YYYYMMDD.jsonl` (append-only; re-run same day accumulates).
2. **Validate (optional)** → `python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.<topcv|itviec|vnworks>_validate`.
3. **Bronze** → `python -m src.storage_layer.MinIO_S3.layer.bronze.main [--source topcv|itviec|vietnamworks]`. Gzips temp JSONL → uploads to S3, then **clears** that source's temp files.
4. **Silver** → per-site `python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_<site>.main_process --from_date YYYY-MM-DD --to_date YYYY-MM-DD [--no_save] [--export_parquet]`.
5. **Supabase** → `python -m src.storage_layer.Supabase.scripts.load_silver_to_supabase --from_date … --to_date …` (UPSERT on `job_url`). Maps cleaned columns back to raw names for frontend compatibility (e.g. `clean_job_title` → `job_title`).
6. **MotherDuck (Gold)** → `python -m src.storage_layer.MotherDuck.scripts.load_silver_to_gold` (DuckDB reads S3 parquet via hive partitioning, no intermediate Polars/boto3 layer).

## Critical gotchas

- `src/` has **no `__init__.py`** — all imports use absolute `src.*`; always `cd` to repo root before `python -m`. Dockerfile sets `PYTHONPATH=/app` to mirror this.
- Bucket names come from `S3_BRONZE_BUCKET` and `S3_SILVER_BUCKET` in `.env` — they must be globally unique on S3.
- `MinIO_S3` folder name is **legacy** — talks to real AWS S3 via `boto3`, not MinIO. The `get_s3_client()` in `minio_connect.py` is the single S3 client factory.
- Silver schema is derived from `SilverJobItem` dataclass in `data_model/data_class.py` — add/remove fields there, not in hand-edited schema strings. `silver_schema_to_polars()` auto-generates the Polars schema.
- Bronze S3 key uses `source/` prefix; Silver uses `source_site=` — both implemented via `BronzeBucketPaths`/`SilverBucketPaths` in `config/path.py`.
- `.env` is git-ignored; ITviec needs `ITVIEC_USERNAME`/`ITVIEC_PASSWORD`. ITviec and VietnamWorks `__main__.py` have a Windows-only `ProactorEventLoop` `__del__` patch — don't remove it.
- No `pytest`/`ruff`/CI. Verify with `--no_save` (silver) or `--max-pages 1` (crawlers).

## Non-obvious architecture details

- Crawlers use two different stacks: TopCV uses `aiohttp` (no browser), while ITViec/VietnamWorks use `nodriver` (headless Chrome). Only the nodriver-based crawlers need `xvfb-run` and the ProactorEventLoop Windows patch.
- Airflow DAGs are pure orchestrators — all business logic runs inside `lakehouse-crawler` Docker containers via `DockerOperator`. Airflow never imports crawler/storage modules.
- Seed taxonomy files in `silver/seeds/` are loaded from Google Sheets first, falling back to local CSV. Configured via `GOOGLE_SHEETS_CREDENTIALS_FILE` + `GOOGLE_SHEETS_SPREADSHEET_ID` in `.env`.
- Silver reader (`reader.py`) uses `LastModified` to pick only the latest parquet per day partition — re-runs append new files, they don't overwrite.
- Supabase `JobData` dataclass in `Supabase/schema/data_class.py` is the target schema — it maps Silver's cleaned columns back to original names (e.g. `clean_job_title` aliased to `job_title`).
- DAG params are overridable via Airflow UI "Trigger DAG w/ config" JSON: `{"keyword": "...", "max_pages": N}` for crawls, `{"from_date": "...", "to_date": "..."}` for silver/supabase.
- Bronze upload per source is destructive — it gzips temp JSONL files → uploads → deletes both `.gz` and the source's `.jsonl` files locally. Use `--source` to isolate one site.
