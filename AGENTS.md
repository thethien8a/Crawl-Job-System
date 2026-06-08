# AGENTS.md

This file provides guidance to agents when working with code in this repository.

# Lakehouse-Lite — Agent Notes

## Pipeline stages (must run in order)

1. **Crawl** → appends to `src/crawl_layer/temp_data/<source>_jobs_YYYYMMDD.jsonl` (append-only; re-run same day accumulates).
2. **Validate (optional)** → `python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.<topcv|itviec|vnworks>_validate`.
3. **Bronze** → `python -m src.storage_layer.MinIO_S3.layer.bronze.main [--source topcv|itviec|vietnamworks]`. Gzips temp JSONL → uploads to S3, then **clears** that source's temp files.
4. **Silver** → per-site `python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_<site>.main_process --from_date YYYY-MM-DD --to_date YYYY-MM-DD [--no_save] [--export_parquet]`.
5. **Supabase** → `python -m src.storage_layer.Supabase.scripts.load_silver_to_supabase --from_date … --to_date …` (UPSERT on `job_url`).
6. **MotherDuck (Gold)** → `python -m src.storage_layer.MotherDuck.scripts.load_silver_to_gold` (DuckDB reads S3 parquet via hive partitioning).

## Critical gotchas

- `src/` has **no `__init__.py`** — all imports use absolute `src.*`; always `cd` to repo root before `python -m`. Dockerfile sets `PYTHONPATH=/app` to mirror this.
- Bucket names are **hardcoded** in `src/storage_layer/MinIO_S3/config/bucket.yml` — globally unique on S3; rename if sharing an AWS account.
- `MinIO_S3` folder name is **legacy** — talks to real AWS S3 via `boto3`, not MinIO.
- Silver schema is derived from `SilverJobItem` dataclass — add/remove fields there, not in hand-edited schema strings.
- Bronze S3 key uses `source/` prefix; Silver uses `source_site=` (README diagram is slightly off on Silver).
- `.env` is git-ignored; ITviec needs `ITVIEC_USERNAME`/`ITVIEC_PASSWORD`. ITviec's `__main__.py` has a Windows-only `ProactorEventLoop` `__del__` patch — don't remove it.
- No `pytest`/`ruff`/CI. Verify with `--no_save` (silver) or `--max-pages 1` (crawlers).
