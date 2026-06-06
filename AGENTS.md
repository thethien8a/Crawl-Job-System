# Lakehouse-Lite — Agent Notes

Medallion pipeline (crawl → Bronze S3 → Silver S3 → Supabase) orchestrated by Airflow with DockerOperator. Python-only, no `src/__init__.py`, all code uses absolute `src.*` imports.

## Working directory & commands

- Always `cd` to the **repo root** before running anything; `src/` has no `__init__.py`, so `python -m src.…` only resolves from here. The Dockerfile sets `PYTHONPATH=/app` to mirror this inside the image.
- Activate `venv/` (Windows: `venv\Scripts\activate`) and `pip install -r requirements.txt` for local work. Outside that, prefer Docker — the Airflow DAGs run every step as a DockerOperator child container, never in-process.
- No `pytest`, `ruff`, `mypy`, `pre-commit`, or CI. The `**/test/` directories hold ad-hoc runnable scripts, not a real test suite. Verify changes by running the relevant entry point with `--no_save` (silver) or a tiny `--max-pages 1` (crawlers).

## End-to-end pipeline order

1. Crawl → appends to `src/crawl_layer/temp_data/<source>_jobs_YYYYMMDD.jsonl` (file is **appended, not overwritten**; re-running the same day accumulates).
2. Validate (optional) → `python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.<topcv|itviec|vnworks>_validate`.
3. Bronze → `python -m src.storage_layer.MinIO_S3.layer.bronze.main [--source topcv|itviec|vietnamworks]`. Compresses temp `.jsonl` → `.jsonl.gz` and uploads, then **clears** the source's temp files.
4. Silver → per-site `python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_<topcv|itviec|vnworks>.main_process --from_date YYYY-MM-DD --to_date YYYY-MM-DD [--entity_name jobs] [--no_save] [--export_parquet]`. `--no_save` is a dry run; `--export_parquet` dumps to `src/storage_layer/MinIO_S3/layer/silver/debug_output/`.
5. Supabase → `python -m src.storage_layer.Supabase.scripts.load_silver_to_supabase --from_date … --to_date …`. Idempotent: it runs `CREATE TABLE IF NOT EXISTS` on every run, then UPSERTs on `job_url`.

## Crawlers (`src/crawl_layer/crawler/<site>/`)

- All take `--keyword` (default `"data"`) and `--max-pages` (default `2`). Browser-based sites also take `--headless` (default is **windowed**; the flag flips to hidden).
- TopCV is pure HTTP/async (`aiohttp` + `curl_cffi`, concurrency 5, 4–6 s delay) — no Chrome, fastest of the three.
- ITviec and VietnamWorks use `nodriver` against real Chrome. They **must** run under `xvfb-run` in containers; the Airflow DAG already wraps them in `xvfb-run --server-args='-screen 0 1280x1024x24'`.
- ITviec reads `ITVIEC_USERNAME` / `ITVIEC_PASSWORD` from `.env` (loaded by `dotenv` in `src/storage_layer/MinIO_S3/config/key.py` at import — which means *any* module transitively importing AWS keys also pulls in the env).
- ITviec's `__main__.py` contains a Windows-only `asyncio` `ProactorEventLoop` `__del__` patch to silence noisy tracebacks on exit. Don't remove it.

## Storage layer quirks

- Bucket names are **hardcoded** in `src/storage_layer/MinIO_S3/config/bucket.yml` (`thethien-lakehouse-lite-bronze`, `thethien-lakehouse-lite-silver`). If you fork or share an AWS account, rename them — they are globally unique on S3.
- Bronze S3 key: `source/jobs/year=YYYY/month=MM/day=DD/<source>_jobs_YYYYMMDD_HHMMSS.jsonl.gz`.
- Silver S3 key (from `SilverBucketPaths.get_file_key`): `jobs/source_site=<site>/year=YYYY/month=MM/day=DD/clean_bronze_<timestamp>.parquet` — note **`source_site=`**, not `source/` (the README diagram is slightly off here).
- The `MinIO_S3` folder name is **legacy** — it talks to real AWS S3 via `boto3`, not MinIO.
- The Silver schema is derived from the `SilverJobItem` dataclass in `src/storage_layer/MinIO_S3/layer/silver/data_model/data_class.py:8` via `silver_schema_to_polars()`. Add/remove a field there and the whole pipeline picks it up; do not hand-edit schema strings elsewhere.
- `enforce_silver_schema` (`src/storage_layer/MinIO_S3/layer/silver/cleaning/common/pipeline.py:34`) bridges `String → Boolean` through `Int64` because Polars can't cast directly. Keep this when touching cast logic.

## Silver cleaning order matters

In `clean_<site>_jobs(df)` the order is load-bearing:

- `drop_unecessary_cols(df)` must run first.
- `clean_location` and `apply_industry_cleaning` **drop the original `location` / `job_industry` columns** — anything downstream must not reference them. Don't reorder cleaners without checking what each one drops.

## Airflow (`src/orchestration_layer/`)

- Boot: `docker compose --project-directory . -f src/orchestration_layer/docker-compose.yaml up -d` (webserver on `:8080`).
- Per-site DAG files (`crawl_topcv.py`, `silver_topcv.py`, …) are **1-line wrappers** around `_dag_factory.py`. Do not put business logic in them; add a new entry to `SITE_CONFIGS` and three thin files if you add a new site.
- `_dag_factory.py` is the single source of pipeline shape. Schedules: `crawl` `0 */3 * * *`, `silver` `0 */8 * * *`, `supabase_load_all` `0 */6 * * *`.
- The factory reads `HOST_REPO_PATH` (absolute path to this repo on the **host** filesystem) and `PIPELINE_IMAGE` from the Airflow container's env. Without `HOST_REPO_PATH` the bind mounts `temp_data/` and `.env` will fail silently.
- Crawl DAGs trigger `validate_bronze_<site>` via `TriggerDagRunOperator` with `wait_for_completion=False` — validation/bronze upload is decoupled.
- `shm_size=2GB` on DockerOperator is required for Chrome; do not lower it.

## Environment

- `.env` is git-ignored; copy from `.env.example`. Required keys for local crawlers + bronze/silver: `ITVIEC_USERNAME`, `ITVIEC_PASSWORD`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` (default `ap-southeast-1`). For Airflow: also `FERNET_KEY`, `WEBSERVER_SECRET_KEY`, `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD`, `HOST_REPO_PATH`, `PIPELINE_IMAGE`, `AIRFLOW_UID` (default `50000`).
- `IS_DOCKER=1` is set in the Dockerfile so any module can branch on it.

## Git-ignored artifacts (intentional, do not commit)

`temp_data/`, `debug_output/`, `*.parquet`, `*.jsonl`, `*.json`, `*.csv`, `*.xlsx`, `venv/`, `src/orchestration_layer/logs/`, `.env`, `.serena/`. Treat them as local-only.

## Dockerfile gotcha

The image is `python:3.11-slim` even though the README says Python 3.13. Don't change one without the other. `CHROME_BIN=/usr/bin/google-chrome` is set explicitly because nodriver's `find_chrome_executable` is unreliable inside DockerOperator-spawned containers.
