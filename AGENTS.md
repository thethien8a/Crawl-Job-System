# AGENTS.md

## Build / Run / Test
- Setup: `python -m venv venv && venv\Scripts\activate && pip install -r requirements.txt`. Copy `.env.example` to `.env` and fill credentials (ITVIEC_*, LINKEDIN_*).
- Run a crawler: `python -m src.crawl_layer.crawler.<site> --keyword "data" --max-pages 2 [--no-headless]` (sites: `itviec`, `linkedin`, `topcv`, `vietnamworks`).
- Run Bronze→Silver ETL: `python -m src.storage_layer.MinIO_S3.layer.silver.etl_bronze_to_silver`. Bronze loader: `python -m src.storage_layer.MinIO_S3.layer.bronze.main`.
- No pytest suite is configured. Ad-hoc scripts live in `src/crawl_layer/test/`; run individually e.g. `python src/crawl_layer/test/test_vnwork.py`. There is no lint/format config (no ruff/black/mypy); match existing style.
-

## Architecture
- Lakehouse-Lite job-board pipeline. Two layers: `src/crawl_layer/` (scrapers) and `src/storage_layer/MinIO_S3/` (Bronze=raw JSONL.gz, Silver=cleaned Parquet partitioned by source/year/month).
- Each crawler under `src/crawl_layer/crawler/<site>/` follows the same module split: `browser.py` (nodriver session/login), `parser.py` (HTML→dataclass), `crawler.py` (orchestration), `config.py` (selectors/URLs), `__main__.py` (argparse CLI). `topcv` uses `http_client.py` (curl_cffi) instead of `browser.py`.
- Shared dataclasses: `src/crawl_layer/data_model/data_class.py` (`JobItem` base + per-site subclasses). Loader writes JSONL to `TEMP_DIR` then uploads gzipped to MinIO via `src/crawl_layer/utils/loader.py` and `src/storage_layer/MinIO_S3/utils/minio_connect.py`.
- Silver layer uses Polars + s3fs; cleaning helpers in `layer/silver/cleaning/common/`, manifest dedup via `utils/manifest.py`. MinIO endpoint/keys come from env (`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`).
- OLAP (Gold Layer) will be stored in Clickhouse (Get data from Silver layer), OLTP will be stored in Supabase (Get data from Silver layer). Silver Layer, Bronze Layer is stored in MinIO is source of truth

## Code Style
- Python 3.13, `from __future__ import annotations` at top of new modules. Imports ordered: stdlib, third-party, then absolute `src.crawl_layer...` / `src.storage_layer...`, then relative (`.browser`).
- Naming: `snake_case` functions/vars, `PascalCase` classes (suffix `JobItem`, `Crawler`, `Browser`, `Parser`, `Error`), `UPPER_SNAKE` constants in `config.py`.
- Types: prefer `str | None` (PEP 604); `Optional[T]` is tolerated for consistency with existing dataclass fields. Use `@dataclass` for data containers and `dataclasses.asdict` before serialization.
- Logging: `logger = logging.getLogger(__name__)` per module; CLIs configure `logging.basicConfig` in `__main__`. No `print` in library code.
- Errors: define site-specific exceptions (e.g. `ItviecLoginError`); catch narrowly and log with context. Never use bare `except:`.
- Comments in English, explain WHY not WHAT. No emoji in code. Paths via `pathlib.Path` (see `src/crawl_layer/config/path.py`). Secrets only via `.env` / `os.getenv`, never hardcoded.
