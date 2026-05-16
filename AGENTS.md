# AGENTS.md

## Build / Run / Test
- **Setup:** `python -m venv venv && venv\Scripts\activate && pip install -r requirements.txt`. Copy `.env.example` to `.env` and fill in required credentials (`ITVIEC_*`, `LINKEDIN_*`, `MINIO_*`).
- **Run a crawler:** `python -m src.crawl_layer.crawler.<site> --keyword "data" --max-pages 2 [--no-headless]` (supported sites: `itviec`, `linkedin`, `topcv`, `vietnamworks`).
- **Run Bronze loader:** `python -m src.storage_layer.MinIO_S3.layer.bronze.main` (zips `TEMP_DIR` contents to `.jsonl.gz` and uploads to MinIO bronze bucket).
- **Silver ETL:** Silver layer ETL (`src/storage_layer/MinIO_S3/layer/silver/`) is heavily under construction. Empty files like `main_process.py` in `cleaning/clean_<site>/` exist but are not yet functional.
- **Testing:** No pytest suite is configured. Ad-hoc scripts live in `src/crawl_layer/test/`; run individually (e.g., `python src/crawl_layer/test/test_vnwork.py`).
- **Lint/Format:** There is no lint/format config (no ruff/black/mypy). Match existing project style.

## Architecture
- Lakehouse-Lite is a job-board pipeline with two main directories: `src/crawl_layer/` (scrapers) and `src/storage_layer/MinIO_S3/` (Bronze=raw JSONL.gz, Silver=cleaned Parquet partitioned by source/year/month).
- **Crawlers (`src/crawl_layer/crawler/<site>/`):** 
  - Standard module split: `browser.py` (nodriver session/login), `parser.py` (HTML→dataclass), `crawler.py` (orchestration), `config.py` (selectors/URLs), `__main__.py` (CLI). 
  - *Exception:* `topcv` uses `http_client.py` (via `curl_cffi`) instead of `browser.py`.
- **Shared Data Models:** Uses dataclasses in `src/crawl_layer/data_model/data_class.py` (`JobItem` base + per-site subclasses).
- **Bronze Loader:** Crawlers write JSONL to `TEMP_DIR`. The loader compresses these and uploads them to MinIO (`utils/loader.py`). MinIO endpoint/keys come from environment variables.
- **Storage Vision:** MinIO is the source of truth for Bronze and Silver layers. Future OLAP (Gold) in Clickhouse and OLTP in Supabase will be populated from the Silver layer.

## Code Style
- **Python 3.13:** Use `from __future__ import annotations` at the top of new modules. 
- **Imports:** Order by stdlib, third-party, absolute project imports (`src.crawl_layer...` / `src.storage_layer...`), then relative imports (`.browser`).
- **Naming Conventions:** `snake_case` functions/vars, `PascalCase` classes (suffix `JobItem`, `Crawler`, `Browser`, `Parser`, `Error`), `UPPER_SNAKE` constants in `config.py`.
- **Typing:** Prefer PEP 604 unions like `str | None` over `Optional`. Use `@dataclass` for data containers and `dataclasses.asdict` before serialization.
- **Logging:** Use `logger = logging.getLogger(__name__)` per module. CLI entry points (`__main__.py`) configure `logging.basicConfig`. No `print()` calls in library code.
- **Errors:** Define site-specific exceptions (e.g., `ItviecLoginError`); catch narrowly and log with context. Never use bare `except:`.
- **Comments & Hardcoding:** Explain WHY, not WHAT. No emoji in code. Use `pathlib.Path` for paths (see `src/crawl_layer/config/path.py`). Load secrets via `.env` / `os.getenv` only.

## OS Quirks & Gotchas
- **Windows Asyncio with nodriver:** Due to Windows `ProactorEventLoop` cleanup issues, CLI runners include a patch overriding `__del__` on transport pipes (see `__main__.py` in crawlers).
- **nodriver Warnings:** To suppress noisy KeyError warnings for unrecognized CDP events, set `logging.getLogger("nodriver.core.connection").setLevel(logging.ERROR)` in runner scripts.
