# Lakehouse-Lite

A lightweight data lakehouse that crawls job postings from three Vietnamese recruitment platforms — **TopCV**, **ITviec**, and **VietnamWorks** — with a focus on data roles (Data Engineer, Data Analyst, Data Scientist, AI/ML, BI).

The pipeline follows the **medallion architecture** (Bronze → Silver → Gold) on AWS S3, serves cleaned data to **Supabase** (OLTP, for a job-search frontend) and **MotherDuck** (OLAP, for BI), and is orchestrated end-to-end by **Apache Airflow** running every task inside Docker containers.

## Architecture

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

Everything is scheduled by **Airflow** (`DockerOperator`) — DAGs never import business logic; they only run `python -m …` commands inside the `lakehouse-crawler` image.

## Tech stack

| Layer | Technology |
|---|---|
| Crawling | [nodriver](https://github.com/ultrafunkamsterdam/nodriver) (headless Chrome), `curl_cffi`, `aiohttp`, `parsel` |
| Validation | Great Expectations |
| Bronze / Silver | AWS S3 (`boto3`), Polars, FlashText, RapidFuzz |
| Gold | MotherDuck (DuckDB) — reads S3 parquet via hive partitioning |
| Serving | Supabase (PostgreSQL, `psycopg2`) |
| Taxonomy seeds | Google Sheets (`gspread`) with local CSV fallback |
| Orchestration | Apache Airflow 2.10 + DockerOperator |
| Monitoring | Prometheus, Grafana, StatsD exporter, Caddy, Nginx, Altair dashboards |
| BI | Power BI (`src/bi_report_layer/analysis_dashboard.pbix`) |

## Project structure

```
Lakehouse-Lite/
├── Dockerfile                     # python:3.11-slim + Chrome + xvfb (pipeline image)
├── requirements.txt
├── .env.example                   # copy to .env and fill in
├── init.sh                        # start both stacks locally
├── init_orchestration.sh          # deploy Airflow stack (EC2-A)
├── init_monitoring.sh             # deploy monitoring stack (EC2-B)
├── documents/                     # design docs, deployment & security guides
└── src/
    ├── crawl_layer/
    │   ├── crawler/{topcv,itviec,vietnamworks}/   # one module per site
    │   ├── data_model/data_class.py               # JobItem + per-site dataclasses
    │   └── temp_data/                             # local JSONL staging (git-ignored)
    ├── storage_layer/
    │   ├── MinIO_S3/              # legacy name — talks to real AWS S3
    │   │   └── layer/
    │   │       ├── local_temp/validation/         # Great Expectations checks
    │   │       ├── bronze/main.py                 # gzip + upload + clear temp
    │   │       └── silver/
    │   │           ├── cleaning/{common,clean_topcv,clean_itviec,clean_vnworks}/
    │   │           ├── data_model/data_class.py   # SilverJobItem = source of truth
    │   │           └── seeds/                     # taxonomy CSVs (skills, industries…)
    │   ├── Supabase/scripts/load_silver_to_supabase.py
    │   └── MotherDuck/scripts/{load_silver_to_gold,load_taxonomy_to_gold}.py
    ├── orchestration_layer/
    │   ├── dags/                  # Airflow DAGs (pure orchestrators)
    │   └── docker-compose.yaml    # Airflow + Postgres + statsd-exporter
    ├── monitoring_layer/
    │   ├── docker-compose.yaml    # Caddy + Prometheus + Grafana + Nginx
    │   ├── business/              # Bronze/Silver HTML dashboard generators (Altair)
    │   └── grafana/ prometheus/ nginx/ Caddyfile
    └── bi_report_layer/analysis_dashboard.pbix
```

## Getting started

### Prerequisites

- Python 3.11, Docker + Docker Compose v2
- AWS S3 buckets (bronze + silver), Supabase project, MotherDuck token
- Google Chrome (installed automatically inside the Docker image)

### Setup

```bash
git clone https://github.com/thethien8a/Crawl-Job-System.git
cd Lakehouse-Lite

python -m venv venv
venv/Scripts/activate        # Windows  (Linux/macOS: source venv/bin/activate)
pip install -r requirements.txt

cp .env.example .env         # then fill in credentials
```

Key variables in `.env` (see `.env.example` for the full annotated list):

| Variable | Purpose |
|---|---|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | S3 access |
| `S3_BRONZE_BUCKET` / `S3_SILVER_BUCKET` | bucket names (must be globally unique) |
| `ITVIEC_USERNAME` / `ITVIEC_PASSWORD` | ITviec crawler auto-login |
| `SUPABASE_*` | Supabase Postgres connection |
| `MOTHERDUCK_TOKEN` / `MOTHERDUCK_DATABASE` | Gold layer |
| `GOOGLE_SHEETS_*` | taxonomy seeds source (optional, falls back to local CSV) |
| `FERNET_KEY`, `WEBSERVER_SECRET_KEY`, `_AIRFLOW_WWW_USER_*` | Airflow |

> **Note:** all `python -m` commands must run from the repo root — `src/` has no `__init__.py` and imports are absolute (`src.*`).

## Running the pipeline manually

The stages must run in order:

**1. Crawl** → appends to `src/crawl_layer/temp_data/<source>_jobs_YYYYMMDD.jsonl`

```bash
python -m src.crawl_layer.crawler.topcv        --keyword "data" --max-pages 2
python -m src.crawl_layer.crawler.itviec       --keyword "data" --max-pages 2
python -m src.crawl_layer.crawler.vietnamworks --keyword "data" --max-pages 2
```

**2. Validate** (optional) — Great Expectations checks on temp JSONL

```bash
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.topcv_validate
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.itviec_validate
python -m src.storage_layer.MinIO_S3.layer.local_temp.validation.vnworks_validate
```

**3. Bronze** — gzip temp JSONL → upload to S3 → clear local temp files

```bash
python -m src.storage_layer.MinIO_S3.layer.bronze.main --source topcv
# omit --source to upload all sources
```

**4. Silver** — per-site cleaning (Polars), writes parquet partitioned by `source_site=/year=/month=/day=`

```bash
python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_topcv.main_process   --from_date 2026-01-01 --to_date 2026-01-07
python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_itviec.main_process  --from_date 2026-01-01 --to_date 2026-01-07
python -m src.storage_layer.MinIO_S3.layer.silver.cleaning.clean_vnworks.main_process --from_date 2026-01-01 --to_date 2026-01-07
# add --no_save for a dry run, --export_parquet to dump local debug parquet
```

**5. Supabase** — UPSERT cleaned jobs into Postgres (conflict key: `job_url`)

```bash
python -m src.storage_layer.Supabase.scripts.load_silver_to_supabase --from_date 2026-01-01 --to_date 2026-01-07
```

**6. Gold (MotherDuck)** — DuckDB reads Silver parquet straight from S3 and builds fact/dim tables

```bash
python -m src.storage_layer.MotherDuck.scripts.load_silver_to_gold
python -m src.storage_layer.MotherDuck.scripts.load_taxonomy_to_gold   # seed dimension tables
```

## Running with Airflow (production)

Build the pipeline image and start everything:

```bash
./init_orchestration.sh   # builds lakehouse-crawler:latest + starts Airflow stack
./init_monitoring.sh      # starts Caddy + Prometheus + Grafana + dashboards
# or, for a single local machine:
./init.sh
```

- Airflow UI: `http://localhost:8080`
- Monitoring (via Caddy, Basic Auth): `http://<MONITORING_DOMAIN>/business/` and `/grafana/`

### DAG overview

| DAG | Schedule | What it does |
|---|---|---|
| `crawl_topcv` / `crawl_itviec` / `crawl_vietnamworks` | every 3h (staggered :00/:15/:30) | crawl → trigger validate+bronze DAG |
| `validate_bronze_<site>` | triggered | Great Expectations validate → upload to Bronze |
| `silver_<site>` | every 8h | clean Bronze → Silver parquet |
| `supabase_load_all` | every 6h | Silver → Supabase UPSERT |
| `load_silver_to_gold` | every 6h | Silver → MotherDuck Gold tables |
| `load_taxonomy_to_gold` | manual | seed CSVs → Gold dimension tables |
| `cluster_company_name` | manual | RapidFuzz clustering for company-name canonicalization review |
| `generate_bronze_dashboard` / `generate_silver_dashboard` | daily | render static HTML data-quality dashboards |

DAG params can be overridden per-run via Airflow UI *Trigger DAG w/ config*, e.g. `{"keyword": "python", "max_pages": 5}` for crawls or `{"from_date": "...", "to_date": "..."}` for silver/supabase.

## Deployment

The production setup splits across two EC2 instances sharing an EFS volume:

- **EC2-A (orchestration):** Airflow + Postgres + statsd-exporter + pipeline containers — needs more CPU/RAM (headless Chrome).
- **EC2-B (monitoring):** Caddy (TLS + Basic Auth) + Prometheus + Grafana + Nginx static dashboards.

See [`documents/deploy-two-ec2-efs.md`](documents/deploy-two-ec2-efs.md) and [`documents/security-group-guide.md`](documents/security-group-guide.md) for the full runbook.

## Documentation

| Document | Content |
|---|---|
| [`documents/system-design-report.md`](documents/system-design-report.md) | full architecture & layer design |
| [`documents/system-analysis-report.md`](documents/system-analysis-report.md) | requirements, data sources, data models |
| [`documents/technology-stack-documentation.md`](documents/technology-stack-documentation.md) | every technology used and why |
| [`documents/build-crawl-module-guide.md`](documents/build-crawl-module-guide.md) | how to add a new site crawler |
| [`documents/cv-recommendation-feature.md`](documents/cv-recommendation-feature.md) | planned CV → job recommendation feature (Qdrant) |
| [`AGENTS.md`](AGENTS.md) | pipeline gotchas & notes for AI coding agents |

## Notes & gotchas

- The `MinIO_S3` folder name is **legacy** — it talks to real AWS S3 via `boto3`.
- Bronze upload is **destructive** locally: it deletes the source's temp JSONL after a successful upload.
- The Silver schema is generated from the `SilverJobItem` dataclass — add/remove fields there, never in hand-written schema strings.
- ITviec and VietnamWorks crawlers patch a Windows-only `ProactorEventLoop` teardown bug for nodriver — don't remove it.
- No pytest/CI yet — verify changes with `--no_save` (silver) or `--max-pages 1` (crawlers).

## License

[Apache 2.0](LICENSE)
