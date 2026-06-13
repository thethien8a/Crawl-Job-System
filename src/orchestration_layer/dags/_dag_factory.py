"""Shared helpers for per-site DAG factories.

Every DAG in this package runs business logic inside sibling ``lakehouse-pipeline``
containers via DockerOperator. Airflow only orchestrates; it never imports the
crawler / storage / Supabase modules directly.
"""

from __future__ import annotations
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv
from typing import Any
from datetime import datetime, timedelta
import os
from docker.types import Mount
from dotenv import load_dotenv

load_dotenv()

# Config 
SITE_CONFIGS: dict[str, dict[str, str]] = {
    "topcv": {
        "crawl_module": "topcv",
        "validate_module": "topcv_validate",
        "bronze_source": "topcv",
        "silver_module": "clean_topcv",
        "supabase_site": "topcv",
    },
    "vietnamworks": {
        "crawl_module": "vietnamworks",
        "validate_module": "vnworks_validate",
        "bronze_source": "vietnamworks",
        "silver_module": "clean_vnworks",
        "supabase_site": "vietnamworks",
    },
    "itviec": {
        "crawl_module": "itviec",
        "validate_module": "itviec_validate",
        "bronze_source": "itviec",
        "silver_module": "clean_itviec",
        "supabase_site": "itviec",
    },
}

MID_NIGHT_SCHEDULE = "0 0 * * *"
CLEAN_SCHEDULE = "0 */8 * * *"
LOAD_SUPABASE_SCHEDULE = "0 */6 * * *"
CRAWL_SCHEDULE = "0 */3 * * *"
CRAWL_KEYWORD = "data"
CRAWL_MAX_PAGES = 2
START_DATE = datetime(2026, 1, 1)

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

HOST_REPO_PATH = os.getenv("HOST_REPO_PATH")
PIPELINE_IMAGE = os.getenv("PIPELINE_IMAGE", "lakehouse-crawler:latest")

TEMP_DATA_MOUNT = Mount(
    source=f"{HOST_REPO_PATH}/src/crawl_layer/temp_data",
    target="/app/src/crawl_layer/temp_data",
    type="bind",
)

ENV_FILE_MOUNT = Mount(
    source=f"{HOST_REPO_PATH}/.env",
    target="/app/.env",
    type="bind",
    read_only=True,
)

COMMON_MOUNTS = [TEMP_DATA_MOUNT, ENV_FILE_MOUNT]

COMMON_DOCKER_KWARGS: dict[str, Any] = dict(
    image=PIPELINE_IMAGE,
    mounts=COMMON_MOUNTS,
    # No network_mode override: default Docker bridge gives outbound
    # internet access, which is all we need to reach AWS S3.
    auto_remove="success",
    mount_tmp_dir=False,
    docker_url="unix:///var/run/docker.sock",
    shm_size=2 * 1024 * 1024 * 1024,  # 2 GB shared memory for Chrome in Docker
)


# Code run
def create_crawl_dag(site: str) -> DAG:
    """Return a DAG that crawls one site.

    Overridable via Airflow UI *Trigger DAG w/ config* (JSON):
        {"keyword": "data engineer", "max_pages": 5}
    """
    cfg = SITE_CONFIGS[site]
    dag_id = f"crawl_{site}"

    with DAG(
        dag_id=dag_id,
        description=f"Crawl {site} jobs -> local temp JSONL (DockerOperator)",
        schedule=CRAWL_SCHEDULE,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        params={
            "keyword": CRAWL_KEYWORD,
            "max_pages": CRAWL_MAX_PAGES,
        },
        tags=["lakehouse", "crawl", site],
    ) as dag:

        crawl = DockerOperator(
            task_id=f"crawl_{site}",
            # Wrap with `sh -c` so xvfb-run isn't PID 1 inside the container.
            command=[
                "sh",
                "-c",
                (
                    "xvfb-run --server-args='-screen 0 1280x1024x24' "
                    f"python -m src.crawl_layer.crawler.{cfg['crawl_module']} "
                    "--keyword {{ params.keyword }} "
                    "--max-pages {{ params.max_pages }}"
                ),
            ],
            **COMMON_DOCKER_KWARGS,
        )

        trigger_validate = TriggerDagRunOperator(
            task_id=f"trigger_validate_bronze_{site}",
            trigger_dag_id=f"validate_bronze_{site}",
            # Don't block crawl DAG waiting for validate to finish.
            wait_for_completion=False,
        )

        crawl >> trigger_validate

    return dag


def create_validate_bronze_dag(site: str) -> DAG:
    """Return a DAG that validates local temp data then uploads to Bronze.

    Validation failure stops the pipeline for this site so corrupt data
    never reaches the Bronze bucket on S3.
    """
    cfg = SITE_CONFIGS[site]
    dag_id = f"validate_bronze_{site}"

    with DAG(
        dag_id=dag_id,
        description=f"Validate {site} temp JSONL -> upload Bronze (DockerOperator)",
        schedule=None,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["lakehouse", "validate", "bronze", site],
    ) as dag:

        validate = DockerOperator(
            task_id=f"validate_{site}",
            command=(
                "python -m "
                "src.storage_layer.MinIO_S3.layer.local_temp.validation"
                f".{cfg['validate_module']}"
            ),
            **COMMON_DOCKER_KWARGS,
        )

        bronze_upload = DockerOperator(
            task_id=f"bronze_{site}",
            command=(
                "python -m src.storage_layer.MinIO_S3.layer.bronze.main "
                f"--source {cfg['bronze_source']}"
            ),
            **COMMON_DOCKER_KWARGS,
        )

        validate >> bronze_upload

    return dag


def create_silver_dag(site: str) -> DAG:
    """Return a DAG that cleans Bronze -> Silver."""
    cfg = SITE_CONFIGS[site]
    dag_id = f"silver_{site}"
    today = datetime.now().strftime("%Y-%m-%d")

    with DAG(
        dag_id=dag_id,
        description=f"Silver clean for {site} (DockerOperator)",
        schedule=CLEAN_SCHEDULE,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        params={
            "from_date": today,
            "to_date": today,
        },
        tags=["lakehouse", "silver", site],
    ) as dag:

        silver = DockerOperator(
            task_id=f"silver_{site}",
            command=(
                "python -m src.storage_layer.MinIO_S3.layer.silver.cleaning"
                f".{cfg['silver_module']}.main_process "
                "--from_date {{ params.from_date }} --to_date {{ params.to_date }}"
            ),
            **COMMON_DOCKER_KWARGS,
        )

    return dag

def create_supabase_dag() -> DAG:
    """Return a DAG that loads all Silver data into Supabase."""
    dag_id = "supabase_load_all"
    today = datetime.now().strftime("%Y-%m-%d")

    with DAG(
        dag_id=dag_id,
        description="Load all Silver data into Supabase (DockerOperator)",
        schedule=LOAD_SUPABASE_SCHEDULE,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        params={
            "from_date": today,
            "to_date": today,
        },
        tags=["lakehouse", "supabase", "all_sites"],
    ) as dag:

        supabase = DockerOperator(
            task_id="supabase_load_all",
            command=(
                "python -m src.storage_layer.Supabase.scripts.load_silver_to_supabase "
                "--from_date {{ params.from_date }} --to_date {{ params.to_date }}"
            ),
            **COMMON_DOCKER_KWARGS,
        )

    return dag

def create_load_silver_to_gold_dag() -> DAG:
    """Return a DAG that loads all Silver data into MotherDuck Gold layer.

    Schedule mirrors ``supabase_load_all`` so Gold stays current after Silver runs.
    """
    dag_id = "load_silver_to_gold"

    with DAG(
        dag_id=dag_id,
        description="Load Silver parquet data into MotherDuck Gold layer (DockerOperator)",
        schedule=LOAD_SUPABASE_SCHEDULE,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["lakehouse", "motherduck", "gold"],
    ) as dag:

        load_silver_to_gold = DockerOperator(
            task_id="load_silver_to_gold",
            command="python -m src.storage_layer.MotherDuck.scripts.load_silver_to_gold",
            **COMMON_DOCKER_KWARGS,
        )

    return dag


def create_load_taxonomy_to_gold_dag() -> DAG:
    """Return a DAG that loads taxonomy CSVs into MotherDuck Gold dimension tables.

    Manual trigger only — taxonomy seeds change rarely.
    """
    dag_id = "load_taxonomy_to_gold"

    with DAG(
        dag_id=dag_id,
        description="Load taxonomy CSV seeds into MotherDuck Gold dim tables (DockerOperator)",
        schedule=None,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["lakehouse", "motherduck", "gold", "taxonomy"],
    ) as dag:

        load_taxonomy_to_gold = DockerOperator(
            task_id="load_taxonomy_to_gold",
            command="python -m src.storage_layer.MotherDuck.scripts.load_taxonomy_to_gold",
            **COMMON_DOCKER_KWARGS,
        )

    return dag

def create_cluster_company_name_dag() -> DAG:
    """Return a DAG that clusters company names using fuzzy matching.

    Manual trigger only — pass date range via *Trigger DAG w/ config*.
    """
    dag_id = "cluster_company_name"
    today = datetime.now().strftime("%Y-%m-%d")

    with DAG(
        dag_id=dag_id,
        description="Cluster company names using fuzzy matching (DockerOperator)",
        schedule=None,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        params={
            "from_date": today,
            "to_date": today,
        },
        tags=["lakehouse", "fuzzy", "cluster", "company_name"],
    ) as dag:

        cluster_company_name = DockerOperator(
            task_id="cluster_company_name",
            command=(
                "python -m src.storage_layer.MinIO_S3.layer.silver.utils.script_fuzzy "
                "--from_date {{ params.from_date }} --to_date {{ params.to_date }}"
            ),
            **COMMON_DOCKER_KWARGS,
        )

    return dag


def create_bronze_dashboard_dag() -> DAG:
    """Return a DAG that generates the Bronze business dashboard.

    Runs at midnight to generate a static HTML report from S3 Bronze objects.
    """
    dag_id = "generate_bronze_dashboard"

    with DAG(
        dag_id=dag_id,
        description="Generate Bronze business dashboard HTML (DockerOperator)",
        schedule=MID_NIGHT_SCHEDULE,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["lakehouse", "dashboard", "bronze", "monitoring"],
    ) as dag:

        generate_bronze_dashboard = DockerOperator(
            task_id="generate_bronze_dashboard",
            command="python -m src.monitoring_layer.business.bronze_dashboard",
            **COMMON_DOCKER_KWARGS,
        )

    return dag


def create_silver_dashboard_dag() -> DAG:
    """Return a DAG that generates the Silver business dashboard.

    Runs at midnight to generate a static HTML report from S3 Silver data.
    """
    dag_id = "generate_silver_dashboard"

    with DAG(
        dag_id=dag_id,
        description="Generate Silver business dashboard HTML (DockerOperator)",
        schedule=MID_NIGHT_SCHEDULE,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["lakehouse", "dashboard", "silver", "monitoring"],
    ) as dag:

        generate_silver_dashboard = DockerOperator(
            task_id="generate_silver_dashboard",
            command="python -m src.monitoring_layer.business.silver_dashboard",
            **COMMON_DOCKER_KWARGS,
        )

    return dag