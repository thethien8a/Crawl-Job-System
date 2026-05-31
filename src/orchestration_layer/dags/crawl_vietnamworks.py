"""Crawl DAG for VietnamWorks.com."""
# Airflow DAG
from _dag_factory import create_crawl_dag

dag = create_crawl_dag("vietnamworks")