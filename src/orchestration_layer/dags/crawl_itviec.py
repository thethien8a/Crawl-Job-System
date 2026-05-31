"""Crawl DAG for ITviec.com."""
# Airflow DAG
from _dag_factory import create_crawl_dag

dag = create_crawl_dag("itviec")