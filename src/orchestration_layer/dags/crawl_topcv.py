"""Crawl DAG for TopCV.vn."""
# Airflow DAG
from _dag_factory import create_crawl_dag

dag = create_crawl_dag("topcv")