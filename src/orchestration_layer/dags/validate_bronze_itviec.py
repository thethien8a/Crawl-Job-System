"""Validate + Bronze DAG for ITviec.com."""
# Airflow DAG
from _dag_factory import create_validate_bronze_dag

dag = create_validate_bronze_dag("itviec")