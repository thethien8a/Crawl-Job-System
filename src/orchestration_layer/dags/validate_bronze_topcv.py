"""Validate + Bronze DAG for TopCV.vn."""
# Airflow DAG
from _dag_factory import create_validate_bronze_dag

dag = create_validate_bronze_dag("topcv")