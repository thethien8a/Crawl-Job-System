"""Silver DAG for ITviec."""
# Airflow DAG
from _dag_factory import create_silver_dag

dag = create_silver_dag("itviec")
