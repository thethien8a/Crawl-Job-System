"""Silver DAG for VietnamWorks."""
# Airflow DAG
from _dag_factory import create_silver_dag

dag = create_silver_dag("vietnamworks")
