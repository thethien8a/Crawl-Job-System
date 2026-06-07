"""MotherDuck Gold Layer — load Silver data to Gold tables."""
# Airflow DAG
from _dag_factory import create_load_silver_to_gold_dag

dag = create_load_silver_to_gold_dag()
