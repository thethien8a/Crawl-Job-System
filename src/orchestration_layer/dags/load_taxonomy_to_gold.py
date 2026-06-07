"""MotherDuck Gold Layer — load taxonomy CSV seeds to Gold dimension tables."""
# Airflow DAG
from _dag_factory import create_load_taxonomy_to_gold_dag

dag = create_load_taxonomy_to_gold_dag()
