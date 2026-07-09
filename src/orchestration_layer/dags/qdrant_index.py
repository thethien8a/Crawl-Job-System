"""Qdrant index DAG for Silver jobs."""
# Airflow DAG

from _dag_factory import create_qdrant_index_dag

dag = create_qdrant_index_dag()
