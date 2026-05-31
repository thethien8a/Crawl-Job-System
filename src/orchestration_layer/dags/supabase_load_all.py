"""Supabase Load DAG for all sites."""
# Airflow DAG
from _dag_factory import create_supabase_dag

dag = create_supabase_dag()
