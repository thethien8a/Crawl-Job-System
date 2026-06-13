"""Silver Dashboard — generate static HTML report for Silver layer monitoring."""
# Airflow DAG
from _dag_factory import create_silver_dashboard_dag

dag = create_silver_dashboard_dag()
