"""Bronze Dashboard — generate static HTML report for Bronze layer monitoring."""
# Airflow DAG
from _dag_factory import create_bronze_dashboard_dag

dag = create_bronze_dashboard_dag()
