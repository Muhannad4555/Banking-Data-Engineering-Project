from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PYTHON = "/home/airflow/dbt_venv/bin/python"
DBT = (
    "/home/airflow/dbt_venv/bin/dbt {cmd} "
    "--project-dir /opt/airflow/banking_dbt --profiles-dir /opt/airflow/banking_dbt "
    "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs"  # keep dbt output off the mounted project folder
)

with DAG(
    "banking_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    load_raw = BashOperator(task_id="load_raw", bash_command=f"{PYTHON} /opt/airflow/scripts/load_raw.py")
    dbt_run = BashOperator(task_id="dbt_run", bash_command=DBT.format(cmd="run"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=DBT.format(cmd="test"))

    load_raw >> dbt_run >> dbt_test
