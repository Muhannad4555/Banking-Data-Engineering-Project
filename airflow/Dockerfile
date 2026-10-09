FROM apache/airflow:2.10.5-python3.12

# dbt gets its own venv: its dependencies conflict with the versions Airflow pins
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake==1.12.1
