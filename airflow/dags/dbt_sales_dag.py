"""
# DAG: dbt sales transformations
- Запускается после minio_to_clickhouse
- Выполняет dbt run: stg_sales_events → mart_sales_daily
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_sales",
    start_date=datetime(2026, 4, 1),
    schedule=None,  # запускается только через триггер
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["dbt", "clickhouse"],
) as dag:
    dag.doc_md = __doc__

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
    )
