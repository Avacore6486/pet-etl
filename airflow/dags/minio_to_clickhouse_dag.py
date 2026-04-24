"""
# DAG: MinIO → ClickHouse (raw_sales_events)
- Читает Parquet-файлы из MinIO (s3a://lake/sales_events) за дату запуска
- Загружает данные в ClickHouse таблицу raw_sales_events через Spark
- Мат. вьюха mv_sales_daily автоматически заполняет витрину sales_metrics_daily
- Запускается каждый час
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
APPS_DIR = "/opt/spark-apps"

JARS = ",".join([
    f"{APPS_DIR}/hadoop-aws-3.3.4.jar",
    f"{APPS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
    f"{APPS_DIR}/commons-pool2-2.11.1.jar",
    f"{APPS_DIR}/clickhouse-jdbc-0.6.3-all.jar",
])

SPARK_CMD = (
    f"docker exec spark_app_pet {SPARK_SUBMIT} "
    f"--master {SPARK_MASTER} "
    f"--total-executor-cores 1 "
    f"--executor-memory 1G "
    f"--jars {JARS} "
    f"{APPS_DIR}/minio_to_clickhouse.py "
    "{{ ds }}"
)

with DAG(
    dag_id="minio_to_clickhouse",
    start_date=datetime(2026, 4, 1),
    schedule="0 * * * *",  # каждый час
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["clickhouse", "spark", "minio"],
) as dag:
    dag.doc_md = __doc__

    load_to_clickhouse = BashOperator(
        task_id="load_to_clickhouse",
        bash_command=SPARK_CMD,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dbt_sales",
    )

    load_to_clickhouse >> trigger_dbt
