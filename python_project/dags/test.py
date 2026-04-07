from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


def test_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_raw")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT 1;")
    result = cursor.fetchone()

    print(result)


with DAG(
    dag_id="test_postgres_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test = PythonOperator(
        task_id="test_connection",
        python_callable=test_postgres,
    )
