"""
# DAG генерация и вставка в БД
- Генерируем данные и каждые 5 минут вставляем в таблицу kafka.sales_events
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import random

DEFAULT_ARGS = {
    "owner": "evgenia",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

CATEGORIES = ["electronics", "clothing", "food", "sports", "home"]
REGIONS = ["EU", "US", "ASIA", "LATAM"]


def generate_and_insert():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="postgres",
        user="evgenia",
        password="pass",
    )
    cur = conn.cursor()

    rows = [
        (
            random.randint(1, 100),
            random.choice(CATEGORIES),
            random.randint(1, 20),
            round(random.uniform(5.0, 500.0), 2),
            random.choice(REGIONS),
        )
        for _ in range(random.randint(2, 5))  # 5-20 строк за раз
    ]

    cur.executemany(
        """
        INSERT INTO kafka.sales_events (product_id, category, quantity, price, region)
        VALUES (%s, %s, %s, %s, %s)
        """,
        rows,
    )

    conn.commit()
    print(f"Inserted {len(rows)} rows")
    cur.close()
    conn.close()


with DAG(
    dag_id="generate_sales_events",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",  # каждые 5 минут
    catchup=False,
    tags=["pet", "postgres"],
) as dag:
    dag.doc_md = __doc__

    insert_task = PythonOperator(
        task_id="insert_sales_rows",
        python_callable=generate_and_insert,
    )
