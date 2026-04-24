"""
# DAG генерация и вставка в БД
- Генерируем данные и каждые 5 минут вставляем в таблицу kafka.sales_events
- Расширенная схема: заказы, клиенты, скидки, платежи — для Spark-аналитики
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import uuid
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    "owner": "evgenia",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

CATEGORIES = [
    "electronics",
    "clothing",
    "food",
    "sports",
    "home",
    "beauty",
    "toys",
    "automotive",
]

PRODUCTS = {
    "electronics": [
        "laptop",
        "smartphone",
        "headphones",
        "tablet",
        "smartwatch",
        "camera",
    ],
    "clothing": ["jacket", "jeans", "sneakers", "dress", "hoodie", "t-shirt"],
    "food": ["coffee", "chocolate", "olive_oil", "pasta", "wine", "cheese"],
    "sports": ["yoga_mat", "dumbbell", "bicycle", "running_shoes", "tent", "kayak"],
    "home": ["lamp", "sofa", "blender", "vacuum", "curtains", "shelf"],
    "beauty": ["perfume", "moisturizer", "lipstick", "shampoo", "serum", "mascara"],
    "toys": ["lego", "puzzle", "doll", "board_game", "rc_car", "plush"],
    "automotive": ["dash_cam", "car_seat", "tire", "oil_filter", "gps", "charger"],
}

REGIONS = ["EU", "US", "ASIA", "LATAM", "MEA"]

CITIES = {
    "EU": ["Berlin", "Paris", "Madrid", "Rome", "Amsterdam", "Warsaw"],
    "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Miami"],
    "ASIA": ["Tokyo", "Shanghai", "Seoul", "Singapore", "Mumbai", "Bangkok"],
    "LATAM": ["São Paulo", "Mexico City", "Buenos Aires", "Bogotá", "Lima", "Santiago"],
    "MEA": ["Dubai", "Cairo", "Lagos", "Nairobi", "Riyadh", "Casablanca"],
}

PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "paypal",
    "apple_pay",
    "google_pay",
    "crypto",
]

CHANNELS = ["web", "mobile_app", "marketplace", "retail", "partner"]


def generate_and_insert():
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    batch_size = random.randint(30, 150)
    rows = []

    for _ in range(batch_size):
        region = random.choice(REGIONS)
        category = random.choice(CATEGORIES)
        product_name = random.choice(PRODUCTS[category])
        price = round(random.uniform(5.0, 1500.0), 2)
        quantity = random.randint(1, 10)
        discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20, 30])
        final_price = round(price * quantity * (1 - discount_pct / 100), 2)
        is_returned = random.random() < 0.05

        rows.append(
            (
                str(uuid.uuid4()),  # order_id
                random.randint(1, 10_000),  # customer_id
                random.randint(1, 500),  # product_id
                product_name,  # product_name
                category,  # category
                quantity,  # quantity
                price,  # unit_price
                discount_pct,  # discount_pct
                final_price,  # total_price
                region,  # region
                random.choice(CITIES[region]),  # city
                random.choice(PAYMENT_METHODS),  # payment_method
                random.choice(CHANNELS),  # channel
                is_returned,  # is_returned
                random.randint(1, 5),  # rating
            )
        )

    cur.executemany(
        """
        INSERT INTO kafka.sales_events (
            order_id, customer_id, product_id, product_name,
            category, quantity, unit_price, discount_pct, total_price,
            region, city, payment_method, channel, is_returned, rating
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["pet", "postgres"],
) as dag:
    dag.doc_md = __doc__

    insert_task = PythonOperator(
        task_id="insert_sales_rows",
        python_callable=generate_and_insert,
    )
