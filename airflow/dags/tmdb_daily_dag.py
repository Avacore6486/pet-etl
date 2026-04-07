"""
# DAG FILMS from one date with detailes
- Получаем данные из API https://www.themoviedb.org/settings/api
- Данные загружаются инкрементально один раз в день
"""

from datetime import datetime, timedelta
import json
from psycopg2.extras import execute_values  # массово вставить строки можно в постгрес

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.time_delta import TimeDeltaSensor

# импорт функции из основного файла api
from tmdb_api import run_tmdb_pipeline


CURRENT_VARIABLE_NAME = "tmdb_current_load_date"
NEXT_VARIABLE_NAME = "tmdb_next_load_date"
DEFAULT_START_LOAD_DATE = "2026-03-15"
POSTGRES_CONN_ID = "postgres"


# если есть дубликаты фильмов по id, то удаляем их
def deduplicate_by_movie_id(items):
    unique_items = {}

    for item in items:
        movie_id = item["id"]
        unique_items[movie_id] = item

    return list(unique_items.values())  # вернули список с уникальными фильмами


# сохраняем краткую информацию о фильмах в таблицу raw.tmdb_discover_movies в БД
def insert_discover_movies(conn, load_date, movies):
    rows = [
        (
            load_date,
            movie["id"],
            json.dumps(
                movie, ensure_ascii=False
            ),  # позволяет сохранять текст без экранирования
        )
        for movie in movies
    ]

    if not rows:
        return
    # объект, через который выполнится SQL скрипт
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            insert into raw.tmdb_discover_movies (
                load_date,
                movie_id,
                payload
            ) values %s
            on conflict (load_date, movie_id) do nothing 
            """,
            rows,
        )


# сохраняем полную информацию о фильмах в таблицу raw.tmdb_movie_details в БД
def insert_movie_details(conn, load_date, movie_details_list):
    rows = [
        (
            load_date,
            movie["id"],
            json.dumps(movie, ensure_ascii=False),
        )
        for movie in movie_details_list
    ]

    if not rows:
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            insert into raw.tmdb_movie_details (
                load_date,
                movie_id,
                payload
            ) values %s
            on conflict (load_date, movie_id) do nothing
            """,
            rows,
        )


def run_for_airflow(**kwargs):
    # читает дату из вариабл
    load_date = Variable.get(NEXT_VARIABLE_NAME, default_var=DEFAULT_START_LOAD_DATE)

    result = run_tmdb_pipeline(
        load_date=load_date,
        pages=2,  # 40 фильмов максимум
    )

    movies = deduplicate_by_movie_id(result["movies"])
    movie_details_list = deduplicate_by_movie_id(result["movie_details_list"])
    # лог для Airflow
    print(f"Дата загрузки: {load_date}")
    print(f"Discover после dedup: {len(movies)}")
    print(f"Details после dedup: {len(movie_details_list)}")
    # соединяемся с БД
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    # блок try except
    try:
        insert_discover_movies(conn, load_date, movies)
        insert_movie_details(conn, load_date, movie_details_list)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    #  вычисляем следующую дату (+ 1)
    next_date = (datetime.strptime(load_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    Variable.set(CURRENT_VARIABLE_NAME, load_date)
    Variable.set(NEXT_VARIABLE_NAME, next_date)
    # передаем дату load_date черех xcom в аерфлоу
    kwargs["ti"].xcom_push(key="load_date", value=load_date)
    # логируем для аерфлоу
    print(f"Загружена дата: {load_date}")
    print(f"Следующая дата загрузки: {next_date}")


# описание дага и параметры
with DAG(
    dag_id="tmdb_raw_pipeline",
    start_date=datetime(2026, 3, 22),
    # schedule="@daily",
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 5,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    },
    tags=["tmdb", "raw"],
) as dag:
    dag.doc_md = __doc__
    # таск с оператором сенсор, просто ждем определенное количество времени
    wait_before_api = TimeDeltaSensor(
        task_id="wait_before_api",
        delta=timedelta(minutes=2),
    )
    #  таска с оператором PythonOperator (так как используем код питон)
    load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=run_for_airflow,
    )
    #  таск триггера второго дага, в conf передаем данные для дага
    trigger_stage = TriggerDagRunOperator(
        task_id="trigger_stage",
        trigger_dag_id="tmdb_stage_pipeline",
        conf={"load_date": "{{ ti.xcom_pull(task_ids='load_raw', key='load_date') }}"},
    )
    # порядок
    wait_before_api >> load_raw >> trigger_stage
