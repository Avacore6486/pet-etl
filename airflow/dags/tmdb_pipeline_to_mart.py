"""
# Третий финальный DAG FILMS from one date with detailes
- Загружаем данные из stage таблицы в витрину
- Даг запускается после успешного завершения второго
- Витрина будет использована для визуализации в Superset
- Данные в витрине накапливаются
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


CURRENT_VARIABLE_NAME = "tmdb_current_load_date"
POSTGRES_CONN_ID = "postgres"


# определим дату и передаnm её дальше
def resolve_load_date(**kwargs):
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    load_date = conf.get("load_date")
    if not load_date:
        load_date = Variable.get(CURRENT_VARIABLE_NAME)

    kwargs["ti"].xcom_push(key="load_date", value=load_date)
    print(f"Mart load_date: {load_date}")


with DAG(
    dag_id="tmdb_mart_pipeline",
    start_date=datetime(2026, 3, 22),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["tmdb", "mart"],
) as dag:
    dag.doc_md = __doc__

    get_load_date = PythonOperator(
        task_id="get_load_date",
        python_callable=resolve_load_date,
    )

    load_mart_movies = SQLExecuteQueryOperator(
        task_id="load_mart_movies",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        delete from datamarts.mart_movies
        where load_date = '{{ ti.xcom_pull(task_ids="get_load_date", key="load_date") }}'::date;

        insert into datamarts.mart_movies (
            load_date,
            movie_id,
            title,
            original_language,
            release_date,
            runtime_minutes,
            budget,
            revenue,
            popularity,
            vote_average,
            vote_count
        )
        select
            s.load_date,
            s.movie_id,
            s.title,
            s.original_language,
            s.release_date,
            s.runtime_minutes,
            s.budget,
            s.revenue,
            s.popularity,
            s.vote_average,
            s.vote_count
        from stage.stg_tmdb_movie_details_v2 s
        where s.load_date = '{{ ti.xcom_pull(task_ids="get_load_date", key="load_date") }}'::date;
        """,
    )

    get_load_date >> load_mart_movies
