"""
# Второй DAG FILMS from one date with detailes
- Загружаем данные из сырой таблицы в stage слой с преобразованием
- Даг запускается после успешного завершения первого.
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# текущая дата загрузки
CURRENT_VARIABLE_NAME = "tmdb_current_load_date"
POSTGRES_CONN_ID = "postgres"


def resolve_load_date(**kwargs):
    # достаем данные из прошлого дага
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    load_date = conf.get("load_date")
    if not load_date:
        load_date = Variable.get(CURRENT_VARIABLE_NAME)
    # сохраняем в икском дату для тасок
    kwargs["ti"].xcom_push(key="load_date", value=load_date)
    print(f"Stage load_date: {load_date}")


with DAG(
    dag_id="tmdb_stage_pipeline",
    start_date=datetime(2026, 3, 22),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["tmdb", "stage"],
) as dag:
    dag.doc_md = __doc__
    # таск определения даты обработки
    get_load_date = PythonOperator(
        task_id="get_load_date",
        python_callable=resolve_load_date,
    )
    # таск выполнения sql: удаляем данные за дату, если есть и выполняем вставку
    load_stage_movie_details = SQLExecuteQueryOperator(
        task_id="load_stage_movie_details",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        delete from stage.stg_tmdb_movie_details_v2
        where load_date = '{{ ti.xcom_pull(task_ids="get_load_date", key="load_date") }}'::date;

        insert into stage.stg_tmdb_movie_details_v2 (
            raw_id,
            load_date,
            movie_id,
            ingested_at,
            title,
            original_title,
            original_language,
            overview,
            tagline,
            movie_status,
            homepage,
            imdb_id,
            release_date,
            runtime_minutes,
            budget,
            revenue,
            popularity,
            vote_average,
            vote_count,
            is_adult,
            is_video,
            poster_path,
            backdrop_path,
            genres_json,
            production_companies_json,
            production_countries_json,
            spoken_languages_json,
            raw_payload
        )
        select
            r.id as raw_id,
            r.load_date,
            r.movie_id,
            r.ingested_at,
            r.payload ->> 'title' as title,
            r.payload ->> 'original_title' as original_title,
            r.payload ->> 'original_language' as original_language,
            r.payload ->> 'overview' as overview,
            r.payload ->> 'tagline' as tagline,
            r.payload ->> 'status' as movie_status,
            r.payload ->> 'homepage' as homepage,
            r.payload ->> 'imdb_id' as imdb_id,
            nullif(r.payload ->> 'release_date', '')::date as release_date,
            nullif(r.payload ->> 'runtime', '')::integer as runtime_minutes,
            nullif(r.payload ->> 'budget', '')::bigint as budget,
            nullif(r.payload ->> 'revenue', '')::bigint as revenue,
            nullif(r.payload ->> 'popularity', '')::numeric as popularity,
            nullif(r.payload ->> 'vote_average', '')::numeric as vote_average,
            nullif(r.payload ->> 'vote_count', '')::integer as vote_count,
            nullif(r.payload ->> 'adult', '')::boolean as is_adult,
            nullif(r.payload ->> 'video', '')::boolean as is_video,
            r.payload ->> 'poster_path' as poster_path,
            r.payload ->> 'backdrop_path' as backdrop_path,
            r.payload -> 'genres' as genres_json,
            r.payload -> 'production_companies' as production_companies_json,
            r.payload -> 'production_countries' as production_countries_json,
            r.payload -> 'spoken_languages' as spoken_languages_json,
            r.payload as raw_payload
        from raw.tmdb_movie_details r
        where r.load_date = '{{ ti.xcom_pull(task_ids="get_load_date", key="load_date") }}'::date;
        """,
    )
    # таск триггера для третьего дага
    trigger_mart = TriggerDagRunOperator(
        task_id="trigger_mart",
        trigger_dag_id="tmdb_mart_pipeline",
        # передаем данные для другого дага
        conf={
            "load_date": "{{ ti.xcom_pull(task_ids='get_load_date', key='load_date') }}"
        },
    )

    get_load_date >> load_stage_movie_details >> trigger_mart
