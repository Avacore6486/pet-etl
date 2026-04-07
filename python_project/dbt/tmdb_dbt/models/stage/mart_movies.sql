{{ config(
    materialized='incremental',
    unique_key='mart_row_key',
    schema='datamarts'
) }}

with discover as (

    select
        load_date,
        movie_id,
        title,
        original_language,
        release_date,
        popularity,
        vote_average,
        vote_count
    from {{ ref('stg_tmdb_discover_movies') }}
    where load_date = '{{ var("load_date") }}'::date

),

details as (

    select
        load_date,
        movie_id,
        runtime_minutes,
        budget,
        revenue
    from {{ ref('stg_tmdb_movie_details_v2') }}
    where load_date = '{{ var("load_date") }}'::date

)

select
    md5(discover.load_date::text || '-' || discover.movie_id::text) as mart_row_key,
    discover.load_date,
    discover.movie_id,
    discover.title,
    discover.original_language,
    discover.release_date,
    discover.popularity,
    discover.vote_average,
    discover.vote_count,
    details.runtime_minutes,
    details.budget,
    details.revenue
from discover
left join details
    on discover.load_date = details.load_date
   and discover.movie_id = details.movie_id