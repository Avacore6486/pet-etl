{{ config(
    materialized='incremental',
    unique_key='stage_row_key'
) }}

select
    md5(load_date::text || '-' || movie_id::text) as stage_row_key,
    id as raw_id,
    load_date,
    movie_id,
    ingested_at,
    payload ->> 'title' as title,
    payload ->> 'original_title' as original_title,
    payload ->> 'original_language' as original_language,
    payload ->> 'overview' as overview,
    payload ->> 'tagline' as tagline,
    payload ->> 'status' as movie_status,
    payload ->> 'homepage' as homepage,
    payload ->> 'imdb_id' as imdb_id,
    nullif(payload ->> 'release_date', '')::date as release_date,
    nullif(payload ->> 'runtime', '')::integer as runtime_minutes,
    nullif(payload ->> 'budget', '')::bigint as budget,
    nullif(payload ->> 'revenue', '')::bigint as revenue,
    nullif(payload ->> 'popularity', '')::numeric as popularity,
    nullif(payload ->> 'vote_average', '')::numeric as vote_average,
    nullif(payload ->> 'vote_count', '')::integer as vote_count,
    nullif(payload ->> 'adult', '')::boolean as is_adult,
    nullif(payload ->> 'video', '')::boolean as is_video,
    payload ->> 'poster_path' as poster_path,
    payload ->> 'backdrop_path' as backdrop_path,
    payload -> 'genres' as genres_json,
    payload -> 'production_companies' as production_companies_json,
    payload -> 'production_countries' as production_countries_json,
    payload -> 'spoken_languages' as spoken_languages_json,
    payload as raw_payload
from {{ source('raw', 'tmdb_movie_details') }}
where load_date = '{{ var("load_date") }}'::date