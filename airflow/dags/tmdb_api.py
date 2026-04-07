import time
import requests

# поведение запросов и ретраи
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ключ аутентификации для апи находится в Variable
from airflow.models import Variable

API_KEY = Variable.get("tmdb_api_key")

DISCOVER_URL = "https://api.themoviedb.org/3/discover/movie"  # первый апи
DETAIL_URL_TEMPLATE = "https://api.themoviedb.org/3/movie/{movie_id}"  # второй апи
# одна сессия на все запросы
session = requests.Session()

retry_strategy = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


# получаем данные фильмов по дате
def fetch_movies_by_date(load_date: str, pages: int = 1):
    all_movies = []

    for page in range(1, pages + 1):
        params = {
            "api_key": API_KEY,
            "release_date.gte": load_date,
            "release_date.lte": load_date,
            "page": page,  # сколько страниц результатов загрузить (на каждой странице 20 фильмов)
        }

        response = session.get(DISCOVER_URL, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        all_movies.extend(data.get("results", []))

    return all_movies


# получаем детали фильмов по их id
def fetch_movie_details(movie_id: int):
    url = DETAIL_URL_TEMPLATE.format(movie_id=movie_id)

    response = session.get(
        url,
        params={"api_key": API_KEY},
        timeout=30,
    )
    # выбросит код ошибки, если словит ошибку
    response.raise_for_status()

    return response.json()


#
def run_tmdb_pipeline(load_date: str, pages: int = 2):
    movies = fetch_movies_by_date(load_date=load_date, pages=pages)

    movie_details_list = []

    for movie in movies:
        movie_id = movie["id"]
        details = fetch_movie_details(movie_id)
        movie_details_list.append(details)

        time.sleep(0.2)  # пауза 0.2 секунды между запросами

    print(f"Дата загрузки: {load_date}")
    print(f"Сохранено фильмов из discover: {len(movies)}")
    print(f"Сохранено деталей фильмов: {len(movie_details_list)}")
    # вернули данные из двух апи
    return {
        "movies": movies,
        "movie_details_list": movie_details_list,
    }
