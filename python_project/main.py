from src.tmdb_api import run_tmdb_pipeline

if __name__ == "__main__":
    run_tmdb_pipeline(load_date="2025-10-01", pages=2)
