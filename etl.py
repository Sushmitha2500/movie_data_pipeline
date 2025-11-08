import os
import json
import time
import argparse
from pathlib import Path
import pandas as pd
import requests
from sqlalchemy import (create_engine, Table, Column, Integer, String, MetaData, Float, Text,
                        DateTime, UniqueConstraint, ForeignKey, select)
from sqlalchemy.exc import IntegrityError

DB_URL = os.environ.get("DB_URL", "sqlite:///movies.db")
OMDB_API_KEY = os.environ.get("63be9b70")  
OMDB_CACHE = Path("omdb_cache.json")
MOVIES_CSV = "movies.csv"
RATINGS_CSV = "ratings.csv"
OMDB_SLEEP = 0.2  

engine = create_engine(DB_URL, connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})
metadata = MetaData()

movies = Table('movies', metadata,
    Column('movie_id', Integer, primary_key=True),
    Column('title', Text, nullable=False),
    Column('year', Integer),
    Column('imdb_id', String, unique=True),
    Column('plot', Text),
    Column('box_office', String),
    Column('runtime', Integer),
)

genres = Table('genres', metadata,
    Column('genre_id', Integer, primary_key=True, autoincrement=True),
    Column('genre_name', String, unique=True, nullable=False),
)

movie_genres = Table('movie_genres', metadata,
    Column('movie_id', Integer, ForeignKey('movies.movie_id', ondelete='CASCADE'), primary_key=True),
    Column('genre_id', Integer, ForeignKey('genres.genre_id', ondelete='CASCADE'), primary_key=True),
)

directors = Table('directors', metadata,
    Column('director_id', Integer, primary_key=True, autoincrement=True),
    Column('director_name', String, unique=True, nullable=False),
)

movie_directors = Table('movie_directors', metadata,
    Column('movie_id', Integer, ForeignKey('movies.movie_id', ondelete='CASCADE'), primary_key=True),
    Column('director_id', Integer, ForeignKey('directors.director_id', ondelete='CASCADE'), primary_key=True),
)

ratings = Table('ratings', metadata,
    Column('rating_id', Integer, primary_key=True, autoincrement=True),
    Column('user_id', Integer, nullable=False),
    Column('movie_id', Integer, ForeignKey('movies.movie_id', ondelete='CASCADE')),
    Column('rating', Float, nullable=False),
    Column('timestamp', Integer),
    UniqueConstraint('user_id', 'movie_id', name='uix_user_movie')
)

def ensure_schema():
    metadata.create_all(engine)

def load_cache():
    if OMDB_CACHE.exists():
        return json.loads(OMDB_CACHE.read_text(encoding='utf-8'))
    return {}

def save_cache(cache):
    OMDB_CACHE.write_text(json.dumps(cache, indent=2), encoding='utf-8')

def query_omdb_by_title(title, year=None):
    """
    Query OMDb by title (optionally with year). Returns None if not found or API key missing.
    """
    if not OMDB_API_KEY:
        return None
    params = {"apikey": OMDB_API_KEY, "t": title}
    if year:
        params["y"] = str(year)
    r = requests.get("http://www.omdbapi.com/", params=params, timeout=10)
    if r.status_code != 200:
        return None
    j = r.json()
    if j.get("Response") == "False":
        return None
    return j

def find_or_create_genre(conn, name, cache_genre_ids):
    name = name.strip()
    if not name:
        return None
    if name in cache_genre_ids:
        return cache_genre_ids[name]

    sel = select(genres.c.genre_id).where(genres.c.genre_name == name)
    res = conn.execute(sel).fetchone()
    if res:
        gid = res[0]
    else:
        ins = genres.insert().values(genre_name=name)
        try:
            result = conn.execute(ins)
            gid = result.inserted_primary_key[0] if result.inserted_primary_key else conn.execute(sel).fetchone()[0]
        except IntegrityError:
            res = conn.execute(sel).fetchone()
            gid = res[0]
    cache_genre_ids[name] = gid
    return gid

def find_or_create_director(conn, name, cache_director_ids):
    name = name.strip()
    if not name:
        return None
    if name in cache_director_ids:
        return cache_director_ids[name]
    sel = select(directors.c.director_id).where(directors.c.director_name == name)
    res = conn.execute(sel).fetchone()
    if res:
        did = res[0]
    else:
        ins = directors.insert().values(director_name=name)
        try:
            result = conn.execute(ins)
            did = result.inserted_primary_key[0] if result.inserted_primary_key else conn.execute(sel).fetchone()[0]
        except IntegrityError:
            res = conn.execute(sel).fetchone()
            did = res[0]
    cache_director_ids[name] = did
    return did

def upsert_movie(conn, movie_row):
    """
    movie_row: dict with keys movie_id, title, year, imdb_id, plot, box_office, runtime
    Use SELECT then INSERT/UPDATE for portability.
    """
    sel = select(movies.c.movie_id).where(movies.c.movie_id == movie_row['movie_id'])
    existing = conn.execute(sel).fetchone()
    if existing:
        upd = movies.update().where(movies.c.movie_id == movie_row['movie_id']).values(
            title=movie_row.get('title'),
            year=movie_row.get('year'),
            imdb_id=movie_row.get('imdb_id'),
            plot=movie_row.get('plot'),
            box_office=movie_row.get('box_office'),
            runtime=movie_row.get('runtime')
        )
        conn.execute(upd)
    else:
        ins = movies.insert().values(**movie_row)
        conn.execute(ins)

def upsert_rating(conn, rating_row):
    sel = select(ratings.c.rating_id).where((ratings.c.user_id == rating_row['user_id']) & (ratings.c.movie_id == rating_row['movie_id']))
    existing = conn.execute(sel).fetchone()
    if existing:
        upd = ratings.update().where(ratings.c.rating_id == existing[0]).values(rating=rating_row['rating'], timestamp=rating_row.get('timestamp'))
        conn.execute(upd)
    else:
        ins = ratings.insert().values(**rating_row)
        conn.execute(ins)

def parse_title_and_year(title_raw):
    title = title_raw
    year = None
    if title_raw and title_raw.strip().endswith(")"):
        try:
            i = title_raw.rfind(" (")
            if i != -1:
                year_part = title_raw[i+2:-1]
                if year_part.isdigit():
                    year = int(year_part)
                    title = title_raw[:i]
        except Exception:
            pass
    return title.strip(), year

def main():
    parser = argparse.ArgumentParser(description="ETL for MovieLens + OMDb")
    parser.add_argument("--movies", default=MOVIES_CSV)
    parser.add_argument("--ratings", default=RATINGS_CSV)
    args = parser.parse_args()

    if not Path(args.movies).exists() or not Path(args.ratings).exists():
        print("ERROR: movies.csv or ratings.csv not found in current directory.")
        print("Put MovieLens 'movies.csv' and 'ratings.csv' here and re-run.")
        return

    ensure_schema()
    conn = engine.connect()
    cache = load_cache()
    cache_changed = False

    df_movies = pd.read_csv(args.movies)
    df_ratings = pd.read_csv(args.ratings)

    cache_genre_ids = {}
    cache_director_ids = {}

    res = conn.execute(select(genres.c.genre_id, genres.c.genre_name)).fetchall()
    for gid, gname in res:
        cache_genre_ids[gname] = gid
    res = conn.execute(select(directors.c.director_id, directors.c.director_name)).fetchall()
    for did, dname in res:
        cache_director_ids[dname] = did

    for idx, row in df_movies.iterrows():
        mid = int(row['movieId'])
        raw_title = row['title']
        title, year = parse_title_and_year(raw_title)

        cache_key = f"{title}|||{year}"
        omdb_data = cache.get(cache_key)
        if omdb_data is None and OMDB_API_KEY:
            try:
                omdb_data = query_omdb_by_title(title, year)
                if omdb_data is None and year is not None:
                    time.sleep(OMDB_SLEEP)
                    omdb_data = query_omdb_by_title(title, None)
                cache[cache_key] = omdb_data  
                cache_changed = True
            except Exception as e:
                print(f"Warning: OMDb query failed for {title}: {e}")
                omdb_data = None

            time.sleep(OMDB_SLEEP)

        movie_row = {
            'movie_id': mid,
            'title': title,
            'year': year,
            'imdb_id': None,
            'plot': None,
            'box_office': None,
            'runtime': None
        }
        if omdb_data:
            movie_row['imdb_id'] = omdb_data.get('imdbID')
            movie_row['plot'] = omdb_data.get('Plot')
            movie_row['box_office'] = omdb_data.get('BoxOffice')
            try:
                rt = omdb_data.get('Runtime')
                if rt and rt.lower().endswith('min'):
                    movie_row['runtime'] = int(rt.split()[0])
            except Exception:
                movie_row['runtime'] = None

        upsert_movie(conn, movie_row)

        genres_list = str(row.get('genres', '')).split('|') if pd.notna(row.get('genres')) else []
        for g in genres_list:
            if g == '(no genres listed)' or not g.strip():
                continue
            gid = find_or_create_genre(conn, g, cache_genre_ids)
            sel = select(movie_genres.c.movie_id).where((movie_genres.c.movie_id == mid) & (movie_genres.c.genre_id == gid))
            if not conn.execute(sel).fetchone():
                conn.execute(movie_genres.insert().values(movie_id=mid, genre_id=gid))
        if omdb_data:
            directors_field = omdb_data.get('Director')
            if directors_field and directors_field != "N/A":
                for d in directors_field.split(','):
                    dn = d.strip()
                    if not dn:
                        continue
                    did = find_or_create_director(conn, dn, cache_director_ids)
                    sel = select(movie_directors.c.movie_id).where((movie_directors.c.movie_id == mid) & (movie_directors.c.director_id == did))
                    if not conn.execute(sel).fetchone():
                        conn.execute(movie_directors.insert().values(movie_id=mid, director_id=did))

    for idx, r in df_ratings.iterrows():
        rating_row = {
            'user_id': int(r['userId']),
            'movie_id': int(r['movieId']),
            'rating': float(r['rating']),
            'timestamp': int(r['timestamp']) if 'timestamp' in r and pd.notna(r['timestamp']) else None
        }
        try:
            upsert_rating(conn, rating_row)
        except Exception as e:
            print(f"Warning: failed to insert rating row {rating_row}: {e}")

    if cache_changed:
        save_cache(cache)
        print(f"OMDb cache updated ({len(cache)} entries) -> {OMDB_CACHE}")

    conn.close()
    print("ETL finished.")

if __name__ == "__main__":
    main()