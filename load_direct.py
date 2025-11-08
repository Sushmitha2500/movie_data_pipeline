import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text

DB_FILE = "movies.db"
SCHEMA_FILE = "schema.sql"
MOVIES_CSV = "movies.csv"
RATINGS_CSV = "ratings.csv"

print("Using DB file:", os.path.abspath(DB_FILE))
if not os.path.exists(SCHEMA_FILE):
    raise SystemExit("schema.sql not found in folder. Put schema.sql next to this script.")

with sqlite3.connect(DB_FILE) as conn:
    sql = open(SCHEMA_FILE, "r", encoding="utf-8").read()
    conn.executescript(sql)
print("Schema applied/verified.")

print("Reading CSVs...")
movies_df = pd.read_csv(MOVIES_CSV)
ratings_df = pd.read_csv(RATINGS_CSV)
print("movies shape:", movies_df.shape)
print("ratings shape:", ratings_df.shape)

def parse_title_year(title_raw):
    title = str(title_raw)
    year = None
    if title.endswith(")"):
        i = title.rfind(" (")
        if i != -1:
            part = title[i+2:-1]
            if part.isdigit():
                year = int(part)
                title = title[:i]
    return title.strip(), year

movies_parsed = []
for _, r in movies_df.iterrows():
    mid = int(r["movieId"])
    raw_title = r["title"]
    title, year = parse_title_year(raw_title)
    movies_parsed.append({
        "movie_id": mid,
        "title": title,
        "year": year,
        "imdb_id": None,
        "plot": None,
        "box_office": None,
        "runtime": None
    })
movies_clean = pd.DataFrame(movies_parsed)

ratings_clean = ratings_df.rename(columns={"userId": "user_id", "movieId": "movie_id"})
cols = ["user_id", "movie_id", "rating"]
if "timestamp" in ratings_clean.columns:
    cols.append("timestamp")
ratings_clean = ratings_clean[cols]

engine = create_engine(f"sqlite:///{DB_FILE}")

with engine.begin() as conn:
    conn.execute(text("DELETE FROM movie_genres"))
    conn.execute(text("DELETE FROM movie_directors"))
    conn.execute(text("DELETE FROM genres"))
    conn.execute(text("DELETE FROM directors"))
    conn.execute(text("DELETE FROM ratings"))
    conn.execute(text("DELETE FROM movies"))
    print("Cleared existing rows from tables.")

    movies_clean.to_sql("movies", con=conn, if_exists="append", index=False)
    print("Inserted movies:", len(movies_clean))
    ratings_clean.to_sql("ratings", con=conn, if_exists="append", index=False)
    print("Inserted ratings:", len(ratings_clean))

with sqlite3.connect(DB_FILE) as conn:
    cur = conn.cursor()
    for t in ["movies","ratings","genres","directors","movie_genres","movie_directors"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"{t:15s} : {cur.fetchone()[0]}")
        except Exception as e:
            print(f"{t:15s} : ERROR ->", e)

print("Done.")