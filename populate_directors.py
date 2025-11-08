import sqlite3
import pandas as pd
import requests
import time
import urllib.parse

DB = "movies.db"
OMDB_KEY = "63be9b70"  
OMDB_SLEEP = 0.35  

con = sqlite3.connect(DB)
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS directors (
    director_id INTEGER PRIMARY KEY AUTOINCREMENT,
    director_name TEXT UNIQUE
);
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS movie_directors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER,
    director_id INTEGER
);
""")
con.commit()

movies_df = pd.read_sql_query("SELECT movie_id, title, year FROM movies LIMIT 100", con)

def query_omdb(title, year=None):
    q = {"t": title, "apikey": OMDB_KEY}
    if year and str(year).isdigit():
        q["y"] = str(year)
    url = "http://www.omdbapi.com/?" + urllib.parse.urlencode(q)
    try:
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            return r.json()
        return {"Response": "False", "Error": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"Response": "False", "Error": str(e)}

inserted = 0
for _, row in movies_df.iterrows():
    title = row["title"]
    year = row["year"]
    mid = row["movie_id"]
    res = query_omdb(title, year)
    if res.get("Response") == "True":
        directors_field = res.get("Director", "").strip()
        if directors_field and directors_field != "N/A":
            for dname in [d.strip() for d in directors_field.split(",") if d.strip()]:
                cur.execute("INSERT OR IGNORE INTO directors (director_name) VALUES (?)", (dname,))
                cur.execute("SELECT director_id FROM directors WHERE director_name=?", (dname,))
                did = cur.fetchone()[0]
                cur.execute("INSERT OR IGNORE INTO movie_directors (movie_id, director_id) VALUES (?, ?)", (mid, did))
                inserted += 1
    time.sleep(OMDB_SLEEP)

con.commit()
cur.execute("SELECT COUNT(*) FROM directors")
dcount = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM movie_directors")
mdcount = cur.fetchone()[0]
con.close()

print(f" Directors added: {dcount}, movie_directors links: {mdcount}")
print(f"Inserted {inserted} new links from OMDb.")