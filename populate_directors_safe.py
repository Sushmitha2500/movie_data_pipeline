import sqlite3, pandas as pd, requests, time, urllib.parse

DB = "movies.db"
OMDB_KEY = "63be9b70"
OMDB_SLEEP = 0.4
MAX_RETRIES = 6

def get_conn():
    return sqlite3.connect(DB, timeout=30, check_same_thread=False)

def safe_execute(cur, sql, params=()):
    for attempt in range(MAX_RETRIES):
        try:
            cur.execute(sql, params)
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                print("DB locked, retrying...", attempt+1)
                time.sleep(2)
            else:
                raise
    raise RuntimeError(" Persistent DB lock after retries.")

con = get_conn()
cur = con.cursor()

safe_execute(cur, "CREATE TABLE IF NOT EXISTS directors (director_id INTEGER PRIMARY KEY AUTOINCREMENT, director_name TEXT UNIQUE);")
safe_execute(cur, "CREATE TABLE IF NOT EXISTS movie_directors (id INTEGER PRIMARY KEY AUTOINCREMENT, movie_id INTEGER, director_id INTEGER);")
con.commit()

movies_df = pd.read_sql_query("SELECT movie_id, title, year FROM movies LIMIT 30", con)
print(f" Processing {len(movies_df)} movies...\n")

inserted_links = 0
for i, row in movies_df.iterrows():
    mid = row['movie_id']
    title = row['title']
    year = row['year']
    q = {"t": title, "apikey": OMDB_KEY}
    if year and str(year).isdigit():
        q["y"] = str(year)
    url = "http://www.omdbapi.com/?" + urllib.parse.urlencode(q)
    print(f"[{i+1}/{len(movies_df)}] {title} ({year})")

    try:
        r = requests.get(url, timeout=10)
        j = r.json() if r.status_code == 200 else {"Response":"False"}
    except Exception as e:
        print(" Request error:", e)
        j = {"Response":"False"}

    if j.get("Response") == "True":
        dirs = j.get("Director", "")
        if dirs and dirs != "N/A":
            for dname in [d.strip() for d in dirs.split(",") if d.strip()]:
                safe_execute(cur, "INSERT OR IGNORE INTO directors (director_name) VALUES (?)", (dname,))
                safe_execute(cur, "SELECT director_id FROM directors WHERE director_name=?", (dname,))
                did = cur.fetchone()[0]
                safe_execute(cur, "INSERT OR IGNORE INTO movie_directors (movie_id, director_id) VALUES (?, ?)", (mid, did))
                inserted_links += 1
        else:
            print("No director found.")
    else:
        print(" OMDb returned no data.")

    con.commit()
    time.sleep(OMDB_SLEEP)

cur.execute("SELECT COUNT(*) FROM directors"); dcount = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM movie_directors"); mdcount = cur.fetchone()[0]
con.close()

print(f"\nDone! Directors: {dcount}, movie_directors: {mdcount}, inserted_links: {inserted_links}")