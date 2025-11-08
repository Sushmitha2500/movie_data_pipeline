import sqlite3, pandas as pd

con = sqlite3.connect("movies.db")
cur = con.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS genres (genre_id INTEGER PRIMARY KEY AUTOINCREMENT, genre_name TEXT UNIQUE)")
cur.execute("CREATE TABLE IF NOT EXISTS movie_genres (id INTEGER PRIMARY KEY AUTOINCREMENT, movie_id INTEGER, genre_id INTEGER)")

movies = pd.read_csv("movies.csv")

inserted_links = 0
for _, row in movies.iterrows():
    mid = row["movieId"]
    genres = str(row["genres"]).split("|") if pd.notna(row["genres"]) else []
    for g in genres:
        if g and g != "(no genres listed)":
            cur.execute("INSERT OR IGNORE INTO genres (genre_name) VALUES (?)", (g,))
            cur.execute("SELECT genre_id FROM genres WHERE genre_name=?", (g,))
            gid = cur.fetchone()[0]
            cur.execute("INSERT OR IGNORE INTO movie_genres (movie_id, genre_id) VALUES (?, ?)", (mid, gid))
            inserted_links += 1

con.commit()
con.close()
print(f"Done! Populated genres and movie_genres. Inserted links: {inserted_links}")