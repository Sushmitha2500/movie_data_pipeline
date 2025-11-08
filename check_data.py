import sqlite3

con = sqlite3.connect("movies.db")
cur = con.cursor()

print("\n Table row counts:\n")
for t in ["movies","ratings","genres","directors","movie_genres","movie_directors"]:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"{t:15s} : {cur.fetchone()[0]}")

con.close()