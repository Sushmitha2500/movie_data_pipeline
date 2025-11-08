import sqlite3
import pandas as pd

con = sqlite3.connect("movies.db")

query = """
SELECT m.title,
       ROUND(AVG(r.rating), 2) AS avg_rating,
       COUNT(*) AS cnt
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id
HAVING cnt >= 10
ORDER BY avg_rating DESC
LIMIT 10;
"""

df = pd.read_sql_query(query, con)
print(df)

con.close()