import sqlite3, pandas as pd

con = sqlite3.connect("movies.db")


query = """
SELECT year, COUNT(*) AS movie_count
FROM movies
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year DESC
LIMIT 10;
"""

df = pd.read_sql_query(query, con)
print(df)

con.close()