
import sqlite3
import pandas as pd

con = sqlite3.connect("movies.db")

q = """
SELECT d.director_name,
       ROUND(AVG(r.rating),2) AS avg_rating,
       COUNT(DISTINCT m.movie_id) AS movie_count
FROM directors d
JOIN movie_directors md ON d.director_id = md.director_id
JOIN movies m ON md.movie_id = m.movie_id
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY d.director_id
HAVING movie_count >= 3
ORDER BY avg_rating DESC
LIMIT 5;
"""

df = pd.read_sql_query(q, con)
print(df)

df.to_csv("top_directors.csv", index=False)
print("Wrote top_directors.csv")

con.close()