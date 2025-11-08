import sqlite3, pandas as pd
con = sqlite3.connect("movies.db")
q = """
SELECT user_id, COUNT(*) AS ratings_count
FROM ratings
GROUP BY user_id
ORDER BY ratings_count DESC
LIMIT 10;
"""
print(pd.read_sql_query(q, con))
con.close()