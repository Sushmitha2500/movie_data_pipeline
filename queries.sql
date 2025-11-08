
SELECT m.title, AVG(r.rating) AS avg_rating, COUNT(r.rating) AS cnt
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id
HAVING COUNT(r.rating) >= 5
ORDER BY avg_rating DESC
LIMIT 1;

SELECT g.genre_name, AVG(r.rating) AS avg_rating, COUNT(r.rating) AS cnt
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN ratings r ON mg.movie_id = r.movie_id
GROUP BY g.genre_id
HAVING COUNT(r.rating) >= 50
ORDER BY avg_rating DESC
LIMIT 5;

SELECT d.director_name, COUNT(md.movie_id) AS movie_count
FROM directors d
JOIN movie_directors md ON d.director_id = md.director_id
GROUP BY d.director_id
ORDER BY movie_count DESC
LIMIT 1;

SELECT m.year, ROUND(AVG(r.rating), 3) AS avg_rating, COUNT(r.rating) AS cnt
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE m.year IS NOT NULL
GROUP BY m.year
ORDER BY m.year;