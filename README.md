Data Engineering Assignment — Movie Data Pipeline

Overview
This project implements a complete ETL (Extract, Transform, Load) pipeline that processes movie and ratings data using Python and SQLite.
The goal is to build a small-scale data engineering solution that ingests raw data from CSV files, enriches it using an external API (OMDb), loads it into a normalized database schema, and performs analytical SQL queries to generate insights such as top-rated movies, genres, and directors.

Environment Setup and Execution
download the project

Place all files in a folder (e.g. movie_data_pipeline).

 Install dependencies

pip install -r requirements.txt
3Run the ETL pipeline
This creates and populates the SQLite database (movies.db) with data from the CSV files.
python etl.py

 Populate directors (OMDb API)
Fetches and stores movie directors using the OMDb API.
python populate_directors.py
Note: You can export your OMDb API key as an environment variable before running:
setx OMDB_API_KEY "your_api_key_here"

(Optional) Populate genres
Normalizes genres and links them to movies.

python populate_genres.py
 Run SQL queries
Executes analytical queries and prints summary outputs.
python test_query.py


Design Choices and Assumptions
Database:
Used SQLite for simplicity and portability. It satisfies the “relational database” requirement and works cross-platform without setup.

Schema Design:
The data is normalized across multiple tables to avoid redundancy:
movies — stores movie information
ratings — stores user ratings
genres and movie_genres — many-to-many mapping between movies and genres
directors and movie_directors — many-to-many mapping between movies and directors

ETL Architecture:
Extract: Reads movies.csv and ratings.csv using pandas.
Transform: Cleans data, splits genres, and enriches records via OMDb API.
Load: Inserts data into SQLite using SQLAlchemy for schema creation and transactions.

External API:
Integrated OMDb API to fetch director details dynamically.
Rate-limiting handled via a small sleep delay to prevent throttling.

Assumptions:
Movies with missing or invalid ratings are skipped.
OMDb API may return missing data for some older or foreign titles.
Only movies with ≥5 ratings are used for “Top Movies” queries.


Challenges and Solutions
Challenge	How It Was Solved

Database locking errors	Closed all active connections before new inserts; used transactions to commit safely.
OMDb API limits	Implemented request delay (OMDB_SLEEP = 0.2) to avoid throttling.
Empty genre/director results initially	Added populate_genres.py and populate_directors.py scripts to fully populate mapping tables.
Query validation	Tested queries directly via SQLite Viewer extension in VS Code.
Environment setup	Created requirements.txt for quick reproducibility on any system.


Key Results
Movies loaded: 9,742
Ratings loaded: 100,836
Top Genres: Film-Noir, Crime, Drama
Top Directors: Robert Rodriguez, Martin Scorsese, Mel Gibson
Ratings by year: from 1902 to 2018

 Conclusion

This project demonstrates the full lifecycle of a data engineering workflow — from raw data ingestion to transformation, enrichment, loading, and analytical querying.
It highlights proficiency in Python, SQL, ETL design, and database normalization, fulfilling all requirements of the data engineering assignment.