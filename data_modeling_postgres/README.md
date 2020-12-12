Welcome to the Sparkify Analytics Database
==========================================

The following data architecture was created to facilitate the analysis of data from Sparkify's music streaming app.  The streaming music app produces transaction log data in JSON format. The database and ETL process creates the required infrastructure to allow the Sparkify team to more easily query their data in a logical manner with familiar technology. The database that was chosen for this project was Postgres (SQL) database. 

## Technologies Used
* Python 3.6.3
* PostgreSQL 9.5.21

Running the Scripts
-------------------
1.  First, run the create_tables.py to create the database and all tables.  After this is run the database is ready to accept data.
>python create_tables.py
2.  Process all of the log files, both song data and log data, using the etl.py script.
>python etl.py



ETL Process
-----------

The database tables are created and populated from the source data via two python scripts.

### Create Tables/Database
The *create_tables.py* script uses the predefined queries in *sql_queries.py* to create tables, establish data types, define any primary keys, and defines the insert statements that are required to build the database. The script starts by dropping any tables that may be present and running the create table SQL statements.  The resulting collection of tables are clean and ready for data to be populated.  

### Populating the Database
The next step in the process runs queries intended to process the Sparkify app data, log and song data, and insert the required elements into the appropriate database tables.  This data is contained in two local directories and in JSON format.  The initial processing and cleaning of the data are accomplished in the *etl.py* script.  The transformed data is finally inserted into the database via the *INSERT INTO* statements in the *sql_queries.py* script.

The data is retrieved from local directories and is expecting to find the logs in this type of file structure:
```
|--data
|--log_data
|  |--year
|      |--month
|           |--log files.json
|--song_data
    |--file prefix
         |--file prefix
              |--file prefix
                   |--song files.json
```              

### Database Schema
The database schema is a star schema with the songplays table as the fact table and the other tables acting as dimensions of that table.

<p align="center">
  <img width="400" height="400" src="sparkifyERD.png">
</p>


## Songplays Table (FACT TABLE)
| Column      | Data Type             | Conditions  |
|-------------|-----------------------|-------------|
| songplay_id | auto-incrementing int | PRIMARY KEY |
| start_time  | time                  | NULLS       |
| user_id     | integer               | NULLS       |
| level       | varchar               | NULLS       |
| song_id     | varchar               | NULLS       |
| artist_id   | varchar               | NULLS       |
| session_id  | integer               | NULLS       |
| location    | varchar               | NULLS       |
| user_agent  | varchar               | NULLS       |
| title       | varchar               | NULLS       |
| artist_name | varchar               | NULLS       |
| duration    | decimal               | NULLS       |

## Users Table (DIMENSION TABLE)
| Column     | Data Type             | Conditions  |
|------------|-----------------------|-------------|
| user_id    | integer               | PRIMARY KEY |
| first_name | varchar               | NULLS       |
| last_name  | varchar               | NULLS       |
| gender     | varchar               | NULLS       |
| level      | varchar               | NULLS       |

## Songs Table (DIMENSION TABLE)
| Column      | Data Type | Conditions  |
|-------------|-----------|-------------|
| song_id     | varchar   | PRIMARY KEY |
| title       | varchar   | NULLS       |
| artist_id   | varchar   | NULLS       |
| artist_name | varchar   | NULLS       |
| year        | varchar   | NULLS       |
| duration    | decimal   | NULLS       |

## Artists Table (DIMENSION TABLE)
| Column           | Data Type | Conditions  |
|------------------|-----------|-------------|
| artist_id        | varchar   | PRIMARY KEY |
| artist_name      | varchar   | NULLS       |
| artist_location  | varchar   | NULLS       |
| artist_latitude  | decimal   | NULLS       |
| artist_longitude | decimal   | NULLS       |

## Time Table (DIMENSION TABLE)
| Column     | Data Type | Conditions |
|------------|-----------|------------|
| start_time | time      | NULLS      |
| hour       | integer   | NULLS      |
| day        | integer   | NULLS      |
| week       | integer   | NULLS      |
| month      | integer   | NULLS      |
| year       | integer   | NULLS      |
| weekday    | integer   | NULLS      |