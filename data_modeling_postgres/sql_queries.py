# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id serial PRIMARY KEY,
start_time time NOT NULL,
user_id integer NOT NULL,
level varchar,
song_id varchar,
artist_id varchar,
session_id integer,
location varchar,
user_agent varchar,
title varchar,
artist_name varchar,
duration decimal
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
user_id integer PRIMARY KEY, 
first_name varchar,
last_name varchar, 
gender varchar, 
level varchar
)

""")


song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id varchar PRIMARY KEY,
title varchar NOT NULL,
artist_id varchar,
artist_name varchar NOT NULL,
year integer,
duration decimal NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id varchar PRIMARY KEY, 
artist_name varchar,
artist_location varchar,
artist_latitude decimal,  
artist_longitude decimal
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time time,
hour integer, 
day integer,
week integer,
month integer,
year integer, 
weekday integer
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays 
(start_time ,user_id ,level ,song_id,artist_id ,session_id ,location ,user_agent,title,artist_name,duration)  
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
""")
#'userId', 'lastName','firstName','gender','level'
user_table_insert = ("""
INSERT INTO users
(user_id,last_name,first_name,gender,level) 
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (user_id) DO UPDATE 
SET (last_name,first_name,gender,level)=(EXCLUDED.last_name,EXCLUDED.first_name,EXCLUDED.gender,EXCLUDED.level);
""")

song_table_insert = ("""
INSERT INTO songs
(song_id,title,artist_id,artist_name,year,duration)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists 
(artist_id,artist_name, artist_location,artist_latitude,artist_longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE
SET (artist_name, artist_location,artist_latitude,artist_longitude)=(EXCLUDED.artist_name, EXCLUDED.artist_location,EXCLUDED.artist_latitude,EXCLUDED.artist_longitude) ;
""")


time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)  VALUES (%s,%s,%s,%s,%s,%s,%s)
""")

# FIND SONGS

song_select = ("""
SELECT song_id,artists.artist_id  
FROM songs 
JOIN artists ON songs.artist_id=artists.artist_id
WHERE (title = %s and artists.artist_name= %s and duration = %s)
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]