import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
JSONPATH = config.get('S3','LOG_JSONPATH')
ARN = config['IAM_ROLE']['ARN']

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stage_events 
(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR, 
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionId NUMERIC,
song VARCHAR,
status NUMERIC,
ts timestamp,
userAgent VARCHAR,
userId NUMERIC
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stage_songs 
(
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude FLOAT,  
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INTEGER

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
artist_id varchar NOT NULL,
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
start_time timestamp PRIMARY KEY,
hour integer, 
day integer,
week integer,
month integer,
year integer, 
weekday integer
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id int identity PRIMARY KEY,
start_time timestamp NOT NULL,
user_id integer NOT NULL,
level varchar,
song_id varchar NOT NULL,
artist_id varchar NOT NULL,
session_id integer,
location varchar,
user_agent varchar
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stage_events
    from {}
    credentials 'aws_iam_role={}' 
    region 'us-west-2'
    JSON {}
    TIMEFORMAT AS 'epochmillisecs';
""").format(LOG_DATA,ARN,JSONPATH)

staging_songs_copy = ("""
    copy stage_songs 
    from {} 
    credentials 'aws_iam_role={}' 
    region 'us-west-2'
    FORMAT AS JSON 'auto'
    ;
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays 
(start_time ,user_id ,level ,song_id,artist_id ,session_id ,location ,user_agent) 
SELECT DISTINCT
    e.ts,
    e.userId,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId,
    e.location,
    e.userAgent
FROM stage_events as e
JOIN stage_songs as s
    ON (e.artist = s.artist_name)
    AND (e.song = s.title)
    AND (e.length = s.duration)
    WHERE e.page= 'NextSong'
    AND e.sessionId NOT IN (
        SELECT DISTINCT
        session_id
        FROM songplays
    )
    ;
""")

user_table_insert = ("""
INSERT INTO users
(user_id,last_name,first_name,gender,level) 
(SELECT DISTINCT
    userId,
    lastName,
    firstName,
    gender,
    level
FROM stage_events
WHERE page='NextSong'
    AND userId NOT IN (
        SELECT DISTINCT 
        user_id
        FROM users
    )
    );
""")

song_table_insert = ("""
INSERT INTO songs
(song_id,title,artist_id,year,duration)
(SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM stage_songs
WHERE song_id NOT IN (
    SELECT DISTINCT
    song_id
    FROM songs
)
);
""")

artist_table_insert = ("""
INSERT INTO artists 
(artist_id,artist_name, artist_location,artist_latitude,artist_longitude) 
(SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM stage_songs
WHERE artist_id NOT IN (
    SELECT DISTINCT
    artist_id
    FROM artists
)
) ;
""")

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)  
SELECT DISTINCT
    a.ts,
    EXTRACT(HOUR FROM DATE(a.ts)),
    EXTRACT(DAY FROM DATE(a.ts)),
    EXTRACT(WEEK FROM DATE(a.ts)),
    EXTRACT(MONTH FROM DATE(a.ts)),
    EXTRACT(YEAR FROM DATE(a.ts)),
    EXTRACT(WEEKDAY FROM DATE(a.ts))
    FROM stage_events as a
    WHERE a.ts NOT IN (
        SELECT DISTINCT
        ts
        FROM time
    )
    ;


""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
