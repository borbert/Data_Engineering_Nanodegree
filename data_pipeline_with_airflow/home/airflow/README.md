Sparkify Analytics Data Pipeline
===============================================

Sparkify the music streaming startup wants to convert their current ETL pipeline into Apache Airflow for automation and monitoring of the process.

This data pipeline will take the Sparkify streaming app data, in the form of log files, from AWS S3 buckets into a data warehouse, AWS Redshift to support their data analytics.  The scripts that run as a part of this data pipeline will load the data from S3 into stanging tables before modeling them into a database with a star schema.  

## Technologies Used
---------
* Python 3.6.3
* Apache Airflow
* Amazon Web Services (AWS) Redshift
* AWS S3

## Locations of Application Data
----
* Log data: 
     s3://udacity-dend/log_data
* Song data:
     s3://udacity-dend/song_data

## AWS IAM Setup
-----------------------------
* Create AWS IAM user to access your Redshift cluster.  
* Attach permission policies AmazonRedshiftFullAccess and AmazonS3ReadOnlyAccess.
* Make sure you have (or create) and access key.  Take note or download the access key ID and secret access key.

## AWS Redshift Cluster Setup
-----------------------------
* You will need to create an Redshift cluster before running the ETL pipeline.  
* Stick with suggested options but note the cluster endpoint, default schema, cluster user, and password.

## Creating the Tables in the AWS Redshift Cluster for First Use
-----------------------------
* Run the following code in the Redshift Query Editor to create the staging songs table
 ``` sql
CREATE TABLE public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);
 ```
* Run the following code in the Redshift Query Editor to create the staging events table
 ``` sql
 CREATE TABLE public.staging_events (
    	artist varchar(256),
	   auth varchar(256),
	   firstname varchar(256),
	   gender varchar(256),
	   iteminsession int4,
	   lastname varchar(256),
	   length numeric(18,0),
	   "level" varchar(256),
	   location varchar(256),
	   "method" varchar(256),
	   page varchar(256),
	   registration numeric(18,0),
	   sessionid int4,
	   song varchar(256),
	   status int4,
	   ts int8,
	   useragent varchar(256),
	   userid int4
    );
```
* Run the following code in the Redshift Query Editor to create the time deminsion table
``` sql
CREATE TABLE public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
); 
```
* Run the following code in the Redshift Query Editor to create the users deminsion table
``` sql
CREATE TABLE public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);
```
* Run the following code in the Redshift Query Editor to create the artists deminsion table
``` sql
CREATE TABLE public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);
```
* Run the following code in the Redshift Query Editor to create the songs deminsion table

``` sql
CREATE TABLE public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);
```



* Run the following code in the Redshift Query Editor to create the songplays fact table
``` sql
CREATE TABLE public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);
```


## Setting up the Airflow Envirnoment

* Launch the airflow workspace by running:
     /opt/airflow/start.sh

## Add in the connections to AWS Redshift
1. Create a connection with your IAM user AWS credentials.  The connection id needs to be aws_credentials in order for the pipline to work.
2. Create another connection to Redshift using the Redshfit endpoint, default schema, default and user.

## Resulting Data Pipeline

The resulting data pipeline will resemble this diagram:

<p align="center"
  <img width="400" height="400" src="images/DAG.png"
</p

## Database Schema

The database schema is a star schema with the songplays table as the fact table and the other tables acting as dimensions of that table.

<p align="center"
  <img width="400" height="400" src="images/sparkifyERD.png"
</p

## Database Schema
-------

## Songplays Table (FACT TABLE)
| Column      | Data Type             | Conditions  |
|-------------|-----------------------|-------------|
| songplay_id | integer IDENTITY | PRIMARY KEY |
| start_time  | timestamp          | NULLS       |
| user_id     | integer               | NULLS       |
| level       | varchar               | NULLS       |
| song_id     | varchar               | NULLS       |
| artist_id   | varchar               | NULLS       |
| session_id  | integer               | NULLS       |
| location    | varchar               | NULLS       |
| user_agent  | varchar               | NULLS       |


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
| start_time | timestamp | PRIMARY KEY |
| hour       | integer   | NULLS      |
| day        | integer   | NULLS      |
| week       | integer   | NULLS      |
| month      | integer   | NULLS      |
| year       | integer   | NULLS      |
| weekday    | integer   | NULLS      |
 