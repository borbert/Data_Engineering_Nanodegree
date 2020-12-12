import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as f
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Use SparkSession to create a spark sesssion.
    
    Returns:  
        Spark Session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Process the songs data from the file(s) specified in the parameters.
    
    Args:
        spark: the spark session
        input_data: 
        output_data:
        
    Returns:
        modeled data from songs json files that are written to parquet files back on S3
        
    '''
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select(
        'artist_id',
        col('artist_name').alias('name'),
        col('artist_location').alias('location'),
        col('artist_latitude').alias('latitude'),
        col('artist_longitude').alias('longitude'),
    ).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    '''
    Process the log data from the file(s) specified in the parameters.
    
    Args:
        spark: the spark session
        input_data: 
        output_data:
    
    Returns:
        modeled data from logs and songs json files that are written to parquet files back on S3
    '''
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table =  df.select(
            col('userId').alias('user_id'),
            col('firstName').alias('first_name'),
            col('lastName').alias('last_name'),
            col('gender').alias('gender'),
            col('level').alias('level')
        ).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column  
    df = df.withColumn(
        'timestamp', 
        f.to_timestamp(f.from_unixtime((col('ts') / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp")
    )
    
    # create datetime column from original timestamp column
    df = df.withColumn(
        'ts_datetime', 
        f.to_datetime(col['ts']).cast('Datetime')
    )

    # extract columns to create time table
    time_table = df.withColumn("hour", hour(col("timestamp"))) \
          .withColumn("day", dayofmonth(col("timestamp"))) \
          .withColumn("week", weekofyear(col("timestamp"))) \
          .withColumn("month", month(col("timestamp"))) \
          .withColumn("year", year(col("timestamp"))) \
          .withColumn("weekday", datetime.datetime(col("timestamp")).weekday()) \
          .select(
            col("timestamp").alias("start_time"),
            col("hour"),
            col("day"),
            col("week"),
            col("month"),
            col("year"),
            col("weekday")
          )
    
    # write time table to parquet files partitioned by year and month
    time_table.parquet(output_data + "time.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.withColumn('songplay_id', F.monontonically_increasing_id()).join(song_df, song_df.title== df.song).select(
        'songplay_id'
        ,col().alias('start_time')
        ,col('userId').alias('user_id')
        ,'level'
        ,'song_id'
        ,'artist_id'
        ,col('sessionId').alias('session_id')
        ,'location'
        ,col('userAgent').alias('user_agent')
    )
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays.parquet", mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify_datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
