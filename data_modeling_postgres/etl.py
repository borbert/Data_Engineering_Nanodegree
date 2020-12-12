import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_columns=['song_id','title','artist_id', 'artist_name','year','duration']
    df['duration']=df['duration'].values.astype(float)
    df['year']=df['year'].values.astype(int)
    song_data=[list(row) for row in df[song_columns].itertuples(index=False)][0]
    
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    df[['artist_latitude','artist_longitude']]=df[['artist_latitude','artist_longitude']].values.astype(float)
    artist_columns=['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artist_data = [list(row) for row in df[artist_columns].itertuples(index=False)][0]
    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    This procedure processes the log files. The filepath is passed as an argument to this function.
    The files in the location of the filepath will be processed to insert into the time, user, and songplays tables 
    respectively.
    
    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    '''
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts']=pd.to_datetime(df['ts'],unit='ms')
    t=df
    
    # insert time data records
    #map iterators to decompose the timestamp to variables 
    time,hour,day,week,month,year,weekday= zip(*[(d.time(),d.hour,d.day,d.week,d.month,d.year,d.weekday()) for d in t['ts']])
    #use varibles (iterators) to decompose the timestamp and create columns at the same time
    time_data = t[['ts']].assign(time=time,hour=hour,day=day,week=week,month=month,year=year,weekday=weekday)
    #drop the original timestamp becuase it is fully decomposed
    time_data.drop(columns='ts', inplace=True)
#     time_data = 
#     column_labels = 
    time_df = time_data #.rename(columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_columns=['userId', 'lastName','firstName','gender','level']
    user_df = df[user_columns].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        results=cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else None, None

        start_time=row.ts.time()
        
        # insert songplay record
        songplay_data = (start_time,row.userId ,row.level ,songid,artistid ,row.sessionId ,row.location ,row.userAgent, row.song, row.artist, row.length)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    This function will find all of the file(s) in the given filepath and process them accordingly.  This includes data
    transformation and insertion into the database.  
    
    INPUTS:
    * cur is the cursor to the postgres db 
    * conn is the connection to the postgres db 
    * filepath is the filepath to the file to be processed
    * func is the type of processing function applied to the file, i.e. process_song_file or process_log_file
    
    RETURNS:
    Only a print to console with the number of files processed out of the total.
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()