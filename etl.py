import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime

# def function convert to array time format for time table  
def convert_to_time_data(t):
    """
    This procedure to convert datetime to array with struct (timestamd,hour,day,week,month,year,weekday).
     INPUTS: 
    * t datetime value 
    """
    return [t,t.hour,t.day,t.isocalendar()[1],t.month,t.year,t.weekday()]
def process_song_file(cur, filepath):
      """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
     """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the log information in order to store it into the time table,user table, songplay table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page'] =='NextSong']

    # convert timestamp column to datetime
    df['timestamd'] = df['ts'].apply(lambda x: datetime.fromtimestamp(x/1000.0))
    t = df[['timestamd']].copy()
    
    # insert time data records
    #time = t
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(columns=column_labels)
    for item, value in t['timestamd'].items():
        time_df.loc[len(time_df.index)] = convert_to_time_data(value)
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.timestamd, row.userId, row.level, str(songid), str(artistid), row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.

    INPUTS: 
    * cur the cursor variable
    * conn the connection to the database
    * filepath the file path to the song file
    * func function call
    """
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
    """
    - Connect to the sparkifydb database
    - Process song data
    - Process log data
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()