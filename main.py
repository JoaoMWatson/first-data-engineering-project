import json
import sqlite3
import requests
import datetime
import sqlalchemy
import pandas as pd

DATABASE_LOCATION = 'sqlite://my_played_tracks.sqlite'
USER_ID = ''
TOKEN = "BQDEL3vjmfdJKQnQ1NKfKKWZpiUyn9aihwv6xGlJ7oOy1BaNSMcb2NT-urxIoEE0Fj5JTIbv8UycmEIdKXQCn-lCkRyyCQqNvtyNaEcPTKmHsG8LtuWDsjQNtjhIfkeHBvmd65MP1rLH99LOlldrCUqPVORMjODSE-up6Kk5"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    """Checks if dataframe have errors

    Args:
        df (pd.DataFrame): DataFrame of musics

    Returns:
        bool: Is empty
    """
    # Empty check
    if df.empty:
        print('No songs downloaded. Finishing execution')
        return False

    # Primary key check
    if df.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key constrains violation")
    
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")
    
    # Check that all timestamp are for yesterday
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    timestamps = df['timestamps'].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y=%m-%d") != yesterday:
            raise Exception("At lest one of the returned songs does not come from within the last 24 hours")
    
    

if __name__ == "__main__":
    headers = {
        "Accept": 'application/json',
        "Content-Type": "application/json",
        "Authorization":f"Bearer {TOKEN}"
    }
    
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_time = int(yesterday.timestamp())
    
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after{time}".format(time=yesterday_unix_time), headers=headers)
    
    data = r.json()
    
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    
    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])
        
    song_dict = {
        'song_names':song_names,
        'artist_names' :artist_names, 
        'played_at_list' :played_at_list, 
        'timestamps' :timestamps, 
    }
    
    song_df = pd.DataFrame(song_dict, columns = ['song_names','artist_names','played_at_list','timestamps'])
    
    # Validate
    if check_if_valid_data(song_df):
        print('Data valid, proceed to Load stage')
        
    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()
    
    sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestramp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY_KEY (played_at)
        )
    """
    
    cursor.execute(sql_query)
    print('Database created successfully')
    
    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print('Data already exists')
    
    conn.close()
    print('Connection closed')