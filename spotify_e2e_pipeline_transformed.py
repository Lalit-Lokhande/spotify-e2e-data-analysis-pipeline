import json
import boto3
import pandas as pd 
from datetime import datetime
from io import StringIO

# extract data for albums
def album(data):
    album_lst = []
    for album_data in data:
        album_id = album_data['item']["album"]["id"]
        album_name = album_data['item']["album"]["name"]
        album_release_date = album_data['item']["album"]["release_date"]
        album_tot_tracks = album_data['item']["album"]["total_tracks"]
        album_url = album_data['item']["album"]["uri"]
        album_info = {"album_id": album_id, "album_name": album_name, "album_release_date": album_release_date, "album_tot_tracks": album_tot_tracks, "album_url":album_url}
        album_lst.append(album_info)
    return album_lst

# extract data for artists
def artist(data):
    artist_lst = []
    for item_data in data:
        artist_info = {
            "artist_id": item_data['item']['album']['artists'][0]['id'], 
            "artist_name": item_data['item']['album']['artists'][0]['name'], 
            "artist_external_url": item_data['item']['album']['artists'][0]['href']
        }
        artist_lst.append(artist_info)
    return artist_lst

# extract data for songs
def song(data):
    song_lst = []
    for song_data in data:
        song_info = {
            "song_id" : song_data['item']["id"],
            "song_name" : song_data['item']["name"],
            "song_duration" : song_data['item']["duration_ms"],
            "song_url" : song_data['item']["external_urls"]["spotify"],
            "song_added" : song_data["added_at"],
            "song_album_id" : song_data['item']["album"]["id"],
            "song_artist_id" : song_data['item']["album"]["artists"][0]["id"]
        }
        song_lst.append(song_info)
    return song_lst


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-data-bucket'
    Key = 'raw_data/unprocessed_data/'
    spotify_data = []
    spotify_key = []

    # get the file name and file contents in the S3 bucket
    for obj_cont in s3.list_objects_v2(Bucket=Bucket, Prefix=Key)['Contents']:
        if obj_cont['Key'].endswith('.json'):
            file_key = obj_cont['Key']
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObj=json.loads(content.read())
            spotify_data.append(jsonObj)
            spotify_key.append(file_key)


    # data transformations using Pandas
    for data in spotify_data:
        album_data = album(data)
        artist_data = artist(data)
        song_data = song(data)

        #convert to dataframe
        album_df = pd.DataFrame.from_dict(album_data)
        artist_df = pd.DataFrame.from_dict(artist_data)
        song_df = pd.DataFrame.from_dict(song_data)

        #remove duplicates
        album_df.drop_duplicates(inplace=True)
        artist_df.drop_duplicates(inplace=True)
        song_df.drop_duplicates(inplace=True)

        #convert timestamp to datetime and remove invalid/corrupted data
        song_df["song_added"] = pd.to_datetime(song_df["song_added"])

        album_df = album_df.loc[album_df['album_release_date'].str.match(r'^\d{4}-\d{2}-\d{2}$')]
        album_df["album_release_date"] = pd.to_datetime(album_df.loc[:,"album_release_date"])

        song_df = song_df.loc[song_df['song_name']!='']


        #upload to s3
        song_key = "transformed_data/song_data/song_data_transformed_" + datetime.now().strftime('%Y%m%d_%H%M%S') + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_data = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_data)

        album_key = "transformed_data/album_data/albums_data_transformed_" + datetime.now().strftime('%Y%m%d_%H%M%S') + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_data = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_data)

        artist_key = "transformed_data/artist_data/artists_data_transformed_" + datetime.now().strftime('%Y%m%d_%H%M%S') + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_data = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_data)

    # copy the file in another folder in the same bucket
    s3_clnt = boto3.client('s3')
    for file in spotify_key:
        s3_clnt.copy_object(
            Bucket = Bucket,
            CopySource = {'Bucket' : Bucket, 'Key': file},
            Key = 'raw_data/proccessed_data/file_' + file.split('/')[-1]
        )

    # delete the file from the unprocessed data folder 
    for file in spotify_key:
        s3.delete_object(
            Bucket = Bucket,
            Key = file
        )

    return{
        "statusCode": 200,
        "body": json.dumps("Successfully loaded the files to S3.")
    }