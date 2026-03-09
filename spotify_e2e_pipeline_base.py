import json
import os
import boto3
from datetime import datetime
import requests
import spotipy
from spotipy.exceptions import SpotifyException
# from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials

def lambda_handler(event, context):

    client_id = os.environ['ClientID']
    client_secret = os.environ['Clientsecret']
    playlist_link = os.environ['playlist_link']
    # redirect_uri = os.environ['redirect_uri']
    # scope = os.environ['scope']
    refresh_token = os.environ['refresh_token']

    # refrsh access token withouot browser redirect
    auth_response = requests.post(
        "https://accounts.spotify.com/api/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret
        }
    )

    # raise exception if the access token is not generated
    auth_response_data = auth_response.json()
    if 'access_token' not in auth_response_data:
        raise Exception(f"Failed to refresh token: {auth_response_data}")
        
    access_token = auth_response_data['access_token']

    #---------------------------------------------------------------------------------------------------------
    """
    No need of the SpotifyClientCredentials object since the access token is refreshed without browser redirect 
    For public playlists (No access token is required), use SpotifyClientCredentials as below and 
    For private playlists (access token is required), use the code above to refresh access token
    """
    #---------------------------------------------------------------------------------------------------------

    # setting up client credentials manager (For PUBLC playlists)
    # client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    # sp = spotipy.Spotify(auth_manager = client_credentials_manager)

    # setting up client credentials manager (For PRIVATE playlists)
    # sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
    #                                            client_secret=client_secret,
    #                                            redirect_uri=redirect_uri,
    #                                            scope=scope))
    #---------------------------------------------------------------------------------------------------------

    # set up spotify client
    sp = spotipy.Spotify(auth=access_token)

    # extracting playlist id from link
    playlist_uri = playlist_link.split("/")[-1].split("?")[0]

    # spotify_data = sp.playlist_tracks(playlist_uri) this only extracts the data for first 100 tracks.
    # To extract the data for more than 100 tracks, below approach is used

    try: 
        results = sp.playlist_tracks(playlist_uri)
        tracks = results['items']

        while results['next']:
            results = sp.next(results)
            tracks.extend(results['items'])
        spotify_data = tracks

    except SpotifyException as e:
        print(f"Spotify API error: {e}")
        raise
    
    client = boto3.client('s3')

    file_name = 'spotify_raw' + datetime.now().strftime("%Y%m%d_%H%M%S") + '.json'

    # save the file to S3
    client.put_object(
        Bucket='spotify-etl-data-bucket', 
        Key='raw_data/unprocessed_data/'+file_name, 
        Body=json.dumps(spotify_data)
    )

    return {
        "statusCode": 200,
        "body": json.dumps("Playlist data dumped successfully")
    }
