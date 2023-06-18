from __future__ import annotations

import logging
import shutil
import sys
import tempfile
import time
from pprint import pprint
import os

import pendulum
import boto3
import pandas as pd
import numpy as np
from io import StringIO
import json

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from airflow.models import Variable

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

BASE_DIR = tempfile.gettempdir()


def x():
    pass
ACCESS_ID = Variable.get("aws_access_id")
ACCESS_KEY = Variable.get("aws_secret_access_key")
bucket = 'spotify-airflow-project'


with DAG(
    dag_id="my_dag",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["project"],
) as dag:

    # [START howto_operator_python]
    @task(task_id="get_streaming_data_and_put_in_s3")
    def get_streaming_data_and_put_in_s3(ds=None, **kwargs):

        spotipy_client_id = Variable.get("SPOTIPY_CLIENT_ID")
        spotipy_client_secret = Variable.get("SPOTIPY_CLIENT_SECRET")

        spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=spotipy_client_id, client_secret=spotipy_client_secret))

        new_albums = spotify.new_releases(country=None, limit=50, offset=0)

        df_album_info = pd.DataFrame(columns=['album_name', 'album_uri', 'artist_name', 'artist_uri', 'track_image'])

        for item in new_albums['albums']['items']:
                    # rows_to_add = []
                    for artists in item['artists']:
                        pos = len(df_album_info)
                        df_album_info.loc[pos, 'album_name'] = item['name']
                        df_album_info.loc[pos, 'album_uri'] = item['uri']
                        df_album_info.loc[pos, 'artist_name'] = artists['name']
                        df_album_info.loc[pos, 'artist_uri'] = artists['uri']
                        df_album_info.loc[pos, 'tracks_image'] = item['images'][1]['url']

        # getting tracks info

        df_album_tracks = pd.DataFrame(columns=['album_name', 'album_uri', 'artist_name', 'artist_uri','tracks_name', 
                                                            'tracks_uri', 'track_image', 'track_url', 'popularity'])
        for index, rows in df_album_info.iterrows():
            album_uri = rows['album_uri']
            results_album = spotify.album_tracks(album_uri, limit=50, offset=0, market=None)
            
            for item in results_album['items']:
                track_uri = item['uri']
                results_tracks = spotify.track(track_uri, market=None)

                pos = len(df_album_tracks)
                df_album_tracks.loc[pos, 'album_name'] = rows['album_name']
                df_album_tracks.loc[pos, 'album_uri'] = rows['album_uri']
                df_album_tracks.loc[pos, 'artist_name'] = rows['artist_name']
                df_album_tracks.loc[pos, 'artist_uri'] = rows['artist_uri']
                df_album_tracks.loc[pos, 'tracks_name'] = item['name']
                df_album_tracks.loc[pos, 'tracks_uri'] = item['uri']
                df_album_tracks.loc[pos, 'tracks_image'] = rows['tracks_image']
                df_album_tracks.loc[pos, 'tracks_url'] = results_tracks['external_urls']['spotify']
                df_album_tracks.loc[pos, 'popularity'] = results_tracks['popularity']

        bucket = 'spotify-airflow-project' # already created on S3
        csv_buffer = StringIO()
        df_album_info.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3', aws_access_key_id=ACCESS_ID, aws_secret_access_key= ACCESS_KEY)
        s3_resource.Object(bucket, 'raw_data/raw_data_album.csv').put(Body=csv_buffer.getvalue())
        csv_track_buffer = StringIO()
        df_album_tracks.to_csv(csv_track_buffer)
        s3_resource.Object(bucket,'raw_data/raw_data_track.csv').put(Body=csv_track_buffer.getvalue())

    run_streaming_data_and_put_in_s3 = get_streaming_data_and_put_in_s3()
    # [END howto_operator_python]

    @task(task_id="run_transform_data")
    def transforming_data(df=None, **kwargs):
        print("transform data done!!!")
    run_transform_data = transforming_data()

    @task(task_id="run_move_data")
    def move_data():
        
        s3_resource = boto3.resource('s3', aws_access_key_id=ACCESS_ID, aws_secret_access_key= ACCESS_KEY)
        new_album_file_name = 'old_raw_data/raw_data_album'+ str(pendulum.now("UTC").format('YYYY-MM-DD'))+'.csv'
        new_track_file_name = 'old_raw_data/raw_data_track'+ str(pendulum.now("UTC").format('YYYY-MM-DD'))+'.csv'

        #copying first
        s3_resource.Object(bucket, new_album_file_name).copy_from(CopySource='spotify-airflow-project/raw_data/raw_data_album.csv')
        s3_resource.Object(bucket, new_track_file_name).copy_from(CopySource='spotify-airflow-project/raw_data/raw_data_track.csv')

        #deleting former 
        s3_resource.Object(bucket, 'raw_data/raw_data_album.csv').delete()
        s3_resource.Object(bucket, 'raw_data/raw_data_track.csv').delete()
    run_move_data = move_data()

    # run_streaming_data_and_put_in_s3 >> run_get_data_from_s3 >> run_transform_data >> run_move_data
    run_streaming_data_and_put_in_s3 >> run_transform_data >> run_move_data