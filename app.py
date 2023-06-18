from tkinter import image_names
from dash import Dash, html, dash_table, dcc, callback, Output, Input
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import dash
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template

#setting-up spotipy
CLIENT_ID = ""
CLIENT_SECRET = ""

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET))

#-----------------------------------------------------------------------------------
df_album_info = pd.read_csv('path/to/album_info.csv file')
df_album_tracks = pd.read_csv('path/to/album_tracks.csv file')

#initial top 10 songs graph
df_top10_songs = df_album_tracks.sort_values(['popularity'], ascending=False)
df_top10_songs = df_top10_songs.drop_duplicates(subset='tracks_name', keep='first')
df_top10_songs = df_top10_songs.head(10)
df_top10_songs.reset_index

load_figure_template('VAPOR')

INITIAL_FIG = px.bar(df_top10_songs, x="popularity" ,y="tracks_name", title='Top new release songs')

# Initialize the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.VAPOR])


#App layout
app.layout = html.Div(
    children = [
        html.Div(
            children = [
                html.H1(['Spotipy - Dashboard'])
            ], style={"text-align": "center"}
        ),
        html.Div(
            children = [
                html.H4(id='song-selected')
            ], style={"text-align": "left", 'margin-top' : '20px', 'margin-left' : '20px'}
        ),
        html.Div(
            children = [
                html.Div([
                    dcc.Graph(id="bar_graph", figure=INITIAL_FIG)
                ], style={'width': '46.8%', 'display' : 'inline-block', 'margin-left': '10px'}),
                html.Div([
                    dcc.Graph(id="table_graph")
                ], style={'width' : '46.8%', 'display' : 'inline-block'})
            ]
        ), 
        html.Div(
            children = [
                html.Div([
                    dcc.Graph(id="line_graph")
                ], style={'width': '46.8%', 'display' : 'inline-block', 'margin-left': '10px'}),
                html.Div(
                    children = [
                        html.Div(children = [
                        html.H2(['Listen to the track']),
                        html.Img(id="track_img"),
                        html.Pre(id='track_link')], style={"display": "flex", "flex-direction": "column", "align-items": "center"},),
                    ], style={'width' : '46%', 'display' : 'inline-block', 'vertical-align' : 'top'})
            ]
        ), 
    ]
)


#CALLBACKS
@app.callback(
    Output(component_id='table_graph', component_property='figure'),
    Output(component_id='line_graph', component_property='figure'),
    Output(component_id='track_img', component_property='src'),
    Output(component_id='track_link', component_property='children'),
    Output(component_id='song-selected', component_property='children'),
    Input(component_id='bar_graph', component_property='hoverData')
)
def on_hover(hover_data):

    # print(hover_data)

    if not hover_data:

        # return everything for the top song
        target_data = df_top10_songs.iloc[0]


        song_select = str('Song selected is ' + target_data['tracks_name'])

        album_uri = target_data['album_uri']

        all_album_songs = df_album_tracks[df_album_tracks.album_uri == album_uri]

        #return for line graph
        graph_line = px.line(all_album_songs, x='tracks_name', y='popularity', markers=True, title='Popularity of the tracks from the same album.')

        #get top songs from the artist
        artist_uri = target_data['artist_uri']
        top_artist_songs = spotify.artist_top_tracks(artist_uri)

        df_top_artist_songs = pd.DataFrame(columns=['Rank', 'Song Name'])
        for track in top_artist_songs['tracks'][:10]:
            pos = len(df_top_artist_songs)
            df_top_artist_songs.loc[pos, 'Rank'] = pos+1
            df_top_artist_songs.loc[pos, 'Song Name'] = track['name']

        #return for table
        top_songs_table = go.Figure(data=[go.Table(
                        columnorder = [1,2],
                        columnwidth = [80,100],
                        header=dict(values=list(df_top_artist_songs.columns),
                                    align='left'),
                        cells=dict(values=[df_top_artist_songs.Rank, df_top_artist_songs['Song Name']],
                                align='left'))
                    ])

        top_songs_table.update_layout(
                            title='Top Songs from the Artist'
                                )

        #Retrive the image url and song url
        image_url = target_data['tracks_image'] #<---------return this

        song_url = target_data['tracks_url']
        return_song_url = html.A('Link to song', href=song_url, target="_blank")

        return top_songs_table, graph_line, image_url, return_song_url, song_select

    else:
        # get the state
        song = hover_data["points"][0]["y"]
        print('##########################################')
        print(song)

        song_select = str('Song selected is ' + song)

        album_uri = df_top10_songs[df_top10_songs.tracks_name == song]['album_uri'].reset_index()

        album_uri = album_uri.iloc[:, 1].astype(str)

        all_album_songs = df_album_tracks[df_album_tracks.album_uri == album_uri[0]]

        #return for line graph
        graph_line = px.line(all_album_songs, x='tracks_name', y='popularity', markers=True, title='Popularity of the tracks from the same album.')

        #get top songs from the artist
        artist_uri = df_top10_songs[df_top10_songs.tracks_name == song]['artist_uri'].reset_index()
        artist_uri = artist_uri.iloc[:, 1].astype(str)
        top_artist_songs = spotify.artist_top_tracks(artist_uri[0])

        df_top_artist_songs = pd.DataFrame(columns=['Rank', 'Song Name'])
        for track in top_artist_songs['tracks'][:10]:
            pos = len(df_top_artist_songs)
            df_top_artist_songs.loc[pos, 'Rank'] = pos+1
            df_top_artist_songs.loc[pos, 'Song Name'] = track['name']

        #return for table
        top_songs_table = go.Figure(data=[go.Table(
                        columnorder = [1,2],
                        columnwidth = [80,100],
                        header=dict(values=list(df_top_artist_songs.columns),
                                    align='left'),
                        cells=dict(values=[df_top_artist_songs.Rank, df_top_artist_songs['Song Name']],
                                align='left'))
                    ])

        top_songs_table.update_layout(
                    title='Top Songs from the Artist'
                        )

        #Retrive the image url and song url
        image_url = df_album_tracks[df_album_tracks.tracks_name == song]['tracks_image'].reset_index()
        image_url = image_url.iloc[:, 1].astype(str) #<---------return this

        song_url = df_album_tracks[df_album_tracks.tracks_name == song]['tracks_url'].reset_index()
        song_url = song_url.iloc[:, 1].astype(str)
        return_song_url = html.A('Link to song', href=song_url[0], target="_blank")


        return top_songs_table, graph_line, image_url[0], return_song_url, song_select


# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)