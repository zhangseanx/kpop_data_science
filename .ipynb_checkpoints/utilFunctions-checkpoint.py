import spotipy 
import psycopg2
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from configparser import ConfigParser


def config(section:str, filename='secrets.ini'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db

def initialize_sp(scope, client_id, client_secret, redirect_uri):
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope, client_id, client_secret,redirect_uri))
    return sp


def getplayListDict(user_id):
    playlists = sp.user_playlists(user_id)
    names = [x['name'] for x in playlists['items']]
    ids = [x['id'] for x in playlists['items']]
    return dict(zip(names, ids))

def getTrackInfoFromPlaylist(user_id, playlist_id):
    offset = 0
    songs = []
    genres = []
    while True:
        content = sp.user_playlist_tracks(user_id, playlist_id, limit = 50, offset=offset)
        songs += content['items']
        if content['next'] is not None:
            offset+=50
        else:
            break
            
    names = [x['track']['name'] for x in songs]
    ids = [x['track']['id'] for x in songs]
    artists = [x['track']['artists'][0]['name'] for x in songs]
    popularity = [x['track']['popularity'] for x in songs]
    
    df = pd.DataFrame({'Track Name': names, 'Track ID': ids, 'Artist': artists, 'Track popularity': popularity})
    return df

def getArtistInfoFromPlaylist(user_id, playlist_id):
    offset = 0
    songs = []
    genres = []
    while True:
        content = sp.user_playlist_tracks(user_id, playlist_id, limit = 50, offset=offset)
        songs += content['items']
        if content['next'] is not None:
            offset+=50
        else:
            break
            
    artist_ids = [x['track']['artists'][0]['id'] for x in songs]
    artist_ids = filter(None, artist_ids)
    
    n_calls = 0
    artists = []
    for artist_id in artist_ids:
        try:
            content = sp.artist(artist_id)
            artists.append(content)
        except Exception as e:
            print(artist_id)
            print(e)
    try:
        followers = [x['followers']['total'] for x in artists]
        genres = [x['genres'] for x in artists]
        popularity = [x['popularity'] for x in artists] 
        name = [x['name'] for x in artists]
        ids = [x['id'] for x in artists]
    except Exception as e:
        print(e)
    
    df = pd.DataFrame({'Artist ID':ids, 'Artist': name,'Artist Followers':followers, 'Artist genres': genres, 'Artist popularity':popularity})
    return df

def getAudioFeatures(sp, track_id_list, limit=50):
    features = []
    index = 0
    while index < len(track_id_list):
        features += sp.audio_features(track_id_list[index:index+limit])
        index += limit
    return features

#Initialize Spotify API
spotify_secrets = config('spotify')
cid = spotify_secrets['cid']
secret = spotify_secrets['secret'] 
user_id = spotify_secrets['user_id']
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret) 
refresh_auth = True
URI = 'http://localhost/'