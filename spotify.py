import random
import requests
import json
from datetime import datetime, timedelta
import psycopg2
from dotenv import load_dotenv
import os
import base64

load_dotenv()

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

def get_token():
    auth_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "client_credentials"
    }
    response = requests.post(url, headers=headers, data=payload)
    result = json.loads(response.text)
    token = result["access_token"]
    return token

token = get_token()

chartToken = os.getenv("CHART_TOKEN")
# Informations de connexion à la base de données
conn = psycopg2.connect(database = os.getenv("DB_NAME"),
                        user = os.getenv("DB_USER"),
                        host= os.getenv("DB_HOST"),
                        password = os.getenv("DB_PASSWORD"),
                        port = 5432)

def tableCreation():
    # Créer un curseur pour exécuter des commandes SQL
    cur = conn.cursor()

    # use search path musique
    cur.execute('set search_path to musique')

    # Créer la table 'artists'
    cur.execute('''
        CREATE TABLE IF NOT EXISTS artists (
            id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )
    ''')

    # Créer la table 'albums'
    cur.execute('''
        CREATE TABLE IF NOT EXISTS albums (
            id VARCHAR(50) PRIMARY KEY,
            type VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            total_tracks INTEGER,
            duration INTEGER,
            release_date DATE
        )'''
    )

    # Créer la table 'countries'

    # cur.execute('DROP TABLE IF EXISTS countries CASCADE')

    # Créer la table 'rank'
    # cur.execute('DROP TABLE IF EXISTS dailyrankings')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS dailyrankings (
            id SERIAL PRIMARY KEY,
            rank INTEGER,
            country_code VARCHAR(3),
            track_id VARCHAR(50) REFERENCES tracks(id),
            streaming_quantity INTEGER,
            date DATE
        )
    ''')

    # Créer la table 'tracks_artists'
    cur.execute('''
                CREATE TABLE IF NOT EXISTS tracks_artists (
                    track_id VARCHAR(50) REFERENCES tracks(id),
                    artist_id VARCHAR(50) REFERENCES artists(id),
                    PRIMARY KEY (track_id, artist_id)
                )
    ''')

    # Créer la table 'album_genre'
    cur.execute('''
        CREATE TABLE IF NOT EXISTS album_genre (
            album_id VARCHAR(50) REFERENCES albums(id),
            genre_id INTEGER REFERENCES genre(id),
            PRIMARY KEY (album_id, genre_id)
        )
    ''')

    # Créer la table 'genre'
    cur.execute('''
        CREATE TABLE IF NOT EXISTS genre (
            id serial PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )
    ''')

    # Créer la table 'tracks'
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            duration INTEGER,
            album_id VARCHAR(50) REFERENCES albums(id),
            popularity INTEGER,
            release_date DATE
        )
    ''')

def getSpotifyDailyRanking(date,country):
    url = f"https://charts-spotify-com-service.spotify.com/auth/v0/charts/regional-{country}-daily/{date}"
    headers = {
    'authority': 'charts-spotify-com-service.spotify.com',
    'accept': 'application/json',
    'accept-language': 'fr,en-US;q=0.9,en;q=0.8,fr-FR;q=0.7,nl;q=0.6,ja;q=0.5,tr;q=0.4',
    'app-platform': 'Browser',
    'authorization': f'Bearer {chartToken}',
    'content-type': 'application/json',
    'origin': 'https://charts.spotify.com',
    'referer': 'https://charts.spotify.com/',
    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'spotify-app-version': '0.0.0.production',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers)
    if 'Token expired' in response.text:
        print("Token expired for charts-spotify")
    else :
        data = json.loads(response.text)
        entries = data["entries"]
        for entry in entries:
            entryData = entry["chartEntryData"]
            rank = entryData["currentRank"]
            streams = entryData["rankingMetric"]["value"]
            trackName = entry["trackMetadata"]["trackName"]
            trackId = entry["trackMetadata"]["trackUri"].split(":")[2]
            trackId = getSpotifyTrack(country,trackId)
            ranking = {}
            ranking['track_id'] = trackId
            ranking['streaming_quantity'] = streams
            ranking['date'] = date
            ranking['country_code'] = country.upper()
            ranking['rank'] = rank
            createDailyRankingIfNotExist(ranking)
            artists = entry["trackMetadata"]["artists"]
            for artist in artists:
                artistData = {}
                artistData['id'] = artist["spotifyUri"].split(":")[2]
                artistData['name'] = artist["name"]
                createArtistIfNotExist(artistData)
                createTrackArtistIfNotExist(trackId,artistData['id'])

# Pas utilisé mais à garder si on décide d'implémenter les classements hebdomadaires
def getSpotifyWeeklyRanking(date,city):
    url = f"https://charts-spotify-com-service.spotify.com/auth/v0/charts/citytoptrack-{city}-weekly/{date}"
    headers = {
    'authority': 'charts-spotify-com-service.spotify.com',
    'accept': 'application/json',
    'accept-language': 'fr,en-US;q=0.9,en;q=0.8,fr-FR;q=0.7,nl;q=0.6,ja;q=0.5,tr;q=0.4',
    'app-platform': 'Browser',
    'authorization': f'Bearer {chartToken}',
    'content-type': 'application/json',
    'origin': 'https://charts.spotify.com',
    'referer': 'https://charts.spotify.com/',
    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'spotify-app-version': '0.0.0.production',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers)
    if 'Token expired' in response.text:
        print("Token expired for charts-spotify")
    else :
        data = json.loads(response.text)
        entries = data["entries"]
        for entry in entries:
            entryData = entry["chartEntryData"]
            rank = entryData["currentRank"]
            streams = entryData["rankingMetric"]["value"]
            trackName = entry["trackMetadata"]["trackName"]
            trackId = entry["trackMetadata"]["trackUri"].split(":")[2]
            # TODO : finir la fonction pour les classements hebdomadaires

def getSpotifyTrack(country,trackId):
    url = f"https://api.spotify.com/v1/tracks?market={country.upper()}&ids={trackId}"
    headers = {
    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'Referer': 'https://developer.spotify.com/',
    'sec-ch-ua-mobile': '?0',
    'Authorization': f'Bearer {token}',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'sec-ch-ua-platform': '"Windows"'
    }

    response = requests.get(url, headers=headers)

    data = json.loads(response.text)
    tracks   = data["tracks"]
    for track in tracks:
        trackData = {}
        trackData['id'] = track["id"]
        trackData['name'] = track["name"]
        trackData['duration'] = track["duration_ms"]
        album_id = track["album"]["id"]
        createAlbumIfNotExist(getSpotifyAlbum(album_id))
        trackData['album_id'] = album_id
        album = getSpotifyAlbum(album_id)
        trackData['release_date'] = track["album"]["release_date"] if type(track["album"]["release_date"]) == datetime.date else None
        trackData['popularity'] = track["popularity"]
        createTrackIfNotExist(trackData)
        return trackData['id']

def getSpotifyAlbum(albumId):
    
    url = f"https://api.spotify.com/v1/albums/{albumId}"

    headers = {
        'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'Referer': 'https://developer.spotify.com/',
        'sec-ch-ua-mobile': '?0',
        'Authorization': f'Bearer {token}',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'
    }

    response = requests.get(url, headers=headers)
    try :
        data = json.loads(response.text)

        album = {}
        album["id"] = data["id"]
        album["type"] = data["album_type"]
        album["name"] = data["name"]
        album["total_tracks"] = data["total_tracks"]
        album["release_date"] = data["release_date"] if type(data["release_date"]) == datetime.date else None
        genres = data["genres"]
        for genre in genres:
            createAlbumGenreIfNotExist(genre,albumId)
        duration = 0
        for track in data["tracks"]["items"]:
            duration += track["duration_ms"]
        album["duration"] = duration
        return album
    except Exception as e:
        print(f"[Error] Album {albumId} not found")
        print(e)
        print(response.text)
        return None

def createTrackIfNotExist(track):
    # Créer un curseur pour exécuter des commandes SQL
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM tracks WHERE id = %s", (track['id'],))
    result = cur.fetchone()
    
    if result is None:
        insert_query = "INSERT INTO tracks (id, name, duration, album_id, release_date,popularity) VALUES (%s, %s, %s, %s, %s, %s)"
        
        try:
            cur.execute(insert_query, (
                track['id'],
                track['name'],
                track['duration'],
                track['album_id'],
                track['release_date'],
                track['popularity']
            ))
            
            conn.commit()
            print(f"Track {track['name']} added")
        except Exception as e:
            print(f"[Error] Track {track['id']} not added")
            print(e)
    else:
        print(f"Track {track['name']} already exists")

def createAlbumIfNotExist(album):
    # Créer un curseur pour exécuter des commandes SQL
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM albums WHERE id = '{album['id']}'")
    result = cur.fetchone()
    if result is None:
        insert_query = "INSERT INTO albums (id, type, name, total_tracks, release_date) VALUES (%s, %s, %s, %s, %s)"""
        cur.execute(insert_query, (
            album['id'],
            album['type'],
            album['name'],
            album['total_tracks'],
            album['release_date']
        ))
        conn.commit()
        print(f"Album {album['name']} added")
    else :
        print(f"Album {album['name']} already exist")

def createDailyRankingIfNotExist(ranking):
    # Créer un curseur pour exécuter des commandes SQL
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM dailyrankings WHERE date = '{ranking['date']}' AND track_id = '{ranking['track_id']} AND country_code = {ranking['country_code']}'")
    result = cur.fetchone()
    if result is None:
        insert_query = "INSERT INTO dailyrankings (country_code, track_id, streaming_quantity, date, rank) VALUES (%s, %s, %s, %s, %s)"
        try :    # Execute the query with the album data
            cur.execute(insert_query, (
                ranking['country_code'],
                ranking['track_id'],
                ranking['streaming_quantity'],
                ranking['date'],
                ranking['rank']
            ))
            conn.commit()
            print(f"Daily Ranking {ranking['track_id']} added (rank : {ranking['rank']}/200, country : {ranking['country_code']})")
        except Exception as e:
            print(f"[Error] Daily Ranking {ranking} not added")
            print(e)
    else :
        print(f"Daily Ranking {ranking['track_id']} already exist")

def createArtistIfNotExist(artist):
    # Créer un curseur pour exécuter des commandes SQL
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM artists WHERE id = '{artist['id']}'")
    result = cur.fetchone()
    if result is None:
        insert_query = "INSERT INTO artists (id, name) VALUES (%s,%s)"
        try :    # Execute the query with the album data
            cur.execute(insert_query, (
                artist['id'],
                artist['name'],
            ))
            conn.commit()
            print(f"Artist {artist['name']} added")
        except Exception as e:
            print(f"[Error] Artist {artist['name']} not added")
            print(e)
    else :
        print(f"Artist {artist['name']} already exist")

def createTrackArtistIfNotExist(track_id,artist_id):
    # Créer un curseur pour exécuter des commandes SQL
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM tracks_artists WHERE track_id = '{track_id}' AND artist_id = '{artist_id}'")
    result = cur.fetchone()
    if result is None:
        insert_query = "INSERT INTO tracks_artists (track_id, artist_id) VALUES (%s,%s)"
        try :    # Execute the query with the album data
            cur.execute(insert_query, (
                track_id,
                artist_id,
            ))
            conn.commit()
            print(f"Track artists {track_id}/{artist_id} added")
        except Exception as e:
            print(f"[Error] Track artists {track_id}/{artist_id} not added")
            print(e)
    else :
        print(f"Track artists {track_id}/{artist_id} already exist")

def createGenreIfNotExist(genre):
    # Créer un curseur pour exécuter des commandes SQL
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM genre WHERE name = '{genre}'")
    result = cur.fetchone()
    if result is None:
        insert_query = "INSERT INTO genre (name) VALUES (%s) RETURNING id"
        try :    # Execute the query with the album data
            genre_id = cur.execute(insert_query, (
                genre,
            ))
            conn.commit()
            print(f"Genre {genre} added")
            return genre_id
        except Exception as e:
            print(f"[Error] Genre {genre} not added")
            print(e)
    else :
        print(f"Genre {genre} already exist")

def createAlbumGenreIfNotExist(genre,album_id):
    # Créer un curseur pour exécuter des commandes SQL
    cur = conn.cursor()

    genre_id = cur.execute(f"SELECT id FROM genre WHERE name = '{genre}'")
    print(f"Genre id : {genre_id} pour {genre}")
    if genre_id is None:
        createGenreIfNotExist(genre)
        genre_id = cur.execute(f"SELECT id FROM genre WHERE name = '{genre}'")
    result = cur.fetchone()
    if result is None:
        insert_query = "INSERT INTO album_genre (album_id, genre_id) VALUES (%s,%s)"
        try :    # Execute the query with the album data
            cur.execute(insert_query, (
                album_id,
                genre_id,
            ))
            conn.commit()
            print(f"Album genre {album_id}/{genre_id} added")
        except Exception as e:
            print(f"[Error] Album genre {album_id}/{genre_id} not added")
            print(e)
    else :
        print(f"Album genre {album_id}/{genre_id} already exist")

def getAllCountriesAndCity():
    url = f"https://charts-spotify-com-service.spotify.com/auth/v0/charts/regional-fr-daily/2023-11-23"

    headers = {
    'authority': 'charts-spotify-com-service.spotify.com',
    'accept': 'application/json',
    'accept-language': 'fr,en-US;q=0.9,en;q=0.8,fr-FR;q=0.7,nl;q=0.6,ja;q=0.5,tr;q=0.4',
    'app-platform': 'Browser',
    'authorization': f'Bearer {chartToken}',
    'content-type': 'application/json',
    'origin': 'https://charts.spotify.com',
    'referer': 'https://charts.spotify.com/',
    'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'spotify-app-version': '0.0.0.production',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers)
    if 'Token expired' in response.text:
        print("Token expired for charts-spotify")
    else :
        data = json.loads(response.text)
        allCountries = []
        for country in data["countryFilters"]:
            if country["code"] != "GLOBAL":
                allCountries.append(country)
    
        allCities = {}

        for country in allCountries:
            print(f"Getting cities for {country['readableName']}")
            url = f"https://charts-spotify-com-service.spotify.com/auth/v0/charts/regional-{country['code']}-daily/2023-11-23"
            headers = {
            'authority': 'charts-spotify-com-service.spotify.com',
            'accept': 'application/json',
            'accept-language': 'fr,en-US;q=0.9,en;q=0.8,fr-FR;q=0.7,nl;q=0.6,ja;q=0.5,tr;q=0.4',
            'app-platform': 'Browser',
            'authorization': f'Bearer {chartToken}',
            'content-type': 'application/json',
            'origin': 'https://charts.spotify.com',
            'referer': 'https://charts.spotify.com/',
            'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'spotify-app-version': '0.0.0.production',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
            }

            response = requests.get(url, headers=headers)
            data = json.loads(response.text)
            if 'message' in data.keys():
                continue
            cities = data["chartNavigationFilters"][-1]['chartNavigationFilterEntries']
            for city in cities:
                if city['navigationFilterEntryText'] not in ["Weekly","Daily"] :
                    if country['code'] not in allCities.keys():
                        allCities[country['code']] = []
                    allCities[country['code']].append({
                        'name':city['navigationFilterEntryText'],
                        'code':city['alias'].split('_')[1].lower()
                    })
            
        # save all cities in json
        with open('countries.json', 'w', encoding='utf-8') as outfile:
            json.dump(allCities, outfile, ensure_ascii=False, indent=4)

def launchDailyRanking():
    countries = json.load(open('countries.json', 'r', encoding='utf-8'))
    yesterday = datetime.today() - timedelta(days=1)
    # Bouclez pour obtenir les 30 jours précédents
    for i in range(30):
        # on prend 3 pays au hasard dans la liste des pays car si on prend tout les pays on fini rapidement rate limit
        randomCountries = random.sample(list(countries.keys()), 3)
        date = yesterday - timedelta(days=i)
        for country in randomCountries :
            getSpotifyDailyRanking(date.strftime("%Y-%m-%d"),country.lower())

def launchWeeklyRanking():
    countries = json.load(open('countries.json', 'r', encoding='utf-8'))
    yesterday = datetime.today() - timedelta(days=1)
    # Bouclez pour obtenir les 4 semaines précédentes
    for i in range(4):
        # on prend 3 pays au hasard dans la liste des pays car si on prend tout les pays on fini rapidement rate limit
        randomCountries = random.sample(list(countries.keys()), 3)
        date = yesterday - timedelta(days=i*7)
        for country in randomCountries :
            for city in countries[country]:
                print(f"Getting data for {date} in {country} in {city['name']}, {city['code']}")
                getSpotifyWeeklyRanking(date.strftime("%Y-%m-%d"),city['code'])


# A lancer une fois pour récupérer les pays et les villes
if os.path.exists('countries.json') == False:
    getAllCountriesAndCity()

# launchWeeklyRanking() # Pas utilisé mais à garder si on décide d'implémenter les classements hebdomadaires
launchDailyRanking()

conn.close()
