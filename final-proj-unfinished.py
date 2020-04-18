from bs4 import BeautifulSoup
import requests
import json
import sqlite3


CACHE_FILE_NAME = 'cache.json'
CACHE_DICT = {}

def load_cache():
    ''' Opens the cache file if it exists and loads the JSON into
    the CACHE_DICT dictionary.
    if the cache file doesn't exist, creates a new cache dictionary
    
    Parameters
    ----------
    None
    
    Returns
    -------
    The opened cache: dict
    '''
    try:
        cache_file = open(CACHE_FILE_NAME, 'r')
        cache_file_contents = cache_file.read()
        cache = json.loads(cache_file_contents)
        cache_file.close()
    except:
        cache = {}
    return cache


def save_cache(cache):
    ''' Saves the current state of the cache to disk
    
    Parameters
    ----------
    cache_dict: dict
        The dictionary to save
    
    Returns
    -------
    None
    '''
    cache_file = open(CACHE_FILE_NAME, 'w')
    contents_to_write = json.dumps(cache)
    cache_file.write(contents_to_write)
    cache_file.close()


def make_url_request_using_cache(url, cache):
    '''Check the cache for a saved result for the url. If the result is 
    found, print 'Using cache' and return it. Otherwise print 'Fetching', 
    send a new request, save it, then return it.
    
    Parameters
    ----------
    url: string
        A url of a webpage that is to be crawled and scraped
    cache: dict
        A dictionary to save the requests and results that have been made
        
    Returns
    -------
    dict
        the results of the query as a dictionary loaded from cache
        JSON
    '''
    if (url in cache.keys()): 
        print("Using cache")
        return cache[url]
    else:
        print("Fetching")
        response = requests.get(url)
        cache[url] = response.text
        save_cache(cache)
        return cache[url]


def build_movie_url_dict():
    movie_url_dict = {}

    baseurl = 'https://www.imdb.com/chart/top/?ref_=nv_mv_250'
    #response = requests.get(baseurl)
    url_text = make_url_request_using_cache(baseurl, CACHE_DICT)

    #soup = BeautifulSoup(response.text, 'html.parser')
    soup = BeautifulSoup(url_text, 'html.parser')

    all_movies_td = soup.find_all('td', class_='titleColumn')
    movie_url_list = []
    for movie_td in all_movies_td:
        movie = movie_td.find('a').string
        movie_url = movie_td.find('a')['href']
        movie_url_complete = 'https://www.imdb.com' + movie_url
        movie_url_list.append(movie_url_complete)
        movie_url_dict[movie] = movie_url_complete
    return movie_url_dict


def get_movie_rank():
    movie_rank = {}

    baseurl = 'https://www.imdb.com/chart/top/?ref_=nv_mv_250'
    #response = requests.get(baseurl)
    #soup = BeautifulSoup(response.text, 'html.parser')
    url_text = make_url_request_using_cache(baseurl, CACHE_DICT)
    soup = BeautifulSoup(url_text, 'html.parser')

    all_movies_td = soup.find_all('td', class_='titleColumn')
    for movie_td in all_movies_td:
        movie = movie_td.find('a').string
        rank_raw = movie_td.text.strip()[:3]
        rank= rank_raw.rstrip().replace('.', '')
        movie_rank[movie] = rank

    return movie_rank
    

def get_movie_dict(movie_url):

    movie_dict = {}

    url_text = make_url_request_using_cache(movie_url, CACHE_DICT)

    #response2 = requests.get(movie_url)
    #soup = BeautifulSoup(response2.text, 'html.parser')
    soup = BeautifulSoup(url_text, 'html.parser')

    name_parent = soup.find('div', class_='title_wrapper')
    name_year = name_parent.find('h1').text
    name = name_year.split('(')[0].strip()
    year = name_year.split('(')[1][:4]
    rating = soup.find('span', itemprop='ratingValue').text.strip()

    genre_tag = name_parent.find('div', class_='subtext')
    genres = genre_tag.find_all('a')
    genre_list = []
    for g in genres[:-1]:
        genre= g.text.strip()
        genre_list.append(genre)
    country = genres[-1].text.strip().split('(')[1][:-1]
    url = movie_url

    movie_dict['name'] = name
    movie_dict['year'] = year
    movie_dict['rating'] = rating
    movie_dict['country'] = country
    movie_dict['url'] = url
    movie_dict['genre'] = genre_list

    return movie_dict


def get_genre():
    genre_url = 'https://help.imdb.com/article/contribution/titles/genres/GZDRMS6R742JRGAG#'
    #response3 = requests.get(genre_url)
    #soup = BeautifulSoup(response3.text, 'html.parser')
    url_text = make_url_request_using_cache(genre_url, CACHE_DICT)
    soup = BeautifulSoup(url_text, 'html.parser')

    genre_parent = soup.find('div', id='article_content')
    genre_subparent = genre_parent.find_all('p')[1]
    genres = genre_subparent.find_all('a')
    genre_list = []
    for genre in genres:
        genre = genre.string.strip()
        genre_list.append(genre)

    return genre_list


DB_NAME = 'imdb-movies.sqlite'
def create_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    drop_movies_sql = 'DROP TABLE IF EXISTS "Movies"'
    drop_genres_sql = 'DROP TABLE IF EXISTS "Genres"'
    drop_movie_rank_sql = 'DROP TABLE IF EXISTS "MovieRank"'
    drop_countries_sql = 'DROP TABLE IF EXISTS "Countries"'
    
    create_movies_sql = '''
        CREATE TABLE IF NOT EXISTS "Movies" (
            "Id" INTEGER PRIMARY KEY AUTOINCREMENT, 
            "MovieId" INTEGER NOT NULL,
            "Year" INTEGER NOT NULL, 
            "Rating" REAL NOT NULL,
            "CountryId" INTEGER NOT NULL,
            "Genre" TEXT NOT NULL,
            "URL" TEXT NOT NULL
        )
    '''
    create_movie_rank_sql = '''
        CREATE TABLE IF NOT EXISTS "MovieRank" (
            "Id" INTEGER PRIMARY KEY AUTOINCREMENT, 
            "MovieName" TEXT NOT NULL,
            "Rank" INTEGER NOT NULL
        )
    '''
    create_countries_sql = '''
        CREATE TABLE IF NOT EXISTS 'Countries'(
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'EnglishName' TEXT NOT NULL,
            'Region' TEXT NOT NULL,
            'Subregion' TEXT NOT NULL,
            'Population' INTEGER NOT NULL
        )
    '''
    create_genres_sql = '''
        CREATE TABLE IF NOT EXISTS "Genres" (
            "Id" INTEGER PRIMARY KEY AUTOINCREMENT, 
            "Genre" TEXT NOT NULL
        )
    '''

    cur.execute(drop_genres_sql)
    cur.execute(drop_movie_rank_sql)
    cur.execute(drop_countries_sql)
    cur.execute(drop_movies_sql)

    cur.execute(create_genres_sql)
    cur.execute(create_movie_rank_sql)
    cur.execute(create_countries_sql)
    cur.execute(create_movies_sql)
    conn.commit()
    conn.close()


def load_movies():

    select_country_id_sql = '''
        SELECT Id FROM Countries
        WHERE EnglishName = ?
    '''

    select_movie_id_sql = '''
        SELECT Id FROM MovieRank
        WHERE MovieName = ?
    '''

    insert_movie_sql = '''
        INSERT INTO Movies
        VALUES (NULL, ?, ?, ?, ?, ?, ?)
    '''

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    movie_url_dict = build_movie_url_dict()
    for url in movie_url_dict.values():
        movie_dict = get_movie_dict(url)

        cur.execute(select_country_id_sql, [movie_dict['country']])
        res = cur.fetchone()
        country_id = None
        if res is not None:
            country_id = res[0]
        else:
            if movie_dict['country'] == 'USA':
                country_id = 240
            if movie_dict['country'] == 'South Korea':
                country_id = 211
            if movie_dict['country'] == 'UK':
                country_id = 239
            if movie_dict['country'] == 'Iran':
                country_id = 108
            
        cur.execute(select_movie_id_sql, [movie_dict['name']])
        res = cur.fetchone()
        movie_id = None
        if res is not None:
            movie_id = res[0]

        for i in range(len(movie_dict['genre'])):
            cur.execute(insert_movie_sql, [
                movie_id, 
                movie_dict['year'],
                movie_dict['rating'],
                country_id,
                movie_dict['genre'][i],
                movie_dict['url']
            ])
    conn.commit()
    conn.close()


def load_genres():

    insert_genre_sql = '''
        INSERT INTO Genres
        VALUES (NULL, ?)
    '''

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    genre_list = get_genre()
    for genre in genre_list:
        cur.execute(insert_genre_sql, [genre])
    conn.commit()
    conn.close()


def load_movie_rank():

    insert_movie_rank_sql = '''
        INSERT INTO MovieRank
        VALUES (NULL, ?, ?)
    '''

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    movie_rank_dict = get_movie_rank()
    for key in movie_rank_dict.keys():
        cur.execute(insert_movie_rank_sql, [key, movie_rank_dict[key]])
    conn.commit()
    conn.close()


def load_countries(): 
    base_url = 'https://restcountries.eu/rest/v2/all'
    #countries = requests.get(base_url).json()
    countries_str = make_url_request_using_cache(base_url, CACHE_DICT)
    countries = json.loads(countries_str)

    insert_country_sql = '''
        INSERT INTO Countries
        VALUES (NULL, ?, ?, ?, ?)
    '''

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for c in countries:
        cur.execute(insert_country_sql,
            [
                c['name'], 
                c['region'],
                c['subregion'],
                c['population']
            ]
        )
    conn.commit()
    conn.close()


if __name__ == "__main__":

    CACHE_DICT = load_cache()

    create_db()
    #load_genres() # I doubt if there's any need to creat a Genre table separaely.  
    load_movie_rank()
    load_countries()
    load_movies()
   
    