#################################
##### Name: Kseniya Husak
##### Uniqname: khusak
##### MOVIES AND REVIEWS PROJECT#
#################################

import requests
from bs4 import BeautifulSoup
import secrets
import json
import sqlite3

CACHE_FILE_NAME = 'cache.json'
CACHE_DICT = {}
MOVIE_API = secrets.OMDB_KEY
NYT_API = secrets.NYTAPI_KEY 
NYT_BASE_URL = 'https://api.nytimes.com/svc/movies/v2/reviews/search.json?'
OMDB_BASE_URL = 'http://www.omdbapi.com/?'
DB_NAME = 'movie_reviews.sqlite'


def create_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    drop_reviews_sql = 'DROP TABLE IF EXISTS "Reviews" '
    drop_movies_sql = 'DROP TABLE IF EXISTS "Movies" '

    create_reviews_sql = '''
        CREATE TABLE IF NOT EXISTS "Reviews" (
            "Id" INTEGER PRIMARY KEY AUTOINCREMENT, 
            "Movie_Title" TEXT NOT NULL,
            "Opening_Date" TEXT NOT NULL,
            "Review_Title" TEXT NOT NULL,
            "Review_by" TEXT NOT NULL,
            "Summary" TEXT NOT NULL,
            "Critics_Pick" INTEGER NOT NULL,
            "Rating" TEXT NOT NULL


        )
        '''

    create_movies_sql = '''
        CREATE TABLE IF NOT EXISTS "Movies" (
            "Id" INTEGER PRIMARY KEY AUTOINCREMENT, 
            "Title" TEXT NOT NULL,
            "Year" INTEGER NOT NULL,
            "Genre" TEXT NOT NULL,
            "Director" TEXT NOT NULL,
            "Country" TEXT NOT NULL,
            "Plot" TEXT NOT NULL,
            "Runtime" INTEGER NOT NULL,
            "imdbRating" INTEGER

        )
    '''
    cur.execute(drop_reviews_sql)
    cur.execute(drop_movies_sql)
    cur.execute(create_reviews_sql)
    cur.execute(create_movies_sql)
    conn.commit()
    conn.close()

# getting 100 results 
def get_NYT_reviews(date):
    results = []
    offset = [0,20,40,60,80]
    for number in offset: 
        call = requests.get(NYT_BASE_URL, params = {'api-key': NYT_API, 'offset': number, 
        'opening-date': date}).json()['results']
        results += call
    return results 


# fetching all movie details from API
def get_movie_details(movie):
    params = {'apikey': MOVIE_API, 't': movie}
    results = requests.get(OMDB_BASE_URL, params=params).json()
    return results 

# getting Rotten T ratings un-nested from movie API results
'''
def movie_rating(movies):
    for movie in movies:
        ratings = movie['Raitings']
        for rating in ratings: 
            if rating['Source'] == 'Rotten Tomatoes':
                movies['RT_Rating'] = rating['Value']
    return movies
'''

# iterating through list of NYT reviews, getting movie details and  ratings
# for now I am not including RT - working on it 
def reviewed_movies_details(NYT_results):
    movies=[]
    for movie in NYT_results:
        movies.append(get_movie_details(movie['display_title']))
    return movies


def load_reviews(reviews):


    insert_review_sql = '''
        INSERT INTO Reviews
        VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)

    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for review in reviews:
        cur.execute(insert_review_sql,
        [
            review['display_title'],
            review['opening_date'],
            review['headline'],
            review['byline'],
            review['summary_short'],
            review['critics_pick'],
            review['mpaa_rating']
        ])

    conn.commit()
    conn.close()

def load_movies(details):

    insert_movies_sql = '''
        INSERT INTO Movies
        VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for movie in details:
        print(movie.keys())
        cur.execute(insert_movies_sql,
        [
            movie['Title'],
            movie['Year'],
            movie['Genre'],
            movie['Director'],
            movie['Country'],
            movie['Plot'],
            movie['Runtime'],
            movie['imdbRating']

        ])
    conn.commit()
    conn.close()


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
    new_content = json.dumps(cache)
    cache_file.write(new_content)
    cache_file.close()


def make_api_request_with_cache(time_frame, cache):
    '''Check the cache for a saved result for the time frame. 
    If the result is found, return it. Otherwise send a new 
    NYT Movie Reviews Web API request with the provided parameters, save it, then return it.
 
    
    Parameters
    ----------
    time_frame: string
        String 
    params: dictionary
        A dictionary of param:value pairs
    cache: dictionary
        A dictionary of of date_frame:api_data pairs
    
    Returns
    -------
    dict
        the data returned from making the request in the form of 
        a dictionary
    '''
    if (time_frame in cache.keys()):
        print("Using Cache")
        return cache[time_frame]
    else:
        print("Fetching")
        response = get_NYT_reviews(time_frame)
        cache[time_frame] = response
        save_cache(cache)
        return cache[time_frame]

def make_omdb_api_request_with_cache(details_frame, cache, nyt_results):
    '''Check the cache for a saved result for the time frame. 
    If the result is found, return it. Otherwise send a new 
    NYT Movie Reviews Web API request with the provided parameters, save it, then return it.
 
    
    Parameters
    ----------
    time_frame: string
        String 
    params: dictionary
        A dictionary of param:value pairs
    cache: dictionary
        A dictionary of of date_frame:api_data pairs
    
    Returns
    -------
    dict
        the data returned from making the request in the form of 
        a dictionary
    '''
    if (details_frame in cache.keys()):
        print("Using Cache")
        return cache[details_frame]
    else:
        print("Fetching")
        response = reviewed_movies_details(nyt_results)
        cache[details_frame] = response
        save_cache(cache)
        return cache[details_frame]



if __name__ == "__main__":

    create_db()
    #load_reviews(PLUG RESULTS)

    # loading cache
    CACHE_DICT = load_cache()

    # getting API from NYT Reviews
    time_frame ='1980-01-01;1982-01-01'
    details_frame = time_frame + 'd'

    # first attempt
    first = make_api_request_with_cache(time_frame, CACHE_DICT)

    # getting movies API from OMDB for the first batch of NYT reviews
    details = make_omdb_api_request_with_cache(details_frame, CACHE_DICT, first)

    #print(details[0]['Title'])

    # loading 
    load_reviews(first)
    load_movies(details)
    
    #print(first[0])


    # format dates like this 
    # date = ('1980-01-01;1990-01-01')