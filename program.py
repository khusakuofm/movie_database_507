#################################
##### Name: Kseniya Husak
##### Uniqname: khusak
##### MOVIES AND REVIEWS PROJECT#
#################################

import requests
import secrets
import json
import sqlite3
from flask import Flask, render_template, request
import plotly.graph_objects as go

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
            "Movie_Title" TEXT NOT NULL PRIMARY KEY,
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
            "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
            "Title" TEXT NOT NULL,
            "Year" INTEGER NOT NULL,
            "Genre" TEXT NOT NULL,
            "Director" TEXT NOT NULL,
            "Country" TEXT NOT NULL,
            "Plot" TEXT NOT NULL,
            "Runtime_min" INTEGER NOT NULL,
            "imdbRating" INTEGER,
            FOREIGN KEY("Title") REFERENCES "Reviews"("Movie_Title")
        )
    '''
    cur.execute(drop_reviews_sql)
    cur.execute(drop_movies_sql)
    cur.execute(create_reviews_sql)
    cur.execute(create_movies_sql)
    conn.commit()
    conn.close()

# getting 160 results 
def get_NYT_reviews(date):
    results = []
    offset = [0,20,40,60,80,100,120,140,160,180]
    for number in offset: 
        call = requests.get(NYT_BASE_URL, params = {'api-key': NYT_API, 'offset': number, 
        'opening-date': date}).json()['results']
        results += call
    return results 


# fetching all movie details from OMDb API

def get_movie_details(movie,year):
    params = {'apikey': MOVIE_API, 't': movie, 'y': year}
    results = requests.get(OMDB_BASE_URL, params=params).json()
    return results 


# iterating through list of NYT reviews, getting movie details and  ratings
# for now I am not including RT - working on it 
# iterating through list of NYT reviews, getting movie details and  ratings
# this drops all titles not found 
def reviewed_movies_details(NYT_results,year):
    movies=[]
    not_found = []
    for movie in NYT_results:
        details = get_movie_details(movie['display_title'],year)
        if details['Response'] == 'True':
            movies.append(details) 
        else:
            not_found.append(movie['display_title'])
    return movies

# some movies have different titles so unfortunately
# we have to drop these as one would manually need to go through and edit them
def crossref_results(NYT_results, movie_results):
    review_titles = []
    correctly_fetched = []
    for review in NYT_results:
        review_titles.append(review['display_title'])
    for movie in movie_results:
        if movie['Title'] in review_titles:
            correctly_fetched.append(movie)
    return correctly_fetched


# aligning NYT review with successfully fetched results 
# as some were not fetched
def align_results(NYT_results, movie_results):
    movie_titles = []
    filtered_reviews = []
    for movie in movie_results:
        movie_titles.append(movie['Title'])
    for review in NYT_results:
        if review['display_title'] in movie_titles:
            filtered_reviews.append(review)
    return filtered_reviews    


# removing 'min' from movie details Runtime
def format_runtime(movie_results):
    for movie in movie_results:
        if movie['Runtime'] != "N/A":
            value = movie['Runtime']
            runtime_list = value.split(' ')
            time = int(runtime_list[0])
            movie['Runtime'] = time
    return movie_results

# replacing missing rating with notRated
def not_rated(review_results):
    for movie in review_results:
        if movie['mpaa_rating'] == '':
            movie['mpaa_rating'] = 'Not Rated'
    return review_results


def format_reviewers (review_results):
    for movie in review_results:
        movie['byline'] = movie['byline'].lower().strip().title()
    return review_results


def format_summary (review_results):
    for movie in review_results:
        if movie['summary_short'] == '':
            movie['summary_short'] = 'Not Available'
    return review_results

def format_nyt_results(review_results):
    results = not_rated(review_results)
    results = format_reviewers(results)
    results = format_summary(results)
    return results

def load_reviews(reviews):
    
    insert_review_sql = '''
        INSERT INTO Reviews
        VALUES ( ?, ?, ?, ?, ?, ?, ?)

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

def make_omdb_api_request_with_cache(details_frame, cache, nyt_results, year):
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
        response = reviewed_movies_details(nyt_results, year)
        cache[details_frame] = response
        save_cache(cache)
        return cache[details_frame]

# formatting user input for year of movies
def format_date(user_input):
    end_date = int(user_input) + 1
    date = f'{user_input}'+'-'+'01'+'-'+'01'+';'+f'{end_date}'+'-'+'01'+'-'+'01'
    return date

###########################
########FLASK APP##########
###########################
app = Flask(__name__)

##SQL COMMANDS

# see all NYT critic picks review
def get_critics_pick(choice):
    conn = sqlite3.connect('movie_reviews.sqlite')
    cur = conn.cursor()
    q = f'''
        SELECT Movie_Title, Review_by, Summary, Rating
        FROM Reviews
        WHERE Critics_Pick = {choice}
      
    '''
    results = cur.execute(q).fetchall()
    conn.close()
    return results


# summarize by reviewer
def count_by_reviewer():
    conn = sqlite3.connect('movie_reviews.sqlite')
    cur = conn.cursor()
    q = f'''
        
        SELECT Review_by, count(Review_by) AS CountOf
        FROM Reviews
        GROUP BY Review_by
    '''
    results = cur.execute(q).fetchall()
    conn.close()
    return results


# comedy, drama, romance, western, horror, crime, mystery 

def join_genre(genre):
    search = f'{genre}' + '%'
    conn = sqlite3.connect('movie_reviews.sqlite')
    cur = conn.cursor()
    q = f'''
        
        SELECT Review_by, ROUND(AVG(imdbRating),2)
        FROM Reviews
        JOIN Movies
        ON Reviews.Movie_Title=Movies.Title
        WHERE Genre LIKE '{search}'
        GROUP BY Review_by
    '''
    results = cur.execute(q).fetchall()
    conn.close()
    return results

def reviewer_runtime():
    conn = sqlite3.connect('movie_reviews.sqlite')
    cur = conn.cursor()
    q = f'''
        
        SELECT Review_by, AVG(Runtime_min) 
        FROM Reviews
        JOIN Movies
        ON Reviews.Movie_Title=Movies.Title
        GROUP BY Review_by

    '''
    results = cur.execute(q).fetchall()
    conn.close()
    return results

@app.route('/')
def solicit_year():
    return render_template('year.html')


@app.route('/handle_form', methods=['POST'])
def handle_the_form():
    year = request.form['year']
    year = str(year)
    create_db()
    # loading cache
    CACHE_DICT = load_cache()
    time_frame = format_date(year)
    details_frame = time_frame + 'd'
    reviews = make_api_request_with_cache(time_frame, CACHE_DICT)
    # formating NYT API results 
    reviews_formatted = format_nyt_results(reviews)
    # getting movies API from OMDB for the first batch of NYT reviews
    details = make_omdb_api_request_with_cache(details_frame, CACHE_DICT, reviews_formatted, year)
    # filtering out movies fetched with slight title variation 
    correctly_fetched = crossref_results(reviews_formatted, details)
    #filtering reviews based on successful fetching from IMDB APO
    filtered_reviews =  align_results(reviews_formatted, correctly_fetched)
    # formatting Runtime 
    formatted_movies = format_runtime(correctly_fetched)
    # loading results into DB
    load_reviews(filtered_reviews)
    load_movies(formatted_movies)
    return render_template('index.html')
    
@app.route('/Runtime')
def review_runtime():
    results = reviewer_runtime()
    x_values = []
    y_values = []
    for pair in results:
        x_values.append(pair[0])
        y_values.append(pair[1]) 
    bars_data = go.Bar(
        x=x_values,
        y=y_values
    )
    fig = go.Figure(data=bars_data)
    div = fig.to_html(full_html=False)
    return render_template('runtime.html', plot_div=div)
 

@app.route('/Movies/CriticsPick')
def nyt_reviews_movies():
    results = get_critics_pick(choice=1)
    return render_template('results.html', results=results)    

@app.route('/Movies/NonCriticsPick')
def nyt_reviews_movies_non_critic():
    results = get_critics_pick(choice=0)
    return render_template('results.html', results=results)  

# this breaks down total reviews by reviewer 
@app.route('/reviews_by_reviewer')
def plot_by_reviewer():
    results = count_by_reviewer()
    x_values = []
    y_values = []
    for pair in results:
        x_values.append(pair[0])
        y_values.append(pair[1])

    bars_data = go.Bar(
        x=x_values,
        y=y_values
    )
    fig = go.Figure(data=bars_data)
    div = fig.to_html(full_html=False)
    return render_template('plot.html', plot_div=div)


@app.route('/handle_genres', methods=['POST'])
def ratings_per_reviewer():
    genre = request.form['genre']
    print(genre)
    results = join_genre(genre)
    x_values = []
    y_values = []
    for pair in results:
        x_values.append(pair[0])
        y_values.append(pair[1])

    bars_data = go.Bar(
        x=x_values,
        y=y_values
    )
    fig = go.Figure(data=bars_data)
    div = fig.to_html(full_html=False)
    return render_template('genre.html', plot_div=div, genre=genre)
    
if __name__ == "__main__":

    app.run(debug=True)
    
    


 