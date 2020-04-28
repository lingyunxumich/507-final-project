from flask import Flask, render_template, request
import sqlite3
import plotly.graph_objects as go


app = Flask(__name__)


def get_results(sort_by, sort_order, region):
    conn = sqlite3.connect('imdb-movies.sqlite')
    cur = conn.cursor()

    if sort_by == 'Rank':
        sort_column = 'Rank'
    else:
        sort_column = 'Year'

    where_clause = ''
    if (region != 'All'):
        where_clause = f'WHERE Region = "{region}"'
    
    q = f'''
        SELECT DISTINCT MovieName, {sort_column}
        FROM Movies
        LEFT JOIN Countries ON Movies.CountryId = Countries.Id
        LEFT JOIN MovieRank ON Movies.MovieId = MovieRank.Id
        {where_clause}
        ORDER BY {sort_column} {sort_order}
        LIMIT 20
    '''

    results = cur.execute(q).fetchall()
    conn.close()
    return results


def get_movie_list():
    conn = sqlite3.connect('imdb-movies.sqlite')
    cur = conn.cursor()
    
    q = f'''
        SELECT DISTINCT MovieName, Rank, URL
        FROM Movies
        JOIN MovieRank ON Movies.MovieId = MovieRank.Id
        ORDER BY Rank
    '''

    results = cur.execute(q).fetchall()
    conn.close()
    return results


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/results', methods=['POST'])
def movies():
    sort_by = request.form['sort']
    sort_order = request.form['dir']
    region = request.form['region']
    results = get_results(sort_by, sort_order, region)
    
    plot_results = request.form.get('plot', False)
    if (plot_results):
        
        x_vals = [r[0] for r in results]
        y_vals = [r[1] for r in results]

        if sort_by == 'Rank':
            bars_data = go.Bar(x=x_vals, y=y_vals)
            fig = go.Figure(data=bars_data)

        else:
            scatter_data = go.Scatter(x=x_vals, y=y_vals)
            fig = go.Figure(data=scatter_data)

        div = fig.to_html(full_html=False)
        return render_template("plot.html", plot_div=div)
        
    else:
        return render_template('results.html', 
            sort=sort_by, results=results)


@app.route('/plot')
def plot():
    x_vals = ['lions', 'tigers', 'bears']
    y_vals = [6, 11, 3]

    bars_data = go.Bar(
        x=x_vals,
        y=y_vals
    )
    fig = go.Figure(data=bars_data)
    div = fig.to_html(full_html=False)
    return render_template("plot.html", plot_div=div)


@app.route('/250list', methods=['POST'])
def movie_list():
    results = get_movie_list()

    data=[]
    for r in results: 
        url = r[2]
        movie = r[0]
        data.append((url,movie))

    return render_template('250list.html', 
        movies=data) 


if __name__ == '__main__':
    app.run(debug=True)
