# http://flask.pocoo.org/docs/0.12/patterns/sqlite3/
import sqlite3
from flask import g, Flask, jsonify, request
from datetime import datetime

DATABASE = 'database.db'
app = Flask(__name__)


# get db from the current context
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db


@app.route('/')
def index():
    return 'Hello'


# POST view function of /cities
def cities_post():
    db = get_db()
    data = dict(request.get_json())

    # validate incoming data
    if 'country_id' not in data:
        return jsonify({"error": "Invalid JSON structure, field 'country_id' needed"}), 400
    if 'city_name' not in data:
        return jsonify({"error": "Invalid JSON structure, field 'city_name' needed"}), 400
    country_id, city_name = data['country_id'], data['city_name']

    # check if such country_id exists
    country = db.execute('''
    SELECT *
    FROM country
    WHERE country_id = ?
    ''', (country_id,)).fetchall()
    # if not
    if not country:
        return jsonify({"error": "Invalid country_id"}), 400

    # insert a new row
    count = get_next_city_id()
    db.execute('INSERT INTO city (city_id, city, country_id, last_update) VALUES (?, ?, ?, ?)',
               (count, city_name, country_id, str(datetime.now),)).fetchall()
    db.commit()

    # return the inserted row as a JSON
    data['city_id'] = count
    return jsonify(data), 200


# GET view function of /cities
def cities_get():
    db = get_db()
    query = request.args

    # prepare a suffix query
    # when page-like displaying on
    limit_offset_suffix = ''
    if 'per_page' in query:
        per_page = int(query['per_page'])
        page = int(query['page'])
        offset = per_page*(page-1)
        limit_offset_suffix = f'''
        LIMIT {per_page}
        OFFSET {offset}
        '''

    # if the query requests only data
    # with specific country_name field
    if 'country_name' in query:
        cities_list = db.execute('''
        SELECT city FROM city 
        JOIN country ON city.country_id = country.country_id
        WHERE country.country = ? 
        ORDER BY city ASC
        ''' + limit_offset_suffix, (query['country_name'],)).fetchall()
    else:
        cities_list = db.execute('SELECT city FROM city ORDER BY city ASC' + limit_offset_suffix)

    return jsonify([city[0] for city in cities_list])


@app.route('/cities', methods=['GET', 'POST'])
def cities():
    if request.method == 'POST':
        return cities_post()
    return cities_get()


# number of roles in all films
# grouped by language
@app.route('/lang_roles')
def lang_roles():
    db = get_db()
    lang_roles_list = db.execute('''
    SELECT film_language.name AS language, COUNT(actor_id) AS count
    FROM 
    (
        SELECT film_id, name
        FROM language
        LEFT JOIN film
        ON film.language_id = language.language_id
    ) film_language
    LEFT JOIN film_actor
    ON film_language.film_id = film_actor.film_id
    GROUP BY film_language.name
    ''').fetchall()

    return jsonify(dict(lang_roles_list))


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


# auto iterate mechanism for city_id
def get_next_city_id():
    with app.app_context():
        db = get_db()
        return db.execute('SELECT MAX(city_id) FROM city').fetchone()[0] + 1


if __name__ == '__main__':
    app.run(debug=True)