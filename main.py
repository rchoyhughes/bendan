import flask
from helper import *
import sqlite3
from sqlite3 import Error
from flask import request
from flask import flash

app = flask.Flask(__name__)
app.secret_key = 'some secret key'

c = None
try:
    c = sqlite3.connect('data/database.db').cursor()
except Error as e:
    print(e)

try:
    with open('data/schema.sql') as schema:
        c.executescript(schema.read())
except Error as e:
    print(e)


@app.route('/login-submit', methods=['POST'])
def login_submit():
    data = request.form
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    sql = 'SELECT * from users where username=?'
    cursor.execute(sql, (data['username'],))
    private_id = hash_string(data['username'] + data['password'])
    if len(cursor.fetchall()) != 0:
        sql = 'SELECT * from users where private_id=?'
        cursor.execute(sql, (private_id,))
        if len(cursor.fetchall()) == 0:
            flash('That username is already taken. Please choose another one.')
            return flask.redirect('/register')
        flash('You have been signed in as ' + data['username'] + '.')
        return flask.redirect('/register')
    else:
        sql = 'INSERT INTO users(username, private_id) VALUES (?,?)'
        cursor.execute(sql, (data['username'], private_id))
        conn.commit()
        flash('Thank you for registering, ' + data['username'] + '.')
        return flask.redirect('/register')


@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
@app.route('/register', methods=['GET', 'POST'])
def login():
    return flask.render_template('login.html')


if __name__ == '__main__':
    app.run(port=8001, host='127.0.0.1', debug=True, use_evalex=False)
