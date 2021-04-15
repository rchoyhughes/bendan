import flask
from helper import *
import sqlite3
from sqlite3 import Error
from flask import request
from flask import flash

app = flask.Flask(__name__)
app.secret_key = 'some secret key'
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = -1

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


@app.errorhandler(405)
@app.errorhandler(404)
def e404(e):
    return flask.render_template('error.html')


@app.route('/<private_id>/create-submit', methods=['POST'])
def create_submit(private_id):
    timestamp = get_timestamp()
    data = request.form
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    sql = 'SELECT username from users where private_id=?'
    cursor.execute(sql, (private_id,))
    username = cursor.fetchall()[0][0]
    post_id = hash_string(username + str(timestamp))
    sql = 'INSERT INTO posts(post_id, title, content, username, timestamp, upvotes) VALUES (?, ?, ?, ?, ?, ?)'
    cursor.execute(sql, (post_id, data['title'], data['content'], username, timestamp, 0))
    conn.commit()
    return flask.redirect('/' + private_id + '/profile')


@app.route('/<private_id>/create', methods=['GET', 'POST'])
def create(private_id):
    return flask.render_template('create.html', private_id=private_id)


@app.route('/<private_id>/profile', methods=['GET', 'POST'])
def profile(private_id):
    pid = private_id
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    # get username from pid
    uname = cursor.execute('SELECT * from users where private_id = ?', (pid,)).fetchone()
    if uname is None:
        flask.abort(404)
    uname = uname[0]

    # get posts with username
    sql = 'SELECT * from posts where username = ?'
    posts = cursor.execute(sql, (uname,)).fetchall()
    post_list = []
    for element in posts:
        post_list.append([element[0], element[1], element[2], time_string(element[4]), element[5]])
    post_list.reverse()
    return flask.render_template('profile.html', username=uname, data=post_list)


@app.route('/profile', methods=['GET', 'POST'])
def default_create():
    return flask.render_template('profile.html')


@app.route('/post/<post_id>', methods=['GET', 'POST'])
def post_view(post_id):
    pid = post_id
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    # get post from pid
    found = cursor.execute('SELECT * from posts where post_id = ?', (pid,)).fetchone()
    if found is None:
        flask.abort(404)
    found = [found[0], found[1], found[2], time_string(found[4]), found[5]]
    return flask.render_template('post.html', post=found)


@app.route('/post', methods=['GET', 'POST'])
def default_post():
    return flask.render_template('post.html')


@app.route('/create', methods=['GET', 'POST'])
def default_profile():
    return flask.render_template('create.html')


@app.route('/login-submit', methods=['POST'])
def login_submit():
    data = request.form
    username = data['username'].lower()
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    sql = 'SELECT * from users where username=?'
    cursor.execute(sql, (username,))
    private_id = hash_string(username + data['password'])
    if len(cursor.fetchall()) != 0:
        sql = 'SELECT * from users where private_id=?'
        cursor.execute(sql, (private_id,))
        if len(cursor.fetchall()) == 0:
            flash('That username is already taken, or the password was incorrect. Please try again.', 'danger')
            return flask.redirect('/register')
        # flash('You have been signed in as ' + data['username'] + '.', 'success')
        return flask.redirect('/' + private_id + '/profile')
    else:
        sql = 'INSERT INTO users(username, private_id) VALUES (?,?)'
        cursor.execute(sql, (username, private_id))
        conn.commit()
        # flash('Thank you for registering, ' + data['username'] + '.', 'success')
        return flask.redirect('/' + private_id + '/profile')


@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
@app.route('/register', methods=['GET', 'POST'])
def login():
    return flask.render_template('login.html')


if __name__ == '__main__':
    app.run(port=8001, host='127.0.0.1', debug=True, use_evalex=False)
