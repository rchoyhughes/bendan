import flask
from helper import *
import sqlite3
from sqlite3 import Error
from flask import request
from flask import flash
from flask import g
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.automap import automap_base

app = flask.Flask(__name__)
app.secret_key = 'some secret key'
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = -1
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data/database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
DATABASE = 'data/database.db'
db = SQLAlchemy(app)

Base = automap_base()
Base.prepare(db.engine, reflect=True)
Users = Base.classes.users
Posts = Base.classes.posts


def get_db():
    my_db = getattr(g, '_database', None)
    if my_db is None:
        my_db = g._database = sqlite3.connect(DATABASE)
    my_db.row_factory = sqlite3.Row
    return my_db


def init_db():
    with app.app_context():
        my_db = get_db()
        with app.open_resource('data/schema.sql', mode='r') as f:
            my_db.cursor().executescript(f.read())
        my_db.commit()
        my_db.close()


def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv


@app.teardown_appcontext
def close_connection(exception):
    db.session.close()


@app.errorhandler(405)
@app.errorhandler(404)
def e404(e):
    return flask.render_template('error.html')


@app.route('/<private_id>/create-submit', methods=['POST'])
def create_submit(private_id):
    timestamp = get_timestamp()
    data = request.form
    username = db.session.query(Users).filter_by(private_id=private_id).first().username
    post_id = hash_string(username + str(timestamp))
    db.session.add(Posts(post_id=post_id, title=data['title'], content=data['content'], username=username, timestamp=timestamp, upvotes=0))
    db.session.commit()
    return flask.redirect('/' + private_id + '/profile')


@app.route('/<private_id>/create', methods=['GET', 'POST'])
def create(private_id):
    return flask.render_template('create.html', private_id=private_id)


@app.route('/<private_id>/profile', methods=['GET', 'POST'])
def profile(private_id):
    q = db.session.query(Users).filter_by(private_id=private_id).first()
    if q is None:
        flask.abort(404)
    uname = q.username

    # get posts with username
    q = db.session.query(Posts).filter_by(username=uname)
    post_list = []
    for post in q:
        post_list.append([post.post_id, post.title, post.content, time_string(post.timestamp), post.upvotes])
    post_list.reverse()
    return flask.render_template('profile.html', username=uname, data=post_list)


@app.route('/profile', methods=['GET', 'POST'])
def default_create():
    return flask.render_template('profile.html')


@app.route('/post/<post_id>', methods=['GET', 'POST'])
def post_view(post_id):
    q = db.session.query(Posts).filter_by(post_id=post_id).first()
    if q is None:
        flask.abort(404)
    found = [q.post_id, q.title, q.content, time_string(q.timestamp), q.upvotes]
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
    q = db.session.query(Users).filter_by(username=username).first()
    private_id = hash_string(username + data['password'])
    if q is not None:
        q = db.session.query(Users).filter_by(private_id=private_id).first()
        if q is None:
            flash('That username is already taken, or the password was incorrect. Please try again.', 'danger')
            return flask.redirect('/register')
        # flash('You have been signed in as ' + data['username'] + '.', 'success')
        return flask.redirect('/' + private_id + '/profile')
    else:
        db.session.add(Users(username=username, private_id=private_id))
        db.session.commit()
        # flash('Thank you for registering, ' + data['username'] + '.', 'success')
        return flask.redirect('/' + private_id + '/profile')


@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
@app.route('/register', methods=['GET', 'POST'])
def login():
    return flask.render_template('login.html')


@app.route('/test-sqlalchemy', methods=['GET', 'POST'])
def test_alchemy():
    results = db.session.query(Posts).all()
    for r in results:
        print(r.content)
    return flask.redirect('/')


if __name__ == '__main__':
    init_db()
    app.run(port=8001, host='127.0.0.1', debug=True, use_evalex=False)
