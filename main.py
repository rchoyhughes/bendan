import flask
from helper import *
import sqlite3
from flask import request
from flask import flash
from flask import g
import flask_login
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.automap import automap_base

app = flask.Flask(__name__)
app.secret_key = 'some secret key'
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = -1
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data/database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
DATABASE = 'data/database.db'
db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)

Base = automap_base()


class User(Base):
    __tablename__ = 'users'

    def is_authenticated(self):
        return self.authenticated

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.private_id

    def __repr__(self):
        return '<User %r>' % (self.username)


Base.prepare(db.engine, reflect=True)
Posts = Base.classes.posts


@login_manager.user_loader
def user_loader(user_id):
    return db.session.query(User).filter_by(private_id=user_id).first()


def init_db():
    conn = sqlite3.connect('data/database.db')
    c = conn.cursor()
    with open('data/schema.sql') as schema:
        c.executescript(schema.read())
    conn.close()


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
    username = db.session.query(User).filter_by(private_id=private_id).first().username
    post_id = hash_string(username + str(timestamp))
    db.session.add(
        Posts(post_id=post_id, title=data['title'], content=data['content'], username=username, timestamp=timestamp,
              upvotes=0))
    db.session.commit()
    return flask.redirect('/' + private_id + '/profile')


@login_required
@app.route('/<private_id>/create', methods=['GET', 'POST'])
def create(private_id):
    return flask.render_template('create.html', private_id=private_id)


@login_required
@app.route('/<private_id>/profile', methods=['GET', 'POST'])
def profile(private_id):
    q = db.session.query(User).filter_by(private_id=private_id).first()
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


@app.route('/test-profile', methods=['GET', 'POST'])
def default_create():
    return flask.render_template('profile.html')


@login_required
@app.route('/post/<post_id>', methods=['GET', 'POST'])
def post_view(post_id):
    q = db.session.query(Posts).filter_by(post_id=post_id).first()
    if q is None:
        flask.abort(404)
    found = [q.post_id, q.title, q.content, time_string(q.timestamp), q.upvotes]
    return flask.render_template('post.html', post=found)


@app.route('/test-post', methods=['GET', 'POST'])
def default_post():
    return flask.render_template('post.html')


@app.route('/test-create', methods=['GET', 'POST'])
def default_profile():
    return flask.render_template('create.html')


@app.route('/login-submit', methods=['POST'])
def login_submit():
    data = request.form
    username = data['username'].lower()
    user = db.session.query(User).filter_by(username=username).first()
    private_id = hash_string(username + data['password'])
    if user is not None:
        user = db.session.query(User).filter_by(private_id=private_id).first()
        if user is None:
            flash('That username is already taken, or the password was incorrect. Please try again.', 'danger')
            return flask.redirect('/register')
        # flash('You have been signed in as ' + data['username'] + '.', 'success')
        user.authenticated = True
        login_user(user)
        db.session.commit()
        return flask.redirect('/' + private_id + '/profile')
    else:
        user = User(username=username, private_id=private_id, authenticated=True)
        db.session.add(user)
        db.session.commit()
        # flash('Thank you for registering, ' + data['username'] + '.', 'success')
        login_user(user)
        return flask.redirect('/' + private_id + '/profile')


@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
@app.route('/register', methods=['GET', 'POST'])
def login():
    current_user.authenticated = False
    db.session.commit()
    logout_user()
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
