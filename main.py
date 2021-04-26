import flask
import io
import sys
from helper import *
import sqlite3
from flask import request, current_app, json, url_for, send_file
from flask import flash
from flask import g
import flask_login
from flask_login import LoginManager, login_user, logout_user, login_required, current_user, UserMixin
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


class User(UserMixin, Base):
    __tablename__ = 'users'

    def get_id(self):
        return self.private_id

    def __repr__(self):
        return '<User %r>' % self.username


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


@login_manager.unauthorized_handler
def unauthorized():
    return flask.render_template('unauthorized.html')


@app.errorhandler(405)
@app.errorhandler(404)
def e404(e):
    return flask.render_template('error.html')


@app.route('/getTSVdump', methods=['POST', 'GET'])
def get_tsv():
    return flask.render_template('downtsv.html')


@app.route('/getTSVfile/<tbtype>', methods=['GET'])
def get_branch_data_file(tbtype):
    # connection = engine.connect()
    data_for_csv = []
    # data = db.session.query(str(tbtype)).all()
    if tbtype == 'posts':
        data = db.session.query(Posts).all()
        file_basename = 'posts.tsv'
        server_path = ''
        w_file = open(server_path + file_basename, 'w')
        w_file.write('post_id\ttitle\tcontent\tusername\ttimestamp\tupvotes\tupvoters\tdownvoters\n')
        for row in data:
            row_as_string = row.post_id + '\t' + row.title + '\t' + row.content + '\t' + row.username + '\t' + str(
                row.timestamp) + '\t'
            row_as_string += str(row.upvotes) + '\t' + row.upvoters + '\t' + row.downvoters + '\n'
            print(row_as_string)
            w_file.write(row_as_string)
    elif tbtype == 'users':
        data = db.session.query(User).all()
        file_basename = 'users.tsv'
        server_path = ''
        w_file = open(server_path + file_basename, 'w')
        w_file.write('username\tprivate_id\tauthenticated\n')
        for row in data:
            row_as_string = row.username + '\t' + row.private_id + '\t' + str(row.authenticated) + '\n'
            print(row_as_string)
            w_file.write(row_as_string)

    w_file.close()
    w_file = open(server_path + file_basename, 'r')
    file_size = len(w_file.read())
    return send_file(server_path + file_basename, as_attachment=True)


#     response = make_response(w_file,200)
#     response.headers['Content-Description'] = 'File Transfer'
#     response.headers['Cache-Control'] = 'no-cache'
#     response.headers['Content-Type'] = 'text/tsv'
#     response.headers['Content-Disposition'] = 'attachment; filename=%s' % file_basename
#     response.headers['Content-Length'] = file_size
#     return response


@login_required
@app.route('/upvote', methods=['POST'])
def upvote_post():
    if request.method == "POST":
        dataGet = json.loads(request.data)
        print('Upvote invoked')
        print(dataGet['postid'])
        post = db.session.query(Posts).filter_by(post_id=dataGet['postid']).first()
        print("Upvoters")
        print(post.upvoters)
        print(current_user.username)
        # current_user.username
        if post:
            allVoters = str(post.upvoters).split(',')
            print(allVoters)
            downvoters = str(post.downvoters).split(',')
            if current_user.username in downvoters:
                print('Change downvote to upvote')
                allVoters.append(current_user.username)
                downvoters.remove(current_user.username)
                allVoters = ','.join(allVoters)
                downvoters = ','.join(downvoters)
                setattr(post, "downvoters", downvoters)
                setattr(post, "upvoters", allVoters)
                setattr(post, "upvotes", post.upvotes + 2)
                db.session.commit()
                return json.dumps({'status': 'success', 'upvotes': post.upvotes})
            elif current_user.username in allVoters:
                print('Remove upvote')
                allVoters.remove(current_user.username)
                allVoters = ','.join(allVoters)
                setattr(post, "upvoters", allVoters)
                setattr(post, "upvotes", post.upvotes - 1)
                db.session.commit()
                return json.dumps({'status': 'success', 'upvotes': post.upvotes})
            else:
                if allVoters == ['']:
                    setattr(post, "upvoters", current_user.username)
                else:
                    allVoters.append(current_user.username)
                    allVoters = ','.join(allVoters)
                    print('updated voters')
                    print(allVoters)
                    setattr(post, "upvoters", allVoters)
                setattr(post, "upvotes", post.upvotes + 1)
                db.session.commit()
                print('Post upvote success')
                return json.dumps({'status': 'success', 'upvotes': post.upvotes})
        return json.dumps({'status': 'no post found'})
    return 'Not POST'


@login_required
@app.route('/downvote', methods=['POST'])
def downvote_post():
    if request.method == "POST":
        dataGet = json.loads(request.data)
        print('Downvote invoked')
        print(dataGet['postid'])
        post = db.session.query(Posts).filter_by(post_id=dataGet['postid']).first()
        print("Downvoters")
        print(post.downvoters)
        print(current_user.username)
        # current_user.username
        if post:
            allVoters = str(post.downvoters).split(',')
            print(allVoters)
            upvoters = str(post.upvoters).split(',')
            if current_user.username in upvoters:
                print('Change upvote to downvote')
                allVoters.append(current_user.username)
                upvoters.remove(current_user.username)
                allVoters = ','.join(allVoters)
                upvoters = ','.join(upvoters)
                setattr(post, "upvoters", upvoters)
                setattr(post, "downvoters", allVoters)
                setattr(post, "upvotes", post.upvotes - 2)
                db.session.commit()
                return json.dumps({'status': 'success', 'upvotes': post.upvotes})
            elif current_user.username in allVoters:
                print('Remove downvote')
                allVoters.remove(current_user.username)
                allVoters = ','.join(allVoters)
                setattr(post, "downvoters", allVoters)
                setattr(post, "upvotes", post.upvotes + 1)
                db.session.commit()
                return json.dumps({'status': 'success', 'upvotes': post.upvotes})
            else:
                if allVoters == ['']:
                    setattr(post, "downvoters", current_user.username)
                else:
                    allVoters.append(current_user.username)
                    allVoters = ','.join(allVoters)
                    print('updated voters')
                    print(allVoters)
                    setattr(post, "downvoters", allVoters)
                setattr(post, "upvotes", post.upvotes - 1)
                db.session.commit()
                print('Post downvote success')
                return json.dumps({'status': 'success', 'upvotes': post.upvotes})
        return json.dumps({'status': 'no post found'})
    return 'Not POST'


@login_required
@app.route('/<private_id>/feed', methods=['GET', 'POST'])
def feed_redirect(private_id):
    if current_user.is_anonymous or private_id != current_user.private_id:
        return current_app.login_manager.unauthorized()
    return flask.redirect('/' + private_id + '/feed/1')


@login_required
@app.route('/<private_id>/feed/<int:page>', methods=['GET', 'POST'])
def feed(private_id, page):
    if current_user.is_anonymous or private_id != current_user.private_id:
        return current_app.login_manager.unauthorized()
    per_page = 5
    posts = db.session.query(Posts).filter(Posts.username != current_user.username).order_by(
        Posts.timestamp.desc()).paginate(page, per_page, error_out=False)
    return flask.render_template('feed.html', posts=posts, time_string=time_string)


@login_required
@app.route('/post/<post_id>/delete')
def delete_post(post_id):
    db.session.query(Posts).filter_by(post_id=post_id).delete()
    db.session.commit()
    return flask.redirect('/' + current_user.private_id + '/profile')


@app.route('/<private_id>/create-submit', methods=['POST'])
def create_submit(private_id):
    timestamp = get_timestamp()
    data = request.form
    username = db.session.query(User).filter_by(private_id=private_id).first().username
    post_id = hash_string(username + str(timestamp))
    db.session.add(
        Posts(post_id=post_id, title=data['title'], content=data['content'], username=username, timestamp=timestamp,
              upvotes=0, upvoters='', downvoters=''))
    db.session.commit()
    return flask.redirect('/' + private_id + '/profile')


@login_required
@app.route('/<private_id>/create', methods=['GET', 'POST'])
def create(private_id):
    if current_user.is_anonymous or private_id != current_user.private_id:
        return current_app.login_manager.unauthorized()
    return flask.render_template('create.html', private_id=private_id)


@login_required
@app.route('/<private_id>/profile', methods=['GET', 'POST'])
def profile(private_id):
    q = db.session.query(User).filter_by(private_id=private_id).first()
    if q is None:
        flask.abort(404)
    uname = q.username
    if current_user.is_anonymous or private_id != current_user.private_id:
        return current_app.login_manager.unauthorized()

    # get posts with username
    q = db.session.query(Posts).filter_by(username=uname)
    post_list = []
    for post in q:
        post_list.append([post.post_id, post.title, post.content, time_string(post.timestamp), post.upvotes])
    post_list.reverse()
    return flask.render_template('profile.html', username=uname, data=post_list)


@login_required
@app.route('/post/<post_id>', methods=['GET', 'POST'])
def post_view(post_id):
    q = db.session.query(Posts).filter_by(post_id=post_id).first()
    if q is None:
        flask.abort(404)
    found = [q.post_id, q.title, q.content, time_string(q.timestamp), q.upvotes, q.username]
    return flask.render_template('post.html', post=found)


@app.route('/login-submit', methods=['POST'])
def login_submit():
    data = request.form
    username = data['username'].lower()
    if ',' in username:
        flash('You cannot have the character \' , \' in your username. Please choose a different one.', 'danger')
        return flask.redirect('/register')
    elif ' ' in username:
        flash('You cannot have a space in your username. Please choose a different one.', 'danger')
        return flask.redirect('/register')
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
        return flask.redirect('/' + private_id + '/feed')
    else:
        user = User(username=username, private_id=private_id, authenticated=True)
        db.session.add(user)
        db.session.commit()
        # flash('Thank you for registering, ' + data['username'] + '.', 'success')
        login_user(user)
        return flask.redirect('/' + private_id + '/feed')


@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
@app.route('/register', methods=['GET', 'POST'])
def login():
    current_user.authenticated = False
    db.session.commit()
    logout_user()
    return flask.render_template('login.html')


if __name__ == '__main__':
    init_db()
    app.run(port=8001, host='127.0.0.1', debug=True, use_evalex=False)
