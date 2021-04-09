import flask
from flask import request
import sqlite3_helper

db = sqlite3_helper.SqliteDB(db_path='data/database.db', row_type='dict', placeholder='$',
                             schema_path='data/schema.sql')
app = flask.Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
@app.route('/register', methods=['GET', 'POST'])
def login():
    return flask.render_template('login.html')


if __name__ == '__main__':
    app.run(port=8001, host='127.0.0.1', debug=True, use_evalex=False)
