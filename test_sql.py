from __future__ import division, unicode_literals
import sqlite3_helper
import config

hdb = sqlite3_helper.SqliteDB(db_path=config.PATH_DATABASE,
		row_type="dict", placeholder="$", commit_every_query=True)

