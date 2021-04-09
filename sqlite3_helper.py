# vim: set fileencoding=utf8 noexpandtab tabstop=4 shiftwidth=4:
"""
Author:    Alexander J. Quinn
Copyright: (c) 2012-2019 Alexander J. Quinn
License:   MIT License
Requires:  Python 2.7, safe_serialization, safe_eval
"""
#from __future__ import division as _, unicode_literals as _; del _

# Useful info about database locking, transactions, and concurrency:
#
# * Write-Ahead Logging (WAL) - mode for file locking and concurrency
#	https://www.sqlite.org/lockingv3.html
#
# * File locking and concurrency for non-WAL databases
#	https://www.sqlite.org/lockingv3.html

# FIXME: atexit.register(conn.commit) doesn't work with multiple threads as of 20190623

# TODO:
# * Figure out concurrency issues for Flask
# * Add: delete_one
# * Add: insert_batch(self, table, on_conflict=None, values, **kw_values)
# * Add: insert_or_select
# * Add: log_calls
# * Add: select_or_insert
# * Add: transaction(self)
# * Add: update_one
# * Add: upsert
# * Test or fix: _get_table_info_rows (for get_columns and get_column_names)
# * Test or fix: insert_with_key_fn

# pylint: disable=too-many-locals,too-many-lines, too-many-arguments, too-many-public-methods, too-many-branches, too-many-instance-attributes, too-many-statements
import sys, collections, re, os, threading, atexit, datetime
from safe_serialization import safe_repr, safe_eval

_DBG_PRINT_QUERIES_TO_STDERR = False  # Mainly for debugging this module

ALL_PLACEHOLDERS = ("?", ":", "$")
ALL_ON_CONFLICT = ("REPLACE", "ROLLBACK", "ABORT", "FAIL", "IGNORE")
# "ABORT" rolls back changes.  "FAIL" does not.

ALL_RESULT_TYPES = (
	"generator",  # generator_of_rows
	"tuple",	  # tuple_of_results
)
ALL_RESULT_SPEC_PARTS = ALL_RESULT_TYPES + (
	"row_id",	  # i.e., cursor.lastrowid
	"count",	  # i.e., cursor.rowcount
	"names",	  # column names
	"row",		  # one row
)
ALL_ROW_TYPES = (
	"tuple",	  # tuple of values
	"dict",		  # dict, of column_name ==> value
	"namedtuple", # collections.namedtuple object
)
_UNSPECIFIED = object()
DB_PATH_MEMORY		   = ":memory:"
DEFAULT_DB_MODULE_NAME = "sqlite3"
DEFAULT_RESULT_TYPE    = "tuple"
DEFAULT_PLACEHOLDER    = "$"
DEFAULT_ROW_TYPE	   = "dict"
assert DEFAULT_RESULT_TYPE in ALL_RESULT_TYPES
assert DEFAULT_ROW_TYPE in ALL_ROW_TYPES


class SqliteDB(object):
	def __init__(self, db_path=DB_PATH_MEMORY,
					schema_path=None,
					schema_sql=None,
					exclusive=False,
					result_type=DEFAULT_RESULT_TYPE,
					row_type=DEFAULT_ROW_TYPE,
#					log_path=None, # TODO, finish
#					log_all=None,  # TODO, finish
					placeholder = "$",
					commit_every_query = False,
					db_module_name = DEFAULT_DB_MODULE_NAME,
					):
		# Store some simple configuration attributes
		self.result_type = result_type
		self.row_type	 = row_type
		self._placeholder = placeholder
		self._schema_path = schema_path and os.path.abspath(schema_path) or None
		self._schema_sql  = schema_sql or None

		self._exclusive_mode = exclusive
		self.commit_every_query = commit_every_query

		# Find the DB PATH
		if db_path != ":memory:" and os.path.isdir(os.path.dirname(db_path)):
			self._db_path = os.path.abspath(db_path)
		else:
			self._db_path = db_path

		# Create empty container for result types (i.e., namedtuples)
		self._result_namedtuples = {}

		# Create thread local data store, to support per-thread DB connections.
		self._thread_local_data = threading.local()

		# This database is NEW if it is in-memory, the file doesn't exist, or the file is empty
		self.is_new = db_path==":memory:"  \
			or not os.path.isfile(db_path) \
			or os.path.getsize(db_path)==0

		# Find the sqlite3 (or equivalent) MODULE
		db_module_name = str(db_module_name)
		try:
			self._db_module = sys.modules[db_module_name]
		except KeyError:
			_db_module_name = "pysqlite2.dbapi2" if db_module_name == "pysqlite2" else db_module_name
			self._db_module = __import__(_db_module_name, None, None, [str('dummy')])
			# The ['dummy'] parameter is a workaround to make __import__ properly import
			# module names containing dots.  http://stackoverflow.com/a/2725668/500022
			# It is wrapped in str(…) because Python 2 requires bytes and Python 3 requires
			# unicode.  str(…) function/type is bytes in Python 2 and unicode in Python 3.

		# Register ADAPTERS / CONVERTERS
		self._init_adapters_converters(self._db_module)

		# Store SQLITE VERSION
		#self.sqlite_version = self._query("select sqlite_version() as v")[0].v
		self.sqlite_version = self._db_module.sqlite_version
	
	def to_blob(self, s):
		return self._db_module.Binary(s)

	def _init_adapters_converters(self, db_module):
		# helper for __init__(..)
		db_module.register_adapter(tuple, safe_repr)
		db_module.register_converter(str("tuple"), safe_eval)
		db_module.register_adapter(dict, safe_repr)
		db_module.register_converter(str("dict"), safe_eval)
		db_module.register_adapter(bool, lambda b:{True:1, False:0, None:None}[b])
		db_module.register_converter(str("bool"),
				lambda s:{"1":True, "True":True, "0":False, "False":False, None:None}[s])
		db_module.register_converter(str("boolean"),
				lambda s:{"1":True, "True":True, "0":False, "False":False, None:None}[s])
		#db_module.register_converter("boolean", convert_bool)

		# Fix broken timestamp converter
		db_module.register_converter(str("timestamp"), _parse_datetime_iso)
		db_module.register_adapter(datetime.datetime, _format_datetime_utc)
		# def convert_timestamp(val):
		#	datepart, timepart = val.split(" ")
		#	year, month, day = map(int, datepart.split("-"))
		#	timepart_full = timepart.split(".")
		#	hours, minutes, seconds = map(int, timepart_full[0].split(":"))
		#	if len(timepart_full) == 2:
		#		microseconds = int('{:0<6.6}'.format(timepart_full[1].decode()))
		#	else:
		#		microseconds = 0

		#	val = datetime.datetime(year, month, day, hours, minutes, seconds, microseconds)
		#	return val

	def _init_schema(self, conn):
		if self._schema_path is not None:
			with open(self._schema_path, "rb") as infile:
				sql = infile.read().decode("utf8")
			conn.executescript(sql)
		if self._schema_sql is not None:
			conn.executescript(self._schema_sql)
	
	def _init_exclusive_mode(self):
		# helper for __init__(..)
		self._execute_query("pragma journal_mode = off")
		self._execute_query("pragma synchronous = off")
		self._execute_query("pragma locking_mode = exclusive")
		# See http://sqlite.org/cvstrac/wiki?p=KeyValueDatabase

	def _select(self, tables, what, where, order, group, limit, offset, parameters, row_type, result_type, with_names, one_row, one_column, where_kw): 

		# Build query
		where_str,parameters = self._convert_where_and_vars(where, parameters, where_kw)
		parts = ["SELECT", _combine(what), "FROM", _combine(tables)]
		_add_clause(parts, "WHERE", where_str)
		_add_clause(parts, "ORDER BY", order)
		_add_clause(parts, "GROUP BY", group)
		_add_clause(parts, "LIMIT", limit, integer=True)
		_add_clause(parts, "OFFSET", offset, integer=True)

		if one_row:
			result_type = "row"
		elif not result_type:
			result_type = self.result_type

		return self.query(
			sql=" ".join(parts),
			parameters=parameters,
			result_spec=(("names", result_type) if with_names else result_type),
			row_type=row_type,
			one_column=one_column
		)

	def _adapt_rows(self, cursor, sql, parameters, row_type, one_column, col_names=None):
		'''
		Get rows from cursor, and ensure that (a) they adhere to the specified
		row_type, and (b) if one_column==True then there is only one column.
		
		If col_names is specified (and not None), then use them as keys for
		dict or namedtuple row (if applicable) instead of extracting the column
		names from attached to the query result (i.e., the cursor).
		'''
		# Adapt ROWS
		if row_type == "tuple":# or cursor.description is None:
			rows = cursor
		else:
			if col_names is None and row_type in ("dict", "namedtuple"):
				col_names = tuple(o[0] for o in cursor.description)

			if row_type == "dict":
				rows = (dict(zip(col_names, row)) for row in cursor)
			elif row_type == "namedtuple":
				result_namedtuple = self._get_result_namedtuple(col_names)
				rows = (result_namedtuple(*row) for row in cursor)
			elif row_type is None and one_column:
				if len(cursor.description) >= 2:
					raise DBTooManyColumnsError(sql, parameters)
				rows = (row[0] for row in cursor)
			else:
				assert False, (locals())
		return rows

	def query(self, sql, parameters=None, result_spec=_UNSPECIFIED, row_type=_UNSPECIFIED,
				one_column=False):

		result_spec = self.result_type if result_spec is _UNSPECIFIED else result_spec
		row_type	= self.row_type    if row_type	  is _UNSPECIFIED else row_type

		# Execute query
		cursor = self._execute_query(sql, parameters)
	
		if result_spec is None:
			return None ############  MIGHT RETURN FROM HERE  ################
		elif cursor.description is None:
			assert not sql.lower().startswith("select ")
			assert tuple(cursor) == ()
			might_have_rows = False
		else:
			assert len(cursor.description) >= 1, (sql, parameters)
			might_have_rows = True

		# Get result parts
		if _is_unicode_or_byte_string(result_spec):
			parts = (result_spec,)
		elif isinstance(result_spec, tuple) and all(s in ALL_RESULT_SPEC_PARTS for s in result_spec):
			parts = result_spec
		else:
			raise ValueError("result_spec must be a string in %r or tuple thereof"%ALL_RESULT_SPEC_PARTS) 
		# Get COLUMN NAMES
		if "names" in parts or row_type in ("dict","namedtuple"):
			if cursor.description is None:
				col_names = ()
			else:
				col_names = tuple(o[0] for o in cursor.description)
		else:
			col_names = None

		# ROWS
		need_row = "row" in parts
		need_tuple = "tuple" in parts
		need_generator = "generator" in parts
		if need_row or need_tuple or need_generator:
			if might_have_rows:
				rows_gen = self._adapt_rows(cursor, sql, parameters, row_type, one_column, col_names)
			else:
				rows_gen = cursor  # This is needed to prevent error in _adapt_rows(..) on
								   # query("pragma...", row_type="dict") due to trying to
								   # get column names even though there will be no rows.
			if need_tuple or need_generator and need_row:
				rows_tuple = tuple(rows_gen)
				if need_row:
					the_row = rows_tuple[0]
			elif need_row:
				the_row = self._get_the_one_and_only_row(rows_gen, sql, parameters)
			elif need_generator and rows_gen is cursor:
				rows_gen = (row for row in rows_gen)
			else:
				assert need_generator and not need_tuple and not need_row

		# RESULT
		result_parts = []
		for part in parts:
			if part == "tuple":
				result_parts.append(rows_tuple)
			elif part == "generator":
				result_parts.append(rows_gen)
			elif part == "row":
				result_parts.append(the_row)
			elif part == "count":
				result_parts.append(cursor.rowcount)
			elif part == "names":
				result_parts.append(col_names)
			elif part == "row_id":
				result_parts.append(cursor.lastrowid)
			else:
				assert False, part

		if _is_unicode_or_byte_string(result_spec):
			result = result_parts[0]
		elif isinstance(result_spec, tuple):
			result = tuple(result_parts)

		return result

	def _get_the_one_and_only_row(self, cursor, sql, parameters):
		try:
			row = next(cursor)
		except StopIteration:
			raise DBItemNotFoundError(sql, parameters)
		try:
			row = next(cursor)
		except StopIteration:
			return row
		else:
			raise DBTooManyItemsError(sql, parameters)

	def select(self, tables, what='*', where=None, order=None, group=None, limit=None, offset=None, parameters=None, with_names=False, row_type=None, result_type=None, **kw_where): 
		return self._select(
			tables		= tables,
			what		= what,
			where		= where,
			order		= order,
			group		= group,
			limit		= limit,
			offset		= offset,
			parameters	= parameters,
			result_type = result_type or self.result_type,
			row_type	= row_type or self.row_type,
			with_names	= with_names,
			one_row		= False,
			one_column	= False,
			where_kw	= kw_where,
		)

	def select_column(self, tables, what, where=None, order=None, group=None, limit=None, offset=None, parameters=None, result_type=None, **kw_where): 
		return self._select(
			tables		= tables,
			what		= what,
			where		= where,
			order		= order,
			group		= group,
			limit		= limit,
			offset		= offset,
			parameters	= parameters,
			result_type = result_type or self.result_type,
			row_type	= None,
			with_names	= False,
			one_row		= False,
			one_column	= True,
			where_kw	= kw_where,
		)

	def select_row(self, tables, what='*', where=None, order=None, group=None, limit=None, offset=None, parameters=None, with_names=False, row_type=None, **kw_where): 
		return self._select(
			tables		= tables,
			what		= what,
			where		= where,
			order		= order,
			group		= group,
			limit		= limit,
			offset		= offset,
			parameters	= parameters,
			result_type = None,
			row_type	= row_type or self.row_type,
			with_names	= with_names,
			one_row		= True,
			one_column	= False,
			where_kw	= kw_where,
		)
	
	def select_value(self, tables, what, where=None, order=None, group=None, limit=None, offset=None, parameters=None, **kw_where): 
		return self._select(
			tables		= tables,
			what		= what,
			where		= where,
			order		= order,
			group		= group,
			limit		= limit,
			offset		= offset,
			parameters	= parameters,
			result_type = None,
			row_type	= None,
			with_names	= False,
			one_row		= True,
			one_column	= True,
			where_kw	= kw_where,
		)

		
	def select_or_insert(self, table, *args, **kwargs):
		invalid_keys = set(("tables", "on_conflict", "return_row_id", "values")).intersection(kwargs)
		if invalid_keys:
			raise ValueError("You can't pass %s to select_or_insert(..)" % (" or ".join(repr(k) for k in kwargs)))

		try:
			row = self.select_row((table,), *args, **kwargs)
		except DBItemNotFoundError:
			row_id = self.insert(table, *args, return_row_id=True, **kwargs)
			#print(row_id)
			row = self.select_row((table,), *args, rowid=row_id, **kwargs)
		return row


	def _convert_values(self, values, kw_values):
		values = values or {}
		if kw_values:
			values.update(kw_values)
		return values

	def insert(self, table, on_conflict=None, return_row_id=False, values=None, **kw_values): 
		# We will build the query by adding parts to a list.
		parts = ["INSERT"]

		_add_clause_on_conflict(parts, on_conflict)

		parts.extend(("INTO", table))

		# Prepare PLACEHOLDERS and PARAMETERS
		insert_names = []
		placeholders = []
		parameters = self._make_mutable_parameters(None)
		for k,v in _iteritems(self._convert_values(values, kw_values)):
			assert _is_identifier(k), k # anti-injection paranoia
			insert_names.append(k)
			if self._placeholder == "?":
				placeholders.append("?")
				parameters.append(v)
			elif self._placeholder in ("$", ":"):
				placeholders.append(self._placeholder + k)
				parameters[k] = v
			else:
				assert False, "Unexpected placeholder:	%r"%self._placeholder

		# Add names and placeholders to query
		parts.append("(" + _combine(insert_names) + ")")
		parts.append("VALUES")
		parts.append("(" + _combine(placeholders) + ")")

		# Execute query
		return self.query(
			sql			 = " ".join(parts),
			parameters	 = parameters,
			result_spec  = ("row_id" if return_row_id else None),
		)

	def update(self, table, values, where=None, parameters=None, return_row_id=False, on_conflict=None, **kw_where):#, **kw_values): 
		where_str,parameters = self._convert_where_and_vars(where, parameters, kw_where)
		parts = ["UPDATE"]
		_add_clause_on_conflict(parts, on_conflict)
		parts.extend((table, "SET"))
		set_parts = []
		if self._placeholder == "?":
			set_parameters = self._make_mutable_parameters(None)
		#values = self._convert_values(values, kw_values)
		for k,v in _iteritems(values):
			if self._placeholder == "?":
				set_parts.append("%s=?"%k)
				set_parameters.append(v)
			elif self._placeholder in ("$", ":"):
				placeholder_name = k
				while placeholder_name in parameters:
					placeholder_name += "_"
				set_parts.append("%s=%s%s"%(k, self._placeholder, placeholder_name))
				parameters[placeholder_name] = v
		if self._placeholder == "?":
			parameters = set_parameters + parameters
		parts.append(", ".join(set_parts))
		_add_clause(parts, "WHERE", where_str)

		# Execute query
		result = self.query(
			sql			 = " ".join(parts),
			parameters	 = parameters,
			result_spec  = ("row_id" if return_row_id else None),  # FIXME: row_id seems to be broken; returns None 20180909-001200
		)
		return result

	def delete(self, table, where=None, order=None, limit=None, offset=None, parameters=None, **kw_where): 
		if where is None and len(kw_where)==0:
			raise ValueError("You must specify a condition to delete(..).  To delete all rows, pass where=True.")
		where_str,parameters = self._convert_where_and_vars(where, parameters, kw_where)
		parts = ["DELETE FROM", table]
		if where is not True:
			_add_clause(parts, "WHERE", where_str)
		_add_clause(parts, "ORDER", order)
		_add_clause(parts, "LIMIT", limit, integer=True)
		_add_clause(parts, "OFFSET", offset, integer=True)
		self._execute_query(
			sql			= " ".join(parts),
			parameters	= parameters,
		)

	def commit(self):
		self._get_db_conn().commit()

	def _check_parameters_type(self, parameters):
		if self._placeholder == "?":
			if parameters is None:
				parameters = ()
			elif isinstance(parameters, list):
				parameters = tuple(parameters)
			elif not isinstance(parameters, tuple):
				raise ValueError("parameters must be a tuple or list with placeholder style %r"%
						self._placeholder)
		elif self._placeholder in ("$", ":"):
			if parameters is None:
				parameters = {}
			elif not isinstance(parameters, dict):
				raise ValueError("parameters must be a dict with placeholder style %r"%
						self._placeholder)
		return parameters

	def _execute_query(self, sql, parameters=None):
		parameters = self._check_parameters_type(parameters)
		assert not isinstance(parameters, list), parameters
		cursor = self._get_db_conn().cursor()
		try:
			cursor.execute(sql, parameters)
		except self._db_module.IntegrityError as e:
			raise DBIntegrityError(inner=e)
		except self._db_module.OperationalError as e:
			raise
		if self.commit_every_query and sql.lower().lstrip().startswith("select"):
			self.commit()
		return cursor
	
#	def _dbg_print_query(sql, parameters):
#		sys.stderr.write("\n".join(("", sql, "--", parameters, "")))

	def _query_insert(self, sql, parameters, return_row_id):
		cursor = self._execute_query(sql, parameters)
		if return_row_id:
			return cursor.lastrowid
		else:
			return None

	def _get_result_type_row_type(self, result_type, row_type, one_row, one_column):
		# helper for query
		if not one_column:
			row_type = row_type or self.row_type
			if row_type not in ALL_ROW_TYPES:
				raise ValueError("row_type must be in %r"%ALL_ROW_TYPES)
		assert one_column == (row_type is None)

		if not one_row:
			result_type = result_type or self.result_type
			if result_type not in ALL_RESULT_TYPES:
				raise ValueError("result_type must be in %r"%ALL_RESULT_TYPES)
		assert one_row == (result_type is None)

		return (result_type, row_type)

	def _get_result_namedtuple(self, col_names):
		# helper for query
		try:
			result_namedtuple = self._result_namedtuples[col_names]
		except KeyError:
			result_namedtuple_name = "ResultType%d"%(len(self._result_namedtuples) + 1)
			result_namedtuple = collections.namedtuple(result_namedtuple_name, col_names)
			self._result_namedtuples[col_names] = result_namedtuple
		return result_namedtuple

	def register_function(self, function, name=None, num_params=None):
		if name is None:
			name = function.__name__
		if num_params is None:
			import inspect
			arg_info = inspect.getargspec(function) # (args, varargs, kwargs, defaults)
			# getargspec(..) returns (args, varargs, kwargs, defaults) or, as of Python 2.6,
			# an equivalent namedtuple.  varargs, kwargs, defaults will be None if absent.
			if tuple(arg_info[1:]) != (None, None, None):
				raise ValueError("The function may not take variable arguments, keyword "
						"arguments, or default values.	Got arg spec %r ."%arg_info)

			num_params = len(arg_info[0])

		self._get_db_conn().create_function(name, num_params, function)
		# def create_function(name, num_params, func)	# in sqlite3 standard module
	
	def register_aggregator(self, class_, name=None, num_params=None):
		if name is None:
			name = class_.__name__
		if num_params is None:
			import inspect
			arg_info = inspect.getargspec(class_.step) # (args, varargs, kwargs, defaults)

			# getargspec(..) returns (args, varargs, kwargs, defaults) or, as of Python 2.6,
			# an equivalent namedtuple.  varargs, kwargs, defaults will be None if absent.
			if tuple(arg_info[1:]) != (None, None, None) or len(arg_info[0]) < 2:
				raise ValueError("The step function must take >=1 plain argument (not"
						" counting 'self') and no variable arguments, keyword"
						" arguments, or default values.  Got arg spec %r ."%arg_info)

			num_params = len(arg_info[0])

		self._get_db_conn().create_aggregate(name, num_params, class_)
		# create_aggregate(name, num_params, aggregate_class) # in sqlite3 standard module

	def register_collation(self, function, name=None):
		if name is None:
			name = function.__name__
		return self._get_db_conn().create_collation(name, function)

	def register_authorizer(self, authorizer):
		self._get_db_conn().set_authorizer(authorizer)
	
	def register_progress_handler(self, handler, n):
		self._get_db_conn().set_progress_handler(handler, n)
	
	@property
	def total_changes(self):
		return self._get_db_conn().total_changes # it's a property or attribute, not method

	def iterdump(self):
		return self._get_db_conn().iterdump()

	def _get_db_conn(self):
		thread_local_data = self._thread_local_data
		try:
			conn = thread_local_data.connection
			#print("## [_ A] access connection from thread %r"%(threading.current_thread().ident))
		except AttributeError:
			# Create CONNECTION
			#conn = thread_local_data.connection = self._db_module.connect(self._db_path)
			conn = thread_local_data.connection = self._db_module.connect(self._db_path,
					detect_types=self._db_module.PARSE_DECLTYPES)
			#print("## [C _] create connection from thread %r"%(threading.current_thread().ident))

			atexit.register(conn.commit)  # FIXME:	This doesn't work with multiple threads as of 20190623

			# This database is also NEW if there are no tables.
			self.is_new = self.is_new or len(self.get_table_names())==0

			# Apply the SCHEMA if this database is new.
			if self.is_new:
				self._init_schema(conn)

			# If EXCLUSIVE, then run some queries to make this a little faster.
			if self._exclusive_mode:
				self._init_exclusive_mode()

		return conn

	def close(self): # added 6/23/2019, mainly as a workaround for the problem
					 # of atexit.register(conn.commit) not working with threads
		thread_local_data = self._thread_local_data
		try:
			conn = thread_local_data.connection
		except AttributeError:
			pass
		else:
			conn.commit()
			conn.close()
			del thread_local_data.connection

	def create_index(self, table_name, columns):
		assert isinstance(columns, (tuple, list)) and len(columns) >= 1, columns
		if not _is_identifier(table_name) or not all(_is_identifier(s) for s in columns):
			raise ValueError("Invalid table_name %s and/or columns %s"%(table_name, columns))
		index_name = "_".join(("i", table_name) + tuple(columns))
		columns_str = ", ".join(columns)
		sql = "create index %s on %s (%s)"%(index_name, table_name, columns_str)
		try:
			self.query(sql)
		except (SystemExit, KeyboardInterrupt, GeneratorExit, StopIteration) as e:
			raise
		except Exception as e:
			if type(e).__name__ == "OperationalError" and str(e)=="index %s already exists"%index_name:
				pass # TODO: Use sqlite_master to verify that the index is actually the same.
			else:
				raise

	@property
	def path(self):
		return self._db_path

	def upsert(self, table, key, return_row_id=False, values=None, **kw_values):
		keys = (key,) if _is_unicode_or_byte_string(key) else tuple(key)
		if not all(_is_unicode_or_byte_string(k) for k in keys):
			raise ValueError("key must be a string or sequence of strings")

		values_combined = self._convert_values(values, kw_values)
		assert isinstance(values_combined, dict) # FIXME: this won't work if
												 # values_combined is not a dict
												 # which may happen if placeholder
												 # is '?'
		if not all(key in values_combined for key in keys):
			raise ValueError("key(s) must be in the values or **kw_values.	Got %r which is not in %r."%(key, tuple(sorted(kw_values))))

		try:
			result = self.insert(table=table, return_row_id=return_row_id, values=values_combined) # True is for seqname, means to return row_id
		except DBIntegrityError as e:
			mo = self._integrity_error_is_due_to_column_nonuniqueness(e)
			if mo is None:
				raise
			where_parts = []
			parameters = self._make_mutable_parameters(None)
#			where_str,parameters = self._convert_where_and_vars(where, parameters)
			for k in keys:
				v = values_combined.pop(k)
				if self._placeholder == "?":
					where_parts.append("%s=?"%k)
					parameters.append(v)
				elif self._placeholder in ("$", ":"):
					where_parts.append("%s=%s%s"%(k,self._placeholder, k))
					parameters[k] = v
			if len(where_parts)==1:
				where, = where_parts # pylint:disable=unbalanced-tuple-unpacking
			else:
				assert len(where_parts) > 1
				where = " and ".join("(%s)"%where_part for where_part in where_parts)
			result = self.update(table, where=where, parameters=parameters, return_row_id=return_row_id, values=values_combined)
			if result is None and return_row_id:
				result = self.select_value(table, what="row_id", where=where, parameters=parameters)
		assert result is not None or not return_row_id
		return result

	def insert_with_key_fn(self, table, key, key_fn, num_retries=100, return_row_id=True, values=None, **kw_values): 
		# key need not be the actual primary key, but if not, it must have a unique constraint. 
		# That is not checked by this function.

		if "on_conflict" in kw_values:
			raise ValueError("You probably didn't mean to pass \"on_conflict\".  It"
					" has no special meaning with insert_with_key_fn(..) and would"
					" be treated as data to be inserted.  Add it to values if that"
					" is really what you wanted.")

		values_combined = self._convert_values(values, kw_values)
		for _ in range(num_retries): # absurd upper bound
			key_value_to_try = key_fn()  # ex:  key_fn = "".join(random.choice(string.letters) for _ in range(6))
			values_combined[key] = key_value_to_try
			try:
				row_id_or_none = self.insert(table, return_row_id=return_row_id, values=values_combined)
				if return_row_id:
					return (row_id_or_none, key_value_to_try)
				else:
					return key_value_to_try
			except DBIntegrityError as e:
				mo = self._integrity_error_is_due_to_column_nonuniqueness(e)
				if mo is None:
					raise # unexpected error from the database
				elif key not in (s.strip() for s in mo.group(1).split(",")):
					raise # key is not part of the constraint that failed
				elif self.select_value(table, "count(*)", **{key:key_value_to_try}) != 1:
					raise # key is not unique, as expected
		else:  # [pylint] allow try..else with return (not break) in body : pylint:disable=W0120
			raise CannotFindUniqueKeyForTableError(table, key, key_value_to_try)

	def get_column_names(self, table_name, except_primary_key=False):
		rows = self._get_table_info_rows(table_name)
		return tuple(r["name"] for r in rows if except_primary_key is False or r["pk"]==0)
	
	def _integrity_error_is_due_to_column_nonuniqueness(self, e):
		message = e.message
		mo = re.match("column (.+) is not unique", message)
		if mo is None:
			mo = re.match("columns (.+) are not unique", message)
		return mo

	def _get_table_info_rows(self, table_name):
		rows = self.query("pragma table_info(%s)"%table_name, row_type="dict", result_spec="tuple")
		if rows == ():	# TODO:  test the case of a table that does not exist
			raise DBTableNotFoundError(table_name)
		rows = sorted(rows, key=lambda row:row["cid"])
		return rows

	def get_columns(self, table_name):
		columns = []
		rows = self._get_table_info_rows(table_name)

		for row in rows:
			column = DBColumn(
				name = row["name"],
				type = row["type"],
				not_null = bool(row["notnull"]),
				default = row["dflt_value"],
				is_primary_key = bool(row["pk"])
			)
			columns.append(column)
		return tuple(columns)

	def has_table(self, table_name):
		return self.select_value("sqlite_master", "count(*)", tbl_name=table_name, type="table") > 0

	def get_table_names(self):
		return self.select_column("sqlite_master", "tbl_name", type="table", result_type="tuple")

	def get_index_names(self):
		return self.select_column("sqlite_master", "tbl_name", type="index", result_type="tuple")

	def get_table_names_row_counts(self):
		# Return name of each table and number of rows in it.
		# Caveat:  This skips any tables that have names that are not identifiers.
		TableInfo = collections.namedtuple("TableInfo", ("name", "row_count"))
		table_names_row_counts = []
		for table_name in self.get_table_names():
			if _is_identifier(table_name):
				row_count = self.select_value(table_name, what="count(*)")
				table_names_row_counts.append( TableInfo(table_name, row_count) )
		return table_names_row_counts

	def get_schema(self, table_name): # TODO: finish
		return self.select_value("sqlite_master", what="sql", name=table_name)

	import contextlib
	@contextlib.contextmanager
	def transaction(self):
		with self._get_db_conn():
			yield
	del contextlib

	def _make_mutable_parameters(self, parameters):
		# helper for _convert_where_and_vars(..)

		if isinstance(parameters, (tuple, list)):
			if len(parameters) >= 1 and self._placeholder in (":", "$"):
				assert False
			return list(parameters)
		elif isinstance(parameters, dict):
			if len(parameters) >= 1 and self._placeholder == "?":
				assert False
			return dict(parameters)
		elif parameters is None:
			if self._placeholder == "?":
				return []
			elif self._placeholder in (":", "$"):
				return {}
			else:
				raise AssertionError("Unexpected placeholder:	%r"%self._placeholder)
		else:
			raise TypeError("parameters should not be of type %r"%type(parameters))

	def _add_dict_to_where_parts(self, where_dict, where_parts, parameters):
		# helper for _convert_where_and_vars(..)

		assert isinstance(where_parts, list)
		for k,v in _iteritems(where_dict):
			assert _is_identifier(k), k # anti-injection paranoia
			if v is None:
				where_parts.append(("%s ISNULL"%k, True))
			elif self._placeholder == "?":
				where_parts.append(("%s=?"%k, False))
				parameters.append(v)
			elif self._placeholder in ("$", ":"):
				placeholder_name = k
				while placeholder_name in parameters:
					placeholder_name += "_"
				term = "%s=%s%s"%(k, self._placeholder, placeholder_name)
				where_parts.append((term, False))
				parameters[placeholder_name] = v
			else:
				assert False, "Unexpected placeholder:	%r"%self._placeholder

	def _convert_where_and_vars(self, where, parameters=None, where_kw=None):
		parameters = self._make_mutable_parameters(parameters)
		where_parts = [] # list of 2-tuples like [(term, needs_parentheses),...]
						 # such that needs_parentheses is either True or False,
						 # to indicate if this term should be parenthesized if
						 # it is AND'd with some other arbitrary term.

		if where is not None and where is not True: # NONE or TRUE
			if _is_unicode_or_byte_string(where):     # STRING
				where_parts.append((where.strip(), True))
			elif isinstance(where, (tuple, list)):	  # SEQUENCE
				where_parts = list((term.strip(), True) for term in where)
			elif isinstance(where, dict):			  # DICT
				where_parts = []
				self._add_dict_to_where_parts(where, where_parts, parameters)
			else:
				raise ValueError("where should be string, sequence, dict, or None; got %r"%where)

		if where_kw:
			self._add_dict_to_where_parts(where_kw, where_parts, parameters)

		where_parts = tuple(s for s in where_parts if s)
		num_where_parts = len(where_parts)
		if num_where_parts == 0:
			where_str = None
		elif num_where_parts == 1:
			where_str = where_parts[0][0]
		else:
			where_str = " AND ".join(("("+ s + ")" if needs_parentheses else s)
									 for (s,needs_parentheses) in where_parts)
		return (where_str,parameters)

def _is_unicode_or_byte_string(s):
	return isinstance(s, _string_unicode_or_bytes)

def _is_identifier(s):
	try:
		cre = _is_identifier.cre
	except AttributeError:
		cre = _is_identifier.cre = re.compile(r'^[_A-Za-z][_A-Za-z0-9]*$')
	return s.isalnum() or cre.match(s) is not None

def _combine(string_or_parts, joiner=", "):
	if _is_unicode_or_byte_string(string_or_parts):
		return string_or_parts
	else:
		return joiner.join(string_or_parts)

def _add_clause(parts, leader, string_or_parts, joiner=", ", trailer=None, integer=None):
	if string_or_parts is not None and string_or_parts != "":
		if integer:
			string_or_parts = _string_unicode(string_or_parts)
		if integer:
			assert string_or_parts.isdigit()
		parts.append(leader)
		parts.append(_combine(string_or_parts=string_or_parts, joiner=joiner))
		if trailer:
			parts.append(trailer)

def _add_clause_on_conflict(parts, on_conflict):
		if on_conflict is not None:
			on_conflict = on_conflict.upper()
			if on_conflict not in ALL_ON_CONFLICT:
				raise ValueError("on_conflict must be one of: %r"%ALL_ON_CONFLICT)
			parts.extend(("OR", on_conflict))

def _vars_kwargs(parameters, kwargs):
	if parameters is None:
		parameters = kwargs
		kwargs = {}
	return (parameters, kwargs)

	
DBColumn = collections.namedtuple("DBColumn", ("name", "type", "default", "not_null", "is_primary_key"))

class CannotFindUniqueKeyForTableError(Exception):
	"Raised by insert_with_key_fn(..) if a unique key value could not be found."

class DBItemNotFoundError(KeyError):
	"Raised by select_one_where and select_value_where if no matching row was found."

class DBTooManyItemsError(Exception):
	"""
	Raised when more than one row was returned by a call to select(), select_row(), or
	select_column() for which only one row was expected.
	"""

class DBTooManyColumnsError(Exception):
	"Raised by select_column and select_value if more than column were returned."

class DBTableNotFoundError(Exception):
	"Raised by get_table_names if the table doesn't exist in the database."

class DBClosedError(Exception):
	pass

class DBIntegrityError(Exception):
	"""
	Wraps sqlite3.IntegrityError.  This is done for consistency with the other errors defined
	here, as well as to insulate caller from differences between the built-in sqlite3 module
	and the pysqlite2 module, which can be substituted using this class.

	WARNING:  It is unlikely that you will need to use this.  Most common cases when you would
			  have been covered by the various convenience functions:  upsert, insert_or_select,
			  and insert_with_key_fn.
	"""
	def __init__(self, inner):
		Exception.__init__(self, *(inner.args))
		self.inner = inner	# sqlite3.IntegrityError is very simple, but we'll save the instance just in case.

def _format_datetime_utc(dt):
	s = dt.isoformat()
	suffix = "Z"
	if s.endswith(suffix):
		s = s[-len(suffix):] + "+00:00"
	return s


def _parse_datetime_iso(s):
	s = s.strip()
	if s.endswith("Z"):
		s = s[:-1]
		tz = TZ(0, "UTC")
	else:
		ptn = r"([-+])(\d\d)(?::?(\d\d))$"
		mo = re.search(ptn, s)
		if mo:
			tz_name = mo.group(0)
			s = s[:-len(tz_name)]
			tz_sign = {"+":1, "-":-1}[mo.group(1)]
			tz_offset_mins = int(mo.group(2)) * 60 * tz_sign
			if mo.group(3) is not None:
				tz_offset_mins += int(mo.group(3)) * tz_sign
			if tz_offset_mins == 0:
				tz_name = "UTC"
			tz = TZ(offset_mins=tz_offset_mins, name=tz_name)
		else:
			tz = None

	formats_tried = []
	for sep in ("T", " "):
		for fmt_micro in ("", ".%f"):
			for time_sep in ("", ":"):
				for date_sep in ("", "-"):
					fmt_date = date_sep.join(("%Y", "%m", "%d"))
					fmt_time = time_sep.join(("%H", "%M", "%S"))
					fmt = fmt_date + sep + fmt_time + fmt_micro
					formats_tried.append(fmt)
					try:
						dt = datetime.datetime.strptime(s, fmt)
					except ValueError:
						pass
					else:
						if tz is not None:
							dt = dt.replace(tzinfo=tz)
						return dt
	raise ValueError("time data '%s' does not match any of these %d formats: %s"%(
			s,
			len(formats_tried),
			", ".join("'%s'"%fmt for fmt in formats_tried)))
						
		

# Credit:  TZ is adapted from the FixedOffset example in the Python documentation, for the datetime module.
class TZ(datetime.tzinfo):
	"""Fixed offset in minutes east from UTC."""

	def __init__(self, offset_mins, name):
		self.__offset_mins = offset_mins
		self.__offset = datetime.timedelta(minutes = offset_mins)
		self.__name = name

	def utcoffset(self, dt):
		return self.__offset

	def tzname(self, dt):
		return self.__name

	def dst(self, dt):
		return datetime.timedelta(0)
	
	def __repr__(self):
		return "%s(%r, %r)"%(type(self).__name__, self.__offset_mins, self.__name)


_PY3 = sys.version_info[0] == 3
_iteritems = (lambda d:d.items()) if _PY3 else (lambda d:d.iteritems())
_string_unicode_or_bytes = (type(b''.decode('utf8')), type(b''))
_string_unicode = type(b''.decode('utf8'))


#________________________________________________________________________________
# SHRAPNEL BELOW HERE
# vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv

#if __name__=="__main__":
#	def _test():
#		db = SqliteDB(":memory:")
#		db.query("create table t (a integer,b integer)")
#		db.create_index("t", ("a", "b"))
#		db.create_index("t", ("a", "b"))
#	_test()

#	def _make_mutable_parameters(self, parameters):
#		# helper for _convert_where_and_vars(..)
#
#		if self._placeholder == "?":
#			if parameters is None:
#				return []
#			elif isinstance(parameters, (tuple, list)):
#				return list(parameters)
#			else:
#				assert False, "Invalid parameters, expected tuple or list, got %r"%parameters
#		elif self._placeholder in (":", "$"):
#			if parameters is None:
#				return {}
#			elif isinstance(parameters, dict):
#				return dict(parameters)
#			else:
#				assert False, "Invalid parameters, expected dict got %r"%parameters
#		else:
#			assert False, "Unexpected placeholder:	%r"%self._placeholder

#	def insert_or_select(self, table, **kwargs):
#		# TODO:  Make this return results in the usual format, i.e., row_type and result_type
#
#		# Hmmm.... Could this raise a DBTooManyItemsError?	If kwargs cases an
#		# IntegrityError, then that would mean they contain field(s) that are
#		# required to be unique.  Thus, in theory when selecting using those
#		# same keys, there shouldn't be more than one.	Still, somehow I wonder
#		# if there's some way this could happen.  Think about it later...  :)
#		try:
#			row_id = self.insert(table, seqname=True, **kwargs) # seqname=True makes it return the row_id
#			row = self.select_row(*args, row_id=row_id, **kwargs)
#		except DBIntegrityError:
#			row = self.select_one_where(table, **kwargs)
#		return row

#	def select_or_insert(self, *args, **kwargs):
#		# TODO:  Make this return results in the usual format, i.e., row_type and result_type
#		try:
#			row = self.select_one_where(*args, **kwargs)
#		except DBItemNotFoundError:
#			row_id = self.insert(*args, return_row_id=True, **kwargs)
#			row = self.select_one_where(*args, row_id=row_id, **kwargs)
#		return row

#	def insert_or_abort(self, tablename, **values): # TODO: finish, do we even need/want this?
#		return self.insert_or(tablename, "abort", **values)

#	def insert_or_replace(self, tablename, **values): # TODO: finish, do we even need/want this?
#		return self.insert_or(tablename, "replace", **values)

#	def insert_or_fail(self, tablename, **values): # TODO: finish, do we even need/want this?
#		return self.insert_or(tablename, "fail", **values)

#	def insert_or_ignore(self, tablename, **values): # TODO: finish, do we even need/want this?
#		return self.insert_or(tablename, "ignore", **values)

#	def insert_or_rollback(self, tablename, **values): # TODO: finish, do we even need/want this?
#		return self.insert_or(tablename, "rollback", **values)

#		self.select			  = db.select
#		# def select(self, tables, vars=None, what='*', where=None, order=None, group=None, limit=None, offset=None, _test=False): 
#		#	  tables ----- string (becomes body of FROM clause), or list of strings (joined by ", " in web.db.sqlite(..))
#		#	  vars ------- dict of the substitutions for placeholders (i.e., in where), if any
#		#	  what ------- string (becomes body of the SELECT clause); may contain commas; terms may include any valid expression; default=*
#		#	  where ------ string (becomes body of the WHERE clause); placeholders are like $foo
#		#	  order ------ string (becomes body of the ORDER BY clause); should probably end with "ASC" or "DESC"
#		#	  group ------ string (becomes body of the GROUP BY clause); may contain commas
#		#	  limit ------ string (becomes body of the LIMIT clause)
#		#	  offset ----- string (becomes body of the OFFSET clause)
#		#	  _test ------ If True, this will return the query itself as a web.db.SQLQuery, without running it or fetching any results.
#
#		self.insert			 = db.insert
#		# def insert(self, tablename, seqname=None, _test=False, **values): 
#		#	  tablename -- Name of table (becomes body of INTO clause) may include database name if any
#		#	  seqname ---- Set to True (or anything other than False, actually) to return the last row ID
#		#	  _test ------ Same as for select_where(..)
#		#	  **values --- Mapping of column name to value; if empty then default values will be inserted
#		#
#		# Note:  If you set seqname=True to receive the row_id, it will run a separate query:
#		#			 SELECT last_insert_row_id();
#		#		 I *think* that could cause a race condition, i.e., if another query happens after the
#		#		 insert query but before the last_insert_row_id() query.
#
#		self.update			 = db.update
#		# def update(self, tables, where, vars=None, _test=False, **values): 
#		#	  tables ----- string (becomes body of FROM clause), or list of strings (joined by ", " in web.db.sqlite(..))
#		#	  where ------ string (becomes body of WHERE clause); placeholders are like $foo
#		#	  vars ------- dict of the substitutions for placeholders (i.e., in where), if any
#		#	  _test ------ If True, this will return the query itself as a web.db.SQLQuery, without running it or fetching any results.
#		#	  **values --- For each K->V mapping, column K will be set to V on every row matched by the where clause.
#
#		self.delete			 = db.delete
#		# def delete(self, table, where, using=None, vars=None, _test=False): 
#		#	  table ------ Name of table to delete from (becomes body of FROM clause)
#		#	  where ------ string (becomes body of WHERE clause); placeholders are like $foo
#		#	  using ------ Do not use; not supported by SQLite
#		#	  vars ------- dict of the substitutions for placeholders (i.e., in where), if any
#		#	  _test ------ If True, this will return the query itself as a web.db.SQLQuery, without running it or fetching any results.
#
#		self.multiple_insert = db.multiple_insert
#		# def multiple_insert(self, tablename, values, seqname=None, _test=False):
#		#	  tablename -- Name of table (becomes body of INTO clause) may include database name if any
#		#	  values ----- Sequence of dicts, each of which maps a column name to the value to be inserted.
#		#	  seqname ---- Set to True (or anything other than False, actually) to return a list of the row IDs for all rows created
#		#	  _test ------ If True, this will return the query itself as a web.db.SQLQuery, without running it or fetching any results.
#		# 
#		# Returns None if seqname=False (or unspecified); otherwise a list of row_ids for each inserted row.
#		# 
#		# Note:  This just calls insert(..) repeatedly, without any transaction and without using the
#		#		 executemany(..) method provided by the sqlite3 module. [as of 12/7/2012]
#
#		self.transaction	 = db.transaction
#		# def transaction(self): 
#		# 
#		# Returns a Transaction object, which works as a context manager (i.e., supports with statement) and
#		# contains a .commit() and .rollback() methods.
#
#		self.query			 = db.query
#		self.query_no_modify = db.query
#		self.query_select	 = db.query
#		self.query_pragma	 = db.query
#		self.query_insert	 = db.query
#		self.query_update	 = db.query
#		# def query(self, sql_query, vars=None, processed=False, _test=False): 
#		#	  sql_query -- SQL query as a string; placeholders are like $foo; query may also be given as a web.db.SQLQuery object
#		#	  vars ------- dict of the substitutions for placeholders in the query, if any
#		#	  processed -- Pass True if vars has already been converted to a "reparam-style list" (meaning?)
#		#	  _test ------ If True, this will return the query itself as a web.db.SQLQuery, without running it or fetching any results.
#
#		# Notice that db is not an instance attribute and is not available by any means other than the above methods.

#		# Find the LOG PATH
#		log_path = db_args.pop("log_path", None)
#		log_all  = db_args.pop("log_all", False) and True


#		# Accept a dictionary for the where parameter to select(..), update(..), and delete(..).
#
#		def wrap_to_adapt_where_and_vars(fn):
#			spec = inspect.getargspec(fn)
#			arg_names = spec.args
#			is_method = inspect.ismethod(fn)
#			assert is_method == (spec.args[0] == "self")
#			if is_method:
#				arg_names = arg_names[1:]
#
#			@functools.wraps(fn)
#			def wrapper(*args, **kwargs):
#				# Get dictionary of actual parameters
#				kwargs = inspect.getcallargs(fn, *args, **kwargs)
#
#				# Deal with `self`
#				if is_method:
#					kwargs.pop("self")
#
#
#				# Modify where and vars, as needed
#				(kwargs["where"],kwargs["vars"]) = _convert_where_and_vars(kwargs["where"],kwargs["vars"])
#
#				# Convert dictionary of actual parameters to (*args, **kwargs) that we can pass to fn.
#				kwargs.update( kwargs.pop(spec.keywords, ()) )
#				args = []
#				for arg in spec.args:
#					if arg in kwargs:
#						args.append(kwargs.pop(arg))
#					else:
#						assert len(kwargs.get(spec.varargs, ())) == 0
#						break
#				else:
#					args.extend(kwargs.pop(spec.varargs, ()))
#
#				return fn(*args, **kwargs)
#			return wrapper
#
#		self.select = wrap_to_adapt_where_and_vars(self.select)
#		self.update = wrap_to_adapt_where_and_vars(self.update)
#		self.delete = wrap_to_adapt_where_and_vars(self.delete)
#
#		# Convert sqlite3.IntegrityError or pysqlite2.IntegrityError to our wrapper.
#		#
#		# This excludes select, query_no_modify, and transaction.  Also, we are not currently
#		# bothering with DataError, DatabaseError, ERror, InterfaceError, InternalError,
#		# NotSupportedError, OperationalError, or Warning, all of which could be raised by
#		# certain calls to the sqlite3 or pysqlite2 APIs.
#		
#		self.execute_script = lambda sql:db.ctx.db.executescript(sql) # [pylint] possibly unnecessary lambda : pylint:disable=W0108
#																	  # not actually sure if the lambda is needed or not
#
#		def wrap_integrity_error(fn):
#			@functools.wraps(fn)
#			def _wrap_integrity_error(*args, **kwargs):
#				try:
#					return fn(*args, **kwargs)
#				except db_module.IntegrityError as e:
#					raise DBIntegrityError(inner=e)
#			return _wrap_integrity_error
#
#		assert "db" in db.ctx
#		_db = db.ctx.db
#		assert _db is not None
#
#		def close():
#			_db.close()
#
#			def fail():
#				raise DBClosedError()
#
#			stems = ("insert", "update", "query", "delete", "transaction")
#			for k in dir(self):
#				if k.startswith(stems):
#					v = getattr(self, k)
#					if callable(v):
#						setattr(self, k, fail)
#
#		self.close			 = close
#
#		self.insert			 = wrap_integrity_error(self.insert)
#		self.update			 = wrap_integrity_error(self.update)
#		self.query			 = wrap_integrity_error(self.query)
#		self.query_insert	 = wrap_integrity_error(self.query_insert)
#		self.query_update	 = wrap_integrity_error(self.query_update)
#		self.delete			 = wrap_integrity_error(self.delete)
#		self.multiple_insert = wrap_integrity_error(self.multiple_insert)
#		self.query			 = wrap_integrity_error(self.query)
#		self.query_pragma	 = wrap_integrity_error(self.query_pragma)
#
#		if log_path:
#			self.insert				 = log_calls(log_path)(self.insert)
#			self.update				 = log_calls(log_path)(self.update)
#			self.query				 = log_calls(log_path)(self.query)
#			self.query_insert		 = log_calls(log_path)(self.query_insert)
#			self.query_update		 = log_calls(log_path)(self.query_update)
#			self.delete				 = log_calls(log_path)(self.delete)
#			self.multiple_insert	 = log_calls(log_path)(self.multiple_insert)
#
#			if log_all:
#				self.select			 = log_calls(log_path)(self.select)
#				self.transaction	 = log_calls(log_path)(self.transaction)
#				self.query			 = log_calls(log_path)(self.query)
#				self.query_no_modify = log_calls(log_path)(self.query_no_modify)
#				self.query_pragma	 = log_calls(log_path)(self.query_pragma)

#		# Expose all exception classes
#		#
#		# These are all of the Exception subclasses found in both sqlite3 and pysqlite2.dbapi2.
#		#
#		# ON HOLD... Doing this would make it more difficult to migrate away from pysqlie2, if desired.
#		#
#		#self.InternalError		= db_module.InternalError
#		#self.ProgrammingError	= db_module.ProgrammingError
#		#self.NotSupportedError = db_module.NotSupportedError
#		#self.DataError			= db_module.DataError
#		#self.IntegrityError	= db_module.IntegrityError
#		#self.Error				= db_module.Error
#		#self.InterfaceError	= db_module.InterfaceError
#		#self.OperationalError	= db_module.OperationalError
#		#self.DatabaseError		= db_module.DatabaseError
#		#self.Warning			= db_module.Warning

#		# [pylint] Lambdas below may be needed to ensure that we refer to the current thread's db connection : pylint:disable=W0108


#	elif vars and kwargs:
#		raise ValueError("You may specify vars by var or **kwargs but not both")
#	else:
#		return vars or kwargs or {}

#	def select_where(self, table, what='*', where=None, parameters=None, order=None, group=None, limit=None, offset=None, _test=False, **kwargs):
#		"""
#		This is exactly the same as web.py's db.where(..) function, except that the following bug is fixed:
#
#		The bug is in Web.py and causes db.where("mytable", mycol=None) to translate into...
#		  SELECT * FROM MYTABLE WHERE MYCOL = NULL;
#		I think it should translate to...
#		  SELECT * FROM MYTABLE WHERE MYCOL ISNULL;
#
#		table ----- Name of table we are querying
#		what ------ string (becomes body of the SELECT clause); may contain commas; terms may include any valid expression; default=*
#		order ----- string (becomes body of the ORDER BY clause); should probably end with "ASC" or "DESC"
#		group ----- string (becomes body of the GROUP BY clause); may contain commas
#		limit ----- string (becomes body of the LIMIT clause)
#		offset ---- string (becomes body of the OFFSET clause)
#		**kwargs -- For each K->V mapping, the result rows will be constrained such that column K must equal V.
#		"""
#		where_clauses = []
#		_parameters = {}
#
#		if where:
#			where_clauses.append(where)
#
#		if parameters:
#			_parameters.update(parameters)
#
#		if len(kwargs) >= 1:
#			for k,v in kwargs.items():
#				if not _is_unicode_or_byte_string(k):
#					raise ValueError(k)  # keys must be strings
#
#				if re.match(r"^[_a-zA-Z]\w*$", k) is None: # ensure that all kwargs look safe (defensive, anti-injection)
#					raise ValueError(k)  # keys must match this regular expression:  [_a-zA-Z]\w*$
#
#				if v is None:
#					where_clause = k + " ISNULL"
#				else:
#					where_clause = k + " = $" + k
#
#				# Add the associated key to the list of columns that will be used in the WHERE clause.
#				where_clauses.append( where_clause )
#
#				# Add the associated value to the variables which will be substituted in safely.
#				_parameters[k] = v
#
#			if len(where_clauses)==1:
#			# Just one condition.  Add it to the query as is.
#				where = where_clauses[0]
#			else:
#			# Multiple conditions.	AND them together.
#				where = " AND ".join("(" + where_clause + ")" for where_clause in where_clauses)
#
#		if len(where_clauses) == 0:
#			assert bool(_parameters) == bool(parameters)
#			if _parameters or parameters:
#				raise ValueError("You can't specify parameters without where.  %r"%parameters)	# Hmm... Could they be used in other clauses?
#			_parameters = None
#			where = None
#		return self.select(tables=(table,), parameters=_parameters, what=what, where=where, order=order, group=group, limit=limit, offset=offset, _test=_test)


#	def delete_where(self, table, **kwargs):
#		"""
#		Delete rows from the specified table that match kwargs.
#
#		table_name -- name of the table from which to delete
#		"""
#
#		# Make sure none of the kwargs will conflict with standard arguments to the delete method.
#		bad_arguments = [k for k in kwargs if k in ("table", "where", "using", "parameters", "_test")]
#		if bad_arguments:
#			raise ValueError("Not allowed as keys in the where clause: "%", ".join(bad_arguments))
#		where,query_vars = _convert_where_and_vars(kwargs)
#		if not where and query_vars:
#			raise ValueError("Can't have query variables without a where clause")
#
#		sql = "delete from " + table_name
#		if where:
#			sql += " where " + where
#			return self.query(sql, parameters=query_vars)
#		else:
#			return self.query(sql)
#
#		# This was causing an exception that appears to be due to a Web.py bug, but who knows...  (11/19/2013)
#		#return self.delete(table=table_name, where=where, parameters=query_vars)

	
#	def insert_or(self, tablename, on_conflict, **values):
#		query_vars = {}
#		param_names = []
#		on_conflict = on_conflict.lower()
#
#		#return_row_id = values.pop("seqname", False) == True
#		assert "seqname" not in values # this doesn't seem to work, yet.  FIXME
#
#		if on_conflict not in ("rollback", "abort", "replace", "fail", "ignore"):
#			raise ValueError("Invalid on_conflict value:  %r"%(on_conflict))
#
#		for k,v in _iteritems(values):
#			query_vars[k] = v
#			param_names.append( k )
#
#		sql = "insert or %s into %s (%s) values (%s)"%(
#			on_conflict,
#			tablename,
#			", ".join(param_names),
#			", ".join("$"+s for s in param_names),
#		)
#
##		if return_row_id:
##			return tuple(self.query_select("select last_insert_row_id() as last_id;"))[0].last_id
##		else:
##			return None
##		row_id = self.query_insert(sql, parameters=query_vars)
##		assert isinstance(row_id, int), row_id
##		return row_id
#		self.query_insert(sql, parameters=query_vars)

#	def _create_db_with_schema(self, schema_path, db_path, db_module):
#		# Note:  This creates its own connection outside of the usual Web.py DB access
#		#		 because Web.py does not seem to support the executescript(..) method
#		#		 of the sqlite3 module.
#		with open(schema_path, "rb") as infile:
#			sql = infile.read()
#			sql = sql.decode("utf8")
#		temp_conn = db_module.connect(db_path)
#		try:
#			temp_conn.executescript(sql)
#		finally:
#			temp_conn.close()
	
#	def _query_select(self, sql, parameters, result_type, row_type, with_names, one_row, one_column):
#		# Get and check row_type and result_type
#		result_type,row_type = self._get_result_type_row_type(
#										result_type, row_type, one_row, one_column)
#
#		# Execute the query
#		cursor = self._execute_query(sql, parameters)
#		assert cursor.description is not None, (sql, parameters)
#
#		# Get COLUMN NAMES
#		if with_names or row_type in ("dict", "namedtuple"):
#			col_names = tuple(o[0] for o in cursor.description)
#
#		# Adapt ROWS
#		if row_type == "tuple":
#			rows = cursor
#		elif row_type == "dict":
#			rows = (dict(zip(col_names,row)) for row in cursor)
#		elif row_type == "namedtuple":
#			result_namedtuple = self._get_result_namedtuple(col_names)
#			rows = (result_namedtuple(*row) for row in cursor)
#		else:
#			assert False, row_type # shouldn't get here
#
#		# Adapt RESULT
#		if result_type == "tuple":
#			rows = tuple(rows)
#		elif result_type == "generator" and row_type=="tuple":
#			rows = (row for row in rows) # convert cursor to a bona fide generator
#		assert isinstance(rows, tuple) == (result_type == "tuple")
#		assert inspect.isgenerator(rows) == (row_type ==)
#
#		if result_type in ("names+tuple", "names+generator"):
#			return (col_names, rows)
#		else:
#			return rows

#	def _adapt_select_result(self, result_type, row_type, col_names):
#		# helper for _query_select
#		if row_type == "tuple":
#			rows = cursor
#		elif row_type == "dict":
#			rows = (dict(zip(col_names,row)) for row in cursor)
#		elif row_type == "namedtuple":
#			result_namedtuple = self._get_result_namedtuple(col_names)
#			rows = (result_namedtuple(*row) for row in cursor)
#		else:
#			assert False, row_type # shouldn't get here
#
#		if result_type == "tuple":
#			rows = tuple(rows)
#		elif result_type == "generator" and row_type=="tuple":
#			rows = (row for row in rows) # convert cursor to a bona fide generator
#		assert isinstance(rows, tuple) == (result_type == "tuple")
#		assert inspect.isgenerator(rows) == (row_type ==)
#
#		return rows
	
#	def update_one(self, table, where, parameters=None, _test=False, **values):  # TODO, finish
#		_where,_parameters = self._convert_where_and_vars(where, parameters)
#		row, = self.select(table, what="count(*) as num_matches", where=_where, parameters=_parameters)
#		changed = False
#		num_matches = row["num_matches"]
#		if num_matches == 1:
#			num_matches = self.update(tables=table, where=_where, parameters=_parameters, _test=_test, **values)
#
#		if num_matches == 1:
#			return num_matches
#		else:
#			if changed:
#				changes_made_msg = "%d changes were made!"%num_matches
#			else:
#				changes_made_msg = ""
#
#			if num_matches == 0:
#				if changed:
#					changes_made_msg = " ... " + changes_made_msg
#				raise DBItemNotFoundError(repr((where, parameters)) + changes_made_msg)
#			elif num_matches >= 2:
#				raise DBTooManyItemsError(where, parameters, changes_made_msg)
#			else:
#				assert False

#	def _make_empty_result_parts_dict(self, result_type):
#		if _is_unicode_or_byte_string(result_type):
#			parts = (result_type,)
#		if not (isinstance(result_type, tuple) and all(s in ALL_RESULT_TYPES for s in result_type)):
#			raise ValueError("result_type must be a string in %r or tuple thereof"%ALL_RESULT_TYPES)
#		return dict.fromkeys(result_type)

#	def select_one_where(self, *args, **kwargs): # TODO: finish
#		"""
#		Return a single row from a table.
#		Raises DBItemNotFoundError if no row matched and default was not specified.
#		Raises DBTooManyItemsError if multiple rows matched
#		"""
#		result = self.select_where(*args, **kwargs)
#		if "_test" not in kwargs:
#			rows = tuple(result)
#			if len(rows)==1:
#				return rows[0]
#			elif len(rows)==0:
#				raise DBItemNotFoundError(repr((args, kwargs)))
#			elif len(rows) > 1:
#				raise DBTooManyItemsError(args, kwargs)
#			assert False
#		else:
#			return result  # actually returns sql

#	@staticmethod
#	def _get_one_and_only_column_from_rows(rows): # GENERATOR FUNCTION	# TODO, uncomment when used
#		for row in rows:
#			value, = row.itervalues()
#			yield value

#	def select_column_where(self, table, what, *args, **kwargs): # GENERATOR FUNCTION # TODO, finish
#		"""
#		Return a list of values from the column named what in the specified table, for rows matching kwargs.
#
#		table ----- name of table
#		what ------ name of column to return
#		*args ----- anything else you would pass to select_where after what
#		**kwargs -- anything else you would pass to select_where
#		"""
#		if "," in what:
#			raise ValueError("For selecting a column, multiple fields are not allowed.	Got %r"%what)
#		result = self.select_where(table, what, *args, **kwargs)
#		if "_test" not in kwargs:
#			rows = result # for clarity
#			return self._get_one_and_only_column_from_rows(rows)
#		else:
#			sql = result # for clarity
#			return sql	# actually returns sql

#	def select_value_where(self, table, what, *args, **kwargs): # TODO, finish
#		"""
#		Return a single value from a table.  Assumes only one row matches
#		kwargs.  Returns the value in the column named what.
#
#		table ----- name of table
#		what ------ name of column to return
#		*args ----- anything else you would pass to select_where after what
#		**kwargs -- anything else you would pass to select_where
#
#		Raises DBItemNotFoundError if no row matched.
#		Raises DBTooManyItemsError if multiple rows matched
#		"""
#		if "," in what:
#			raise ValueError("For selecting a single value, multiple fields are not allowed.  Got %r"%what)
#		row = self.select_one_where(table, what, *args, **kwargs)
#		if "_test" not in kwargs:
#			value, = row.values()
#			return value
#		else:
#			return row	# actually returns sql


