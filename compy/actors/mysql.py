from pymysql.connections import Connection
from pymysql.cursors import DictCursor
from pymysql.constants import FIELD_TYPE
from pymysql.constants.CLIENT import MULTI_STATEMENTS
from pymysql import IntegrityError, ProgrammingError, NotSupportedError, DataError
from util.database import _Database, _DatabaseAuto, _XMLDatabaseMixin, _JSONDatabaseMixin
#from compysition.event import MalformedEventData
import gevent, re
from compysition.errors import MalformedEventData

import traceback

__all__ = [
	"_MySQLMixin",
	"MySQLConnectionPool",
	"XMLMySQLActor",
	"XMLMySQLInsertActor",
	"XMLMySQLWriteActor",
	"JSONMySQLActor",
	"JSONMySQLInsertActor",
	"JSONMySQLWriteActor"
]

class _MySQLConnection(Connection):

	def __init__(self, cursorclass=DictCursor, connect_timeout=30, autocommit=True, *args, **kwargs):
		self.mysql_function_regex = re.compile("^[A-Z1-9]*\(.*\)$")
		self.mysql_subquery_regex = re.compile("^\(.*\)$")
		self.kwargs = kwargs
		self.kwargs['connect_timeout'] = connect_timeout
		super(_MySQLConnection, self).__init__(cursorclass=cursorclass, autocommit=autocommit, **self.kwargs)
		self.decoders[FIELD_TYPE.TINY] = lambda x: bool(int(x))

	def cursor(self, *args, **kwargs):
		while True:
			try:
				return super(_MySQLConnection, self).cursor(*args, **kwargs)
			except Exception as err:
				self.connect()
				gevent.sleep(0.1)

	def __connect(self, connect_timeout=1):
		self.__connected = False
		self.connect_timeout = connect_timeout
		super(_MySQLConnection, self).connect()
		self.__connected = True

	def connect(self, **kwargs):
		original_timeout = self.connect_timeout
		self.__connected = False
		while not self.__connected:
			try:
				self.__connect()
				self.__connect(connect_timeout=int(original_timeout))
			except Exception as err:
				print "Stuck reconnecting with ERROR : {0}".format(err)
				gevent.sleep(1)

	def escape(self, obj, *args, **kwargs):
		try:
			if self.mysql_function_regex.search(obj) or self.mysql_subquery_regex.search(obj):
				return obj
		except Exception:
			pass
		return super(_MySQLConnection, self).escape(obj, *args, **kwargs)


class MySQLConfig():

	_required_configs = ['schema', 'port', 'host', 'username', 'password']

	def __init__(self, db_config):
		self.db_config = db_config
		self.db_connection_opts = dict.fromkeys(self._required_configs)
		self.db_connection_opts.update(self.db_config)
		if self.db_connection_opts["port"]:
			self.db_connection_opts["port"] = int(self.db_connection_opts["port"])

class MySQLConnectionPool:

	def __init__(self, db_config, size=3, *args, **kwargs):
		self.db_options = MySQLConfig(db_config)
		config_size = self.db_options.db_connection_opts.get('pool_size', None)
		self.size = int(config_size if config_size else size)
		self.pool = gevent.queue.Queue(maxsize=self.size)
		self.connection_requests = gevent.queue.Queue()
		self.__initialize_pool(**kwargs)

	def __initialize_pool(self, *args, **kwargs):
		while self.pool.qsize() < self.size:
			self.pool.put(_MySQLConnection(
				host=self.db_options.db_connection_opts['host'],
				port=self.db_options.db_connection_opts['port'],
				user=self.db_options.db_connection_opts['username'],
				password=self.db_options.db_connection_opts['password'],
				database=self.db_options.db_connection_opts['schema'],
				use_unicode=False,
				charset='utf8',
				client_flag=MULTI_STATEMENTS,
				**kwargs))

	def get_connection(self):
		while True:           
			try:
				return self.pool.get_nowait()
			except gevent.queue.Empty:
				gevent.sleep(0)

	def release_connection(self, db_connection):
		self.pool.put_nowait(db_connection)

	def close(self):
		while True:
			try:
				self.pool.get_nowait().close()
			except gevent.queue.Empty:
				break

class _MySQLConnectionManager:
	def __init__(self, db_pool, *args, **kwargs):
		self.db_pool = db_pool
		
	def execute(self, *args, **kwargs):
		self.cursor.execute(*args, **kwargs)
		self.last_id = self.cursor.lastrowid

	def fetchone(self):
		return self.cursor.fetchone()

	def fetchall(self):
		return self.cursor.fetchall()

	def reset(self):
		self.cursor.close()
		self.db_connection.close()
		self.db_connection.connect()
		self.cursor = self.db_connection.cursor()

	def __enter__(self):
		self.db_connection = self.db_pool.get_connection()
		self.cursor = self.db_connection.cursor()
		return self

	def __exit__(self, *exc_info):
		self.cursor.close()
		self.db_pool.release_connection(self.db_connection)


class _MySQLMixin:

	def _create_db_pool(self, db_config):
		return MySQLConnectionPool(db_config=db_config)

	def _execute_query(self, query):
		last_id = None
		with _MySQLConnectionManager(db_pool=self.db_pool) as manager:
			attempts = 1
			while attempts <= self.max_attempts + 1:
				try:
					manager.execute(query)
					attempts = self.max_attempts + 2
				except (IntegrityError, ProgrammingError, NotSupportedError, DataError) as e:
					raise e
				except Exception as e:
					if attempts <= self.max_attempts:
						attempts += 1
						try:
							manager.reset()
						except Exception:
							pass
						gevent.sleep(0.1)
					else:
						raise e
			fetched_results = None
			try:
				fetched_results = manager.fetchall()
			except Exception as e:
				pass
			print fetched_results
			results = [result for result in fetched_results if result] if fetched_results else []
		return results

class _MySQLAutoMixin:
	_fields_query_template = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA LIKE '{__schema}' AND TABLE_NAME LIKE '{__table}'"

	def _get_table_fields(self):
		query = self._assemble_query(query_template=self._fields_query_template)
		results = self._execute_query(query=query)
		return [result.get('COLUMN_NAME') for result in results]

	def _assemble_query(self, query_template, query_params={}, *args, **kwargs):
		query_params = {field: query_params.get(field, '') if field in self.literal_params else "'%s'" % query_params.get(field, '') for field in self.table_fields if query_params.get(field, None) is not None}
		values = ", ".join([value for key, value in query_params.iteritems()])
		fields = ", ".join([key for key in query_params.iterkeys()])
		updates = ", ".join(["%s=%s" % (field, value) for field, value in query_params.iteritems()])
		likes = " AND ".join(["%s LIKE %s" % (field, value) for field, value in query_params.iteritems()])
		query = query_template.format(__schema=self.schema, __table=self.table, __fields=fields, __values=values, __updates=updates, __likes=likes)
		return query

class _MySQLInsertMixin:
	_query_template = "INSERT INTO {__schema}.{__table} ({__fields}) VALUES ({__values})"

class _MySQLWriteMixin:
	_query_template = "INSERT INTO {__schema}.{__table} ({__fields}) VALUES ({__values}) ON DUPLICATE KEY UPDATE {__updates}"


class XMLMySQLActor(_XMLDatabaseMixin, _MySQLMixin, _Database):
	pass
class XMLMySQLInsertActor(_XMLDatabaseMixin, _MySQLMixin, _MySQLInsertMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass
class XMLMySQLWriteActor(_XMLDatabaseMixin, _MySQLMixin, _MySQLWriteMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass


class JSONMySQLActor(_JSONDatabaseMixin, _MySQLMixin, _Database):
	pass
class JSONMySQLInsertActor(_JSONDatabaseMixin, _MySQLMixin, _MySQLInsertMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass
class JSONMySQLWriteActor(_JSONDatabaseMixin, _MySQLMixin, _MySQLWriteMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass
