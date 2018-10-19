from compy.actors.util.mysql import MySQLConnectionPool, _MySQLConnectionManager
from pymysql import IntegrityError, ProgrammingError, NotSupportedError, DataError
import gevent, re

__all__ = [
	"_MySQLMixin",
	"_MySQLAutoMixin",
	"_MySQLInsertMixin",
	"_MySQLWriteMixin",
	"_MySQLSelectMixin"
]

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
			results = [result for result in fetched_results if result] if fetched_results else []
		return results

class _MySQLAutoMixin:
	_fields_query_template = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA LIKE '{__schema}' AND TABLE_NAME LIKE '{__table}'"

	def _get_table_fields(self):
		query = self._assemble_query(query_template=self._fields_query_template)
		results = self._execute_query(query=query)
		return [result.get('COLUMN_NAME') for result in results]

	def __format_value(self, value):
		value = value.replace("'", "\\'")
		return "'%s'" % value

	def _assemble_query(self, query_template, query_params={}, *args, **kwargs):
		query_params = {"`%s`"%field: query_params.get(field, '') if field in self.literal_params else self.__format_value(query_params.get(field, '')) for field in self.table_fields if query_params.get(field, None) is not None}
		values = ", ".join([value for key, value in query_params.iteritems()])
		fields = ", ".join(["%s" % key for key in query_params.iterkeys()])
		all_fields = ", ".join(self.table_fields)
		updates = ", ".join(["%s=%s" % (field, value) for field, value in query_params.iteritems()])
		likes = " AND ".join(["%s LIKE %s" % (field, value) for field, value in query_params.iteritems()])
		query = query_template.format(__schema=self.schema, __table=self.table, __all_fields=all_fields, __fields=fields, __values=values, __updates=updates, __likes=likes)
		return query

class _MySQLInsertMixin:
	_query_template = "INSERT INTO {__schema}.{__table} ({__fields}) VALUES ({__values})"

class _MySQLWriteMixin:
	_query_template = "INSERT INTO {__schema}.{__table} ({__fields}) VALUES ({__values}) ON DUPLICATE KEY UPDATE {__updates}"

class _MySQLSelectMixin:
	_query_template = "SELECT {__all_fields} FROM {__schema}.{__table} WHERE {__likes}"

