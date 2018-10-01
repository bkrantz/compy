import re
import traceback
from lxml import etree
from compy.actor import Actor
from compy.errors import MalformedEventData

__all__ = [
	"_Database",
	"_DatabaseAuto"
]

class InvalidResultsException(Exception):
	def __init__(self, expected, received, *args, **kwargs):
		message = "Query returned unexpected results. Expected {expected} but received {received}".format(
			expected=expected, received=received)
		self.expected = expected
		self.received = received
		super(InvalidResultsException, self).__init__(message, *args, **kwargs)

class _Database(Actor):

	def __init__(self, 
			name,
			query_template=None,
			db_config=None,
			db_pool=None,
			param_scope=["_data"],
			response_scope=["_data"],
			response_join_key="aggregated",
			response_key="result",
			response_plural_key_postfix="s",
			response_plural_key=None,
			static_params={},
			literal_params=[],
			override_static=False,
			output_mode="update",
			expected_results=None,
			max_attempts=3,
			*args,
			**kwargs):
		super(_Database, self).__init__(name, *args, **kwargs)
		self.query_template = query_template
		self.db_pool = self.__get_db_pool(db_config=db_config, db_pool=db_pool)
		self.param_scope = param_scope
		self.response_scope = response_scope
		self.join_key = response_join_key
		self.records_key = response_plural_key if response_plural_key else response_key + response_plural_key_postfix
		self.record_key = response_key
		self.static_params = self.__get_static_params(data=static_params)
		self.literal_params = [param for param in literal_params] + [param for param in self.static_params.iterkeys() if param not in literal_params]
		self.override_static = override_static
		self.output_mode = output_mode
		self.expected_results = expected_results
		self.max_attempts = max_attempts

	def __get_db_pool(self, db_config, db_pool):
		if db_pool:
			return db_pool
		return self._create_db_pool(db_config=db_config)
	
	def __get_static_params(self, data):
		static_params = {}
		try:
			static_params.update(data)
		except ValueError:
			pass
		return static_params

	def __combine_params(self, dynamic_params):
		query_params = {}
		if self.override_static:
			query_params.update(self.static_params)
			query_params.update(dynamic_params)
		else:
			query_params.update(dynamic_params)
			query_params.update(self.static_params)
		return query_params		

	def _assemble_query(self, query_template, query_params, *args, **kwargs):
		return query_template.format(**query_params)

	def _validate_results(self, event, results):
		if self.expected_results and len(results) != self.expected_results:
			raise InvalidResultsException(self.expected_results, len(results))
		return

	def _get_scope(self, event, scope):
		if scope:
			return event.lookup(scope)
		return event

	def consume(self, event, *args, **kwargs):
		dynamic_param_groups = self._get_dynamic_params(event=event)
		results = []
		for dynamic_params in dynamic_param_groups:
			try:
				query_params = self.__combine_params(dynamic_params=dynamic_params)
				query = self._assemble_query(query_template=self.query_template, query_params=query_params)
				results = results + self._execute_query(query=query)
			except Exception as e:
				raise MalformedEventData(str(e))
		self._validate_results(event=event, results=results)
		event = self.format_output(event=event, results=results)
		self.send_event(event)

class _DatabaseAuto(_Database):

	def __init__(self, name, ignore_fields=[], schema="", table="", *args, **kwargs):
		super(_DatabaseAuto, self).__init__(name, *args, **kwargs)
		self.schema = schema
		self.table = table
		self.table_fields = []
		all_table_fields = self._get_table_fields()
		self.table_fields = [field for field in all_table_fields if field not in ignore_fields]
		self.query_template = self._query_template